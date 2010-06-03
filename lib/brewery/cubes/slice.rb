# This comment should be ignored
# Boo baa bah
#

require 'data_objects'

module Brewery

# Portion of a cube. Slices are creating by cutting cubes or other slices. Used mainly
# for aggregating measures and drilling down through dimensions.
#
# @example Slicing a cube (see {Cube#whole})
#   # Assume we have a cube with dimensions date and category
#   slice = cube.whole # Get unsliced cube
#   slice = slice.cut_by_point(:date, [2010])
# @example Aggregate whole slice (see {#aggregate})
#   rows = slice.aggregate(:amount)
#   result = rows[0] # Only one row - summary row
#   puts "Total amount     : result[:sum]"
#   puts "Transaction count: result[:record_count]"
# @example Drill-down and aggregate (see {#aggregate} with row options)
#   rows = slice.aggregate(:amount, {:row_dimension => :date, 
#                                    :row_levels => [:year, :month]})
#   puts "Totals by month"
#   result.each { |row|
#       puts "#{row[:year]} #{row[:month_name]}: #{row[:sum]}"
#   }
#
# @author Stefan Urbanek <stefan@agentfarms.net>

class Slice
include DataObjects::Quoting

# List of cuts which define the slice - portion of a cube.
attr_reader :cuts

# @deprecated
attr_reader :cut_values

# Initialize slice instance as part of a cube
def initialize(cube)
    @cube = cube
    @cut_values = Hash.new
    
    @cuts = Array.new
end

# Copying contructor, called for Slice#dup
def initialize_copy(*)
  @cut_values = @cut_values.dup
  @cuts = @cuts.dup
	
end

# Cut slice by provided cut
# @see Cut#initialize
# @param [Cut] cut to cut the slice by
# @return [Slice] new slice with added cut
def cut_by(cut)
	slice = self.dup
	slice.add_cut(cut)
	return slice
end

# Cut slice by dimension point specified by path
# @param [Array] path Dimension point specified by array of values. See {Dimension}
# @see Cut#initialize
# @return [Slice] new slice with added cut
def cut_by_point(dimension, path)
	return self.cut_by(Cut.point_cut(dimension, path))
end

# Cut slice by ordered dimension from point specified by dimension
# keys in from_key to to_key
# @return [Slice] new slice with added cut
# @param [Array] path Dimension point specified by array of values. See {Dimension}
def cut_by_range(dimension, from_key, to_key)
	return self.cut_by(Cut.range_cut(dimension, from_key, to_key))
end


# Add another cut to the receiver.
# @param [Cut] cut Cut to be added
def add_cut(cut)
	@cuts << cut
end

# Remove all cuts by dimension from the receiver.
def remove_cuts_by_dimension(dimension)
	@cuts.delete_if { |cut|
		cut.dimension == dimension
	}
end

# @deprecated
def dataset
    @cube.dataset
end

# Aggregate measure.
#
# @param [Symbol] measure Measure to be aggregated, for example: amount, price, ...
# @param [Hash] options Options for more refined aggregation
# @option options [Symbol] :row_dimension Dimension used for row grouping
# @option options [Array] :row_levels Group by dimension levels
# @option options [Symbol] :limit_type Possible values: ':value', `:percent`, `:rank`
# @option options [Number] :limit Limit value based on limit_type 
# @option options [Symbol] :limit_sort Possible values: `:ascending`, `:descending`
# == Examples:
# * aggregate(:amount, { :row_dimension => [:date], :row_levels => [:year, :month]} )
# @return [Array] list of rows where each row represents a point at row_dimension.
#   If no row dimension is specified, only one summary row is returned.
# @todo implement limits
def aggregate(measure, options = {})
	row_dimension_name = options[:row_dimension]
	row_dimension = @cube.workspace.dimension(row_dimension_name)
	row_levels = options[:row_levels]
	
	if row_levels && row_levels.class != Array
		raise RuntimeError, "Row levels should be an array"
	end
	if row_dimension_name && row_dimension_name.class != Symbol && row_dimension_name.class != String
		raise RuntimeError, "Row dimension should be name as String or Symbol"
	end	
	if row_dimension_name && !row_dimension
		raise RuntimeError, "Unknown dimension #{row_dimension_name} (does not exist in workspace)"
	end
	
	table_alias = "t"

    ################################################
	# 0. Collect tables to be joined

	dimension_aliases = Hash.new
	all_dimensions = Array.new

	dims = @cuts.collect { |cut| cut.dimension }
	all_dimensions.concat(dims)
	if row_dimension
		all_dimensions << row_dimension_name
	end
	
	all_dimensions = all_dimensions.uniq
	
	i = 0
    all_dimensions.each { |dimension|
    	if dimension_aliases[dimension]
    		next
    	end
    	dimension_aliases[dimension] = "d#{i}"
    	# puts "ADDING ALIAS d#{i} for dimension #{dimension}"
        i += 1
    }

	row_dimension_alias = dimension_aliases[row_dimension_name]

    ################################################
	# 1. What needs to be SELECTed

	selections = Array.new
    # 1.1 Aggregations
	
	operators = [:sum, :average]
	for i in ( 0..(operators.count - 1) )
        selections << sql_field_aggregate(measure, operators[i], "agg_#{i}")
    end
    
    # 1.2 Add total record count
    selections << "COUNT(1) AS record_count"

    # 1.3 Row fields - from dimension
    row_fields = Array.new
    if row_levels
		for i in ( 0..(row_levels.count - 1) )
			level_fields = row_dimension.fields_for_level(row_levels[i])
			level_fields.each { |field|
				selection = "#{row_dimension_alias}.#{field}"
				selections << selection
				row_fields << field
			}
		end
	end
	
    ################################################
	# 2. Filters - for WHERE clausule
	filters = Array.new
	
	@cuts.each { |cut|
		dim_alias = dimension_aliases[cut.dimension]
		dimension = @cube.workspace.dimension(cut.dimension)
		filters << cut.sql_condition(dimension, dim_alias)
	}

    ################################################
	# 3. Grouping and ordering

	groupings = Array.new
	sortings = Array.new
    if row_levels
		for i in ( 0..(row_levels.count - 1) )
			level_fields = row_dimension.fields_for_level(row_levels[i])
			level_fields.each { |field|
				aliased_field = "#{row_dimension_alias}.#{field}"
				groupings << aliased_field
				sortings << aliased_field
			}
		end
	end
	

    ################################################
	# 4. Join

	joins = Array.new

	all_dimensions.each { |dim_name|
		dim_alias = dimension_aliases[dim_name]
		dimension = @cube.dimension(dim_name)
        join_info = @cube.dimension_join_info(dim_name)

        joins << dimension.sql_join_expression(join_info[:dimension_key],
                                               join_info[:table_key],
                                               dim_alias, table_alias)
	}

    ################################################
	# 5. Create SQL SELECT statement
	
	select_expr = selections.join(', ')
	
	join_expr = joins.join(' ')
	
	if filters && filters.count > 0
		joined_filters = filters.join(' AND ')
		filter_expr = "WHERE #{joined_filters}"
	else
		filter_expr = ''
	end
	
	if groupings && groupings.count > 0
		joined_groupings = groupings.join(', ')
		group_expr = "GROUP BY #{joined_groupings}"
	else
		group_expr = ''
	end

	if sortings && sortings.count > 0
		joined_sortings = sortings.join(', ')
		sort_expr = "ORDER BY #{joined_groupings}"
	else
		sort_expr = ''
	end

	table_name = @cube.dataset.table_name
	statement = "SELECT #{select_expr}
				FROM #{table_name} #{table_alias}
				#{join_expr}
				#{filter_expr}
				#{group_expr}
				#{sort_expr}"

    ################################################
	# 6. Execute statement

	# puts "#{statement}"
    selection = @cube.dataset.connection[statement]

    ################################################
	# 7. Collect results

    results = Array.new
    selection.each { |record|
    
        result_row = Hash.new
        
		for i in ( 0..(operators.count - 1) )
            field = "agg_#{i}".to_sym
            value = record[field]
    
            # FIXME: use appropriate type (Sequel SQLite returns String)
            if value.class == String
                value = value.to_f
            end
            result_row[operators[i]] = value
    	end

		# Collect fields from dimension levels
        row_fields.each { |field|
            result_row[field] = record[field]
        }

        value = record[:record_count]
        if value.class == String
            value = value.to_f
        end
        result_row[:record_count] = value

        results << result_row
    }

    return results
end


# @private
def sql_field_aggregate(field, operator, alias_name)
    sql_operators = {:sum => "SUM", :count => "COUNT", :average => "AVG", :min => "MIN", :max => "MAX"}

    sql_operator = sql_operators[operator]

    # FIXME: add this to unit testing
    if !sql_operator
        raise RuntimeError, "Unknown aggregation operator '#{operator}'"
    end
        
    expression = "#{sql_operator}(#{field}) AS #{alias_name}"
    return expression
end


end # class Slice
end # module Brewery