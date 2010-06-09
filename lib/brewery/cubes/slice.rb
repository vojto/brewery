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
	return self.cut_by(Cut.point_cut(@cube.dimension_object(dimension), path))
end

# Cut slice by ordered dimension from point specified by dimension
# keys in from_key to to_key
# @return [Slice] new slice with added cut
# @param [Array] path Dimension point specified by array of values. See {Dimension}
def cut_by_range(dimension, from_key, to_key)
	return self.cut_by(Cut.range_cut(@cube.dimension_object(dimension), from_key, to_key))
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
# @option options [Symbol, Dimension] :row_dimension Dimension used for row grouping
# @option options [Array] :row_levels Group by dimension levels
# @option options [Symbol] :limit Possible values: ':value', `:percent`, `:rank`
# @option options [Symbol] :limit_aggregation Which aggregation is used for determining limit
# @option options [Number] :limit_value Limit value based on limit_type 
# @option options [Symbol] :limit_sort Possible values: `:ascending`, `:descending`
# == Examples:
# * aggregate(:amount, { :row_dimension => [:date], :row_levels => [:year, :month]} )
# @return [Array] list of rows where each row represents a point at row_dimension.
#   If no row dimension is specified, only one summary row is returned.
# @todo implement limits
def aggregate(measure, options = {})
t = Time.now
    if options[:row_dimension]
    	row_dimension = @cube.dimension_object(options[:row_dimension])
    end
    
	row_levels = options[:row_levels]
	
	if row_levels && row_levels.class != Array
		raise RuntimeError, "Row levels should be an array"
	end

	if options[:row_dimension] && !row_dimension
		raise RuntimeError, "Unknown dimension #{options[:row_dimension]} (possibly not joined to cube)"
	end
	
	table_alias = "t"

    ################################################
	# 0. Collect tables to be joined

	dimension_aliases = Hash.new
	all_dimensions = Array.new

	dims = @cuts.collect { |cut| cut.dimension }
	all_dimensions.concat(dims)
	if row_dimension
		all_dimensions << row_dimension
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

	row_dimension_alias = dimension_aliases[row_dimension]

    ################################################
	# 1. What needs to be SELECTed

	selections = Array.new
    # 1.1 Aggregations
	
	operators = [:sum, :average]
    aggregated_fields = Hash.new
    
	for i in ( 0..(operators.count - 1) )
        field = "agg_#{i}"
	    aggregated_fields[operators[i]] = field
        selections << sql_field_aggregate(measure, operators[i], field)
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
	    # puts "CUT #{cut.class}"
		if !cut.dimension
		    raise RuntimeError, "No dimension in cut (#{cut.class}), slicing cube '#{@cube.name}'"
		end
		dimension = @cube.dimension_object(cut.dimension)
		if !dimension
		    raise RuntimeError, "No cut dimension '#{cut.dimension.name}' in cube '#{@cube.name}'"
		end
		dim_alias = dimension_aliases[dimension]
		# puts "==> WHERE COND CUT: #{cut.dimension} DIM: #{dimension} ALIAS: #{dim_alias}"
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

	all_dimensions.each { |dimension|
		dim_alias = dimension_aliases[dimension]
        join = @cube.join_for_dimension(dimension)
        # puts "JOIN FOR DIM: '#{dim_alias}' #{dimension}(#{dimension.class}): #{join.fact_key}=#{join.dimension_key}"

        joins << dimension.sql_join_expression(join.dimension_key,
                                               join.fact_key,
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

	table_name = @cube.fact_table
	if !table_name
	    raise RuntimeError, "No fact table name specified for cube '#{@cube.name}'"
	end
	
	statement = "SELECT #{select_expr}
				FROM #{table_name} #{table_alias}
				#{join_expr}
				#{filter_expr}
				#{group_expr}
				#{sort_expr}"

    ################################################
	# 6. Execute statement

	# puts "SQL: #{statement}"
    if !@cube.dataset
        raise RuntimeError, "No dataset set for cube '#{@cube.name}'"
    end
    
    limit = options[:limit]

    if limit
        limit_aggregation = options[:limit_aggregation]
        limit_value = options[:limit_value]
        limit_sort = options[:limit_sort]

        case limit
        when :rank
            case limit_sort
            when :ascending, :asc, :bottom
                direction = "ASC"
            when :descending, :desc, :top
                direction = "DESC"
            else
                direction = "ASC"
            end
            if !limit_aggregation
                limit_aggregation = :sum
            else
                limit_aggregation = limit_aggregation.to_sym
            end
            
            agg_field = aggregated_fields[limit_aggregation]
            if !agg_field
                raise ArgumentError, "Invalid aggregation '#{limit_aggregation}' to limit"
            end

            if !limit_value
                raise ArgumentError, "Limit value for aggregation rank limit not provided"
            end
            # FIXME: this is not portable
            statement = "SELECT * FROM (#{statement}) s 
                            ORDER BY s.#{agg_field} #{direction} LIMIT #{limit_value}"
        when :percent
            # FIXME: implement :percent limit
            raise NotImplementedError, ":percent limit is not yet implemented"
        when :value
            # FIXME: implement :value limit
            raise NotImplementedError, ":value limit is not yet implemented"
            # "SELECT * FROM (#{statement}) WHERE #{agg_field} #{} LIMIT #{rank}"
        end
    end
puts "==> ELAPSED SETUP: #{Time.now - t}"
t = Time.now
    
    selection = @cube.dataset.connection[statement]
    puts "COUNT: #{selection.count}"
	puts "SQL: #{selection.sql}"

    # @option options [Symbol] :limit_type Possible values: ':value', `:percent`, `:rank`
    # @option options [Symbol] :limit_aggregation Which aggregation is used for determining limit
    # @option options [Number] :limit Limit value based on limit_type 
    # @option options [Symbol] :limit_sort Possible values: `:ascending`, `:descending`

puts "==> ELAPSED EXEC: #{Time.now - t}"

    ################################################
	# 7. Collect results
t = Time.now

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
            result_row[field.to_sym] = record[field.to_sym]
        }

        value = record[:record_count]
        if value.class == String
            value = value.to_f
        end
        result_row[:record_count] = value

        results << result_row
    }
puts "==> ELAPSED COLLECT: #{Time.now - t}"

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