require 'data_objects'

module Brewery

class Slice
include DataObjects::Quoting

# FIXME: validate
attr_accessor :cuts

# FIXME: depreciated
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

def cut_by(cut)
	slice = self.dup
	slice.add_cut(cut)
	return slice
end

def cut_by_point(dimension, path)
	return self.cut_by(Cut.point_cut(dimension, path))
end

def cut_by_range(dimension, from_key, to_key)
	return self.cut_by(Cut.range_cut(dimension, from_key, to_key))
end


def add_cut(cut)
	@cuts << cut
end

def slice(dimension, values)
    slice = self.dup
    slice.cut_values[dimension] = values
    return slice
end

def dataset
    @cube.dataset
end

def aggregate(measure)
    ################################################
    # Prepare selections

	# FIXME
	aggregations = [:sum]

	expressions = Array.new
    i = 0
    aggregations.each { |operator|
        expressions << sql_field_aggregate(measure, operator, "agg_#{i}")
        i = i+1
    }
    
    expressions << "COUNT(1) AS record_count"

    select_expression = expressions.join(', ')
    
    fact_table = @cube.dataset.table_name
    statement = "SELECT #{select_expression} FROM #{fact_table} f"

    joins = Array.new
    conditions = Array.new

    i = 0
    @cut_values.keys.each { |cut_dimension|
        # puts "CUT: #{cut_dimension}"
        dim_alias = "d#{i}"
        i += 1

        cut_values = @cut_values[cut_dimension]

        join_info = @cube.dimension_join(cut_dimension)
        dimension = join_info[:dimension]
        dim_table = dimension.dataset.table_name

        sql_join = dimension.sql_join_expression(join_info[:dimension_key],
                                                 join_info[:table_key],
                                                 dim_alias, 'f')

        joins << sql_join
        
        sql_condition = dimension.sql_condition(cut_values, dim_alias)
        conditions << sql_condition
    }

    if @cut_values.keys.count > 0
        join_expression = joins.join(' ')
        condition_expression = conditions.join(' AND ')
        statement = statement +" #{join_expression} WHERE #{condition_expression}"
    end 
    
    # puts "SQL: #{statement}"
    
    # FIXME: This is very ugly hack, Sequel dependent
    selection = @cube.dataset.connection[statement].first

    i = 0
    result = Hash.new
    
    aggregations.each { |agg|
        field = "agg_#{i}".to_sym
        value = selection[field]

        # FIXME: use appropriate type (Sequel SQLite returns String)
        if value.class == String
            value = value.to_f
        end
        result[agg] = value
    
        i = i+1
    }
    
    # FIXME: use appropriate type (Sequel returns String)
    value = selection[:record_count]
    if value.class == String
        value = value.to_f
    end
    result[:record_count] = value

    return result
end

def drill_down_aggregate(drill_dimension_name, level, measure, aggregations)
    # FIXME: validate: existence of drill_dimension_name, measure, aggregations
    
    drill_join_info = @cube.dimension_join(drill_dimension_name)
    drill_dimension = drill_join_info[:dimension]
    drill_level_fields = drill_dimension.levels[level]
    table_alias = "f"

    selections = Array.new
    joins = Array.new
    conditions = Array.new

    fact_table = @cube.dataset.table_name


    ################################################
    # Prepare selections

    i = 0
    aggregations.each { |operator|
        selections << sql_field_aggregate(measure, operator, "agg_#{i}")
        i = i+1
    }
    selections << "COUNT(1) AS record_count"

    ################################################
    # Prepare cuts
    joined_dimensions = Hash.new
    
    i = 0
    @cut_values.keys.each { |cut_dimension|
        dim_alias = "d#{i}"
        i += 1
    
        joined_dimensions[cut_dimension] = dim_alias
        cut_values = @cut_values[cut_dimension]

        # FIXME: check that dim is within same connection
        join_info = @cube.dimension_join(cut_dimension)
        dimension = join_info[:dimension]
        dim_table = dimension.dataset.table_name

        sql_join = dimension.sql_join_expression(join_info[:dimension_key],
                                                 join_info[:table_key],
                                                 dim_alias, table_alias)

        joins << sql_join

        condition = dimension.sql_condition(cut_values, dim_alias)
        conditions << condition
    }

    ################################################
    # Append drill-down (group by) dimension

    drill_dim_alias = joined_dimensions[drill_dimension_name]

    if !drill_dim_alias
        join_info = @cube.dimension_join(drill_dimension_name)
        drill_dim_alias = "d#{i}"
        sql_join = drill_dimension.sql_join_expression(join_info[:dimension_key],
                                                       join_info[:table_key],
                                          			   drill_dim_alias, table_alias)
        joins << sql_join
    end

     if drill_level_fields && ! drill_level_fields.empty?
         
         drill_select_fields = drill_level_fields.collect { |level|
                 "#{drill_dim_alias}.#{level}"
         }
     
         drill_selection = drill_select_fields.join(', ')
     else
         drill_selection = nil
     end
     
     select_expression = "#{selections.join(', ')}"
 	 if drill_selection
	     select_expression = "#{select_expression}, #{drill_selection}"
 	 end
 	 
     if joins.count > 0
         join_expression = joins.join(' ')
     else
 		join_expression = ""
     end
    
    if conditions.count > 0
        condition_expression = "WHERE " + conditions.join(' AND ')
	else
		condition_expression = ''
    end 

    if drill_select_fields.count > 0
		drill_fields_str = drill_select_fields.join(', ')
        group_expression = "GROUP BY #{drill_fields_str}"
        order_expression = "ORDER BY #{drill_fields_str}"
	else
        group_expression = ""
        order_expression = ""
    end

    statement = "SELECT #{select_expression} FROM #{fact_table} #{table_alias}
    				#{joins.join(' ')}
    				#{condition_expression} #{group_expression} #{order_expression}"
    
    # puts "DRILL SQL: #{statement}"
    # FIXME: This is very ugly hack, Sequel dependent
    selection = @cube.dataset.connection[statement]
    results = Array.new
    selection.each { |record|
    
        i = 0
        result = Hash.new
        
        aggregations.each { |agg|
            field = "agg_#{i}".to_sym
            value = record[field]
    
            # FIXME: use appropriate type (Sequel SQLite returns String)
            if value.class == String
                value = value.to_f
            end
            result[agg] = value
        
            i = i+1
        }
        
        drill_level_fields.each { |level|
            result[level] = record[level]

        }
        value = record[:record_count]
        if value.class == String
            value = value.to_f
        end
        result[:record_count] = value

        results << result
    }
    
    # FIXME: use appropriate type (Sequel returns String)

    return results
end

# FIXME: make this THE aggregation method
# == Options:
# * row_dimension - dimension used for row grouping
# * row_levels - group by dimension levels
# * limit_type - :value, :percent, :rank
# * limit - limit value based on limit_type 
# * limit_sort - :ascending, :descending
# == Examples:
# * aggregate_new(:amount, { :row_dimension => dimension, :row_levels => levels} )
def aggregate_new_sql(measure, options = {})
	row_dimension_name = options[:row_dimension]
	row_dimension = @cube.workspace.dimension(row_dimension_name)
	row_levels = options[:row_levels]
	

    ################################################
	# 0. Collect tables to be joined

	dimension_aliases = Hash.new

	all_dimensions = Array.new

	dims = @cuts.collect { |cut| cut.dimension }
	all_dimensions.concat(dims)
	if row_dimension
		all_dimensions << row_dimension_name
	end
	
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
    if row_levels
		for i in ( 0..(row_levels.count - 1) )
			level_fields = row_dimension.fields_for_level(row_levels[i])
			level_fields.each { |field|
				selection = "#{row_dimension_alias}.#{field}"
				selections << selection
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
	# x. DO IT!
	puts "SLICE"
	
	select_expr = selections.join(', ')
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

raise "FIXME: Continue here: add joins"

	statement = "SELECT #{select_expr} #{filter_expr} #{group_expr} #{sort_expr}"

	puts "#{statement}"

    # return results
end


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