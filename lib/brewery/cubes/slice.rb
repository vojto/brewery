require 'data_objects'

module Brewery

class Slice
include DataObjects::Quoting

# FIXME: validate
attr_accessor :cuts

# Initialize slice instance as part of a cube
def initialize(cube)
    @cube = cube
    @cut_values = Hash.new
end

# Copying contructor, called for Slice#dup
def initialize_copy(*)
  @cut_values = @cut_values.dup
end

def add_cut(dimension, values)
    @cut_values[dimension] = values
end

def remove_cut(dimension)
    @cut_values.delete(dimension)
end

def slice(dimension, values)
    slice = self.dup
    slice.add_cut(dimension, values)
    return slice
end

def dataset
    @cube.dataset
end

def aggregate(measure, aggregations)
    ################################################
    # Prepare selections

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

        join_info = @cube.dimension(cut_dimension)
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
    
    drill_join_info = @cube.dimension(drill_dimension_name)
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
        join_info = @cube.dimension(cut_dimension)
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
        join_info = @cube.dimension(drill_dimension_name)
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