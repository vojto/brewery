module Brewery

# Query denormalized view representing cube data
class CubeQuery
def dimension_values_sql(dimension, path)
    create_dimension_values_statement(dimension, path)
    return @dimension_values_statement
end

def dimension_values(dimension, path)
    create_dimension_values_statement(dimension, path)
    dataset = Brewery.workspace.execute_sql(@dimension_values_statement)
    return dataset
end

private
    
def create_dimension_values_statement(dimension, path)
    if !path
        raise ArgumentError, "Path should not be nil"
    elsif !path.is_kind_of_class(Array)
        raise ArgumentError, "Path should be an array"
    end

    ################################################
    # 1. Conditions

    # FIXME: Use more
    dimension = @cube.dimension_object(dimension)
    hierarchy = dimension.default_hierarchy
    last_level = hierarchy.next_level(path)

    conditions = []
    full_levels = []
    path.each_index { |i|
        value = path[i]
        level = hierarchy.levels[i]
        if value == :all
            full_levels << level 
        else
            field = quote_field(field_reference(level.key_field))
            quoted_value = quote_value(path[i])
            conditions << "#{field} = #{quoted_value}"
        end
    }

    # FIXME: chceck correctness of this:
    ref = quote_field(field_reference(last_level.key_field))
    conditions << "#{ref} IS NOT NULL"	

    full_levels << last_level

    ################################################
    # 2. Selections 
    selections = []
    full_levels.each { |level|
        level.level_fields.each { |field|
            selections << quote_field(field_reference(field))
        }
    }
    select_expression = selections.join(', ')

    ################################################
    # 3. Groupings

    groupings = []
    full_levels.each { |level|
        level.level_fields.each { |field|
            ref = field_reference(field)
            groupings << quote_field(field_reference(field))
        }
    }

    group_expression = groupings.join(', ')


    ################################################
    # 4. Create core SQL SELECT statements: summary and standard

    exprs = Array.new
    exprs << "SELECT #{select_expression}"
    exprs << "FROM #{@view_expression}"
    exprs << @join_expression    

    if conditions.count > 0
        condition_expression = conditions.join(' AND ')
        exprs << "WHERE #{condition_expression}"
    end

    exprs << "GROUP BY #{group_expression}"    

    create_order_by_expression(:default_order_by => last_level.key_field)
    exprs << @order_by_expression    

    create_pagination_expression
    exprs << @pagination_expression

    @dimension_values_statement = create_sql_statement(exprs)
end
end # class CubeQuery

end # module Brewery
