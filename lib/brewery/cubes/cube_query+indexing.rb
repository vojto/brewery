module Brewery

# Query denormalized view representing cube data
class CubeQuery
def create_dimension_index(index_table, dimension, a_hierarchy = nil)
    if a_hierarchy.class == Hierarchy
        if a_hierarchy.dimension == dimension
            hierarchy = a_hierarchy
        else
            raise "given hierarchy #{a_hierarchy.name} is not from indexed dimension #{dimension.name}"
        end
    else
        hierarchy = dimension.default_hierarchy
    end

    if !hierarchy
        raise "no hierarchy for dimension '#{dimension.name}'"
    end

    levels = hierarchy.levels
    path_fields = []
    levels.each { |level|
        path_fields << level.key_field
        level.level_fields.each { |field|
            exprs = []

            path_fields_stmt = path_fields.join(',')
            path_str_stmt = path_fields.join(" || '-' || " )

            exprs << "INSERT INTO #{index_table}"
            exprs << "(dimension, dimension_id, hierarchy, level, level_id, level_key, field, value, path, description_value)"
            exprs << "SELECT '#{dimension.name}', #{dimension.id}, '#{hierarchy.name}', '#{level.name}', #{level.id}, #{level.key_field}, '#{field}', #{field}, #{path_str_stmt}, #{level.description_field}"
            exprs << "FROM #{@view_expression}"
            exprs << "GROUP BY #{path_fields_stmt}, #{field}, #{level.description_field}"
            statement = exprs.join("\n")
            # puts "INDEX SQL: #{statement}"
            dataset = Brewery.workspace.execute_sql_no_data(statement)
        }
    }
end
end # class CubeQuery

end # module Brewery
