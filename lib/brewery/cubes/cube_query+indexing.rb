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
        path_fields << quote_field(level.key_field)
        level.level_fields.each { |field|
            exprs = []

            path_fields_stmt = path_fields.join(',')
            path_str_stmt = path_fields.join(" || '-' || " )

            selection = ["'#{dimension.name}'", dimension.id, "'#{hierarchy.name}'",
                         "'#{level.name}'", level.id, quote_field(level.key_field), 
                         "'#{field}'", quote_field(field), path_str_stmt, 
                         quote_field(level.description_field)]
            selection_str = selection.join(', ')
            

            exprs << "INSERT INTO #{index_table}"
            exprs << "(dimension, dimension_id, hierarchy, level, level_id, level_key, field, value, path, description_value)"
            exprs << "SELECT #{selection_str}"
            exprs << "FROM #{@view_expression}"
            exprs << "GROUP BY #{path_fields_stmt}, #{quote_field(field)}, #{quote_field(level.description_field)}"
            statement = exprs.join("\n")
            puts "INDEX SQL: #{statement}"
            dataset = Brewery.workspace.execute_sql_no_data(statement)
        }
    }
end
end # class CubeQuery

end # module Brewery
