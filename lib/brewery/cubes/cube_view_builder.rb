module Brewery

# Create a denormalized view for a cube. View can be materialized and indexed
class CubeViewBuilder

# Create new builder for a cube
def initialize(cube)
    @cube = cube
    @joins = Hash.new
    @fact_dataset_name = cube.fact_dataset.name
    @fact_table_name = cube.fact_dataset.object_name
    @fact_alias = @fact_dataset_name
    @generated_fields = []
end

# Create SQL view
def create_view(view_name)
    create_select_statement
    statement = "CREATE OR REPLACE VIEW #{view_name} AS #{@select_statement}"
    Brewery.workspace.execute_sql_no_data(statement)
end

# Create materialized view (a table) with appropriate indexes
def create_materialized_view(view_name)
    create_select_statement
    statement = "DROP TABLE IF EXISTS #{view_name}"
    Brewery.workspace.execute_sql_no_data(statement)
    statement = "CREATE TABLE #{view_name} AS #{@select_statement}"
    Brewery.workspace.execute_sql_no_data(statement)

    # CREATE INDEX IDX_CUSTOMER_LAST_NAME on CUSTOMER (Last_Name)
end

# create SQL statement as denormalized star/snowflake cube schema
def create_select_statement
    create_select_expression
    create_join_expression

    exprs = []
    exprs << "SELECT #{@select_expression}"
    exprs << "FROM #{@fact_table_name} AS #{@fact_alias} "
    exprs << @join_expression
    
    @select_statement = exprs.join("\n")
end

# @return statement as denormalized star/snowflake cube schema
def select_statement
    if !@select_statement
        create_select_statement
    end
    return @select_statement
end

private

def create_select_expression
    selections = []
    @cube.fact_dataset.field_descriptions.each { |field|
        field_name = field.name
        selections << "#{@fact_dataset_name}.#{field_name} AS #{field_name}"
    }
    
    # 2. dimension fields
    @cube.dimensions.each { |dim|
        array = []
        i = 0
        dim.levels.each { |level|
            level.level_fields.each { |field|
                ref = field_reference(field)
                selections << "#{ref} AS " + quote_field(ref)
            }
        }
        if dim.key_field
            field = dim.key_field
            ref = field_reference(field)
            selections << "#{ref} AS " + quote_field(ref)
        end
    }

    @select_expression = selections.join(', ')
end

def create_join_expression
    expressions = Array.new

    joins = @cube.joins
    
    joins.each { |join|
        master_table = table_for_dataset(join.master_dataset_name)
        detail_table = table_for_dataset(join.detail_dataset_name)
        master_key = join.master_key
        detail_key = join.detail_key

        expr = "JOIN "
        expr << "#{detail_table} #{join.detail_dataset_name} "
        expr << "ON (#{join.detail_dataset_name}.#{detail_key} = #{join.master_dataset_name}.#{master_key})"
        expressions << expr
        # puts "==> #{expr}"
    }
    if expressions.empty?
        @join_expression = ""
    else
        @join_expression = expressions.join("\n")
    end
end

def quote_field(field)
    return "\"#{field}\""
end

def field_reference(field_string)
    if @generated_fields.include?(field_string)
        return field_string
    end
    
    ref = @cube.field_reference(field_string)
    # FIXME: raise exception if there is no such field
    return "#{ref[0]}.#{ref[1]}"
end
def table_for_dataset(dataset_name)
    dataset = @cube.logical_model.dataset_description_with_name(dataset_name)
    table = dataset.object_name
    return table
end
    
end # class SnowflakeBuilder

end # module Brewery

