module Brewery

# Query denormalized view representing cube data
class CubeQuery

def initialize(cube, view_name)
    @view_name = view_name
    @cube = cube
    @cuts = []
    @generated_fields = []
end

# @return single fact record (detail) by id
def record(detail_id)
    statement = "SELECT * FROM #{@view_name} WHERE id = #{detail_id}"
    dataset = Brewery.workspace.execute_sql(statement)
    return dataset.first
end

# @return dataset representing all facts (details)
def records
    statement = sql_for_records

    create_condition_expression
    
    exprs = Array.new
    exprs << "SELECT *"
    exprs << "FROM #{@view_name} AS #{@fact_alias} "
    if @condition_expression
        exprs << "WHERE #{@condition_expression}"
    end
    
    create_order_by_expression
    exprs << @order_by_expression

    create_pagination_expression
    exprs << @pagination_expression
    
    statement = exprs.join("\n")

    dataset = Brewery.workspace.execute_sql(statement)
    
    # FIXME: should not we return nil instead, if there is no record?
    return dataset
end

end # class CubeViewBuilder

end # module Brewery
