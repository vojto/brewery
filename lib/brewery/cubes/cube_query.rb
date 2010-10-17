module Brewery

# Query denormalized view representing cube data
class CubeQuery
include DataObjects::Quoting


@@sql_operators = {:sum => "SUM", :count => "COUNT", :average => "AVG", :min => "MIN", :max => "MAX"}

# Denormalized fact data view, either view name, table name (for materialized view) or full SELECT statement
# Note: view/table name should contain only valid identifier characters, quoted names (with spaces) are not supported
attr_accessor :view

attr_accessor :order_by
attr_accessor :order_direction
attr_accessor :page
attr_accessor :page_size
attr_accessor :measure

attr_accessor :computed_fields

attr_reader :is_drill_down
attr_reader :has_limit

attr_accessor :view_alias

def initialize(cube, view)
    @view_alias = 'v'
    if view=~ /\w+(\.\w+)?/
        @view_expression = "#{view} AS #{@view_alias}"
    else
        @view_expression = "(#{view}) AS #{@view_alias}"
    end
    @view = view
    @cube = cube
    @cuts = []
    @generated_fields = []
    @measure = nil
end

# @return single fact record statement
def record_sql(detail_id)
    return "SELECT * FROM #{@view_expression} WHERE #{@view_alias}.id = #{detail_id}"
end

# @return single fact record (detail) by id
def record(detail_id)
    dataset = Brewery.workspace.execute_sql(record_sql(detail_id))
    return dataset.first
end

# @return dataset representing all facts (details)
def records_sql
    create_condition_expression
    
    exprs = Array.new
    exprs << "SELECT *"
    exprs << "FROM #{@view_expression}"
    if @condition_expression
        exprs << "WHERE #{@condition_expression}"
    end
    
    create_order_by_expression
    exprs << @order_by_expression

    create_pagination_expression
    exprs << @pagination_expression
    
    exprs.delete_if { |expr| !expr || expr == "" }
    
    statement = create_sql_statement(exprs)
end

# returns enumerable dataset with all records within cube query (filtered, paginated, ordered)
def records
    dataset = Brewery.workspace.execute_sql(records_sql)
    
    # FIXME: should not we return nil instead, if there is no record?
    return dataset
end

# Add a cube cut into query
def add_cut(cut)
    @cuts << cut
end

private

def create_condition_expression
    if !@cuts || @cuts.count == 0
        @condition_expression = nil
        return
    end

    filters = []
    
    @cuts.each { |cut|
        dimension = @cube.dimension_object(cut.dimension)
        if !dimension
            raise RuntimeError, "No cut dimension '#{cut.dimension.name}' in cube '#{@cube.name}'"
        end

        filters << conditions_for_cut(cut)
    }

    @condition_expression = filters.join(" AND ")
end

def create_order_by_expression(options = {})
    if @order_by
        field = field_reference(@order_by)
        if @order_direction
            case @order_direction.to_s.downcase
            when "asc", "ascending"
                direction = "ASC"
            when "desc", "descending"
                direction = "DESC"
            else
                raise ArgumentError, "Unknown order direction '{@order_direction}'"
            end
        else
            direction = "ASC"
        end
        @order_by_expression = "ORDER BY #{quote_field(field)} #{direction}"
    else
        @order_by_expression = ""
    end
end

def create_pagination_expression
    if @page
        @pagination_expression = "LIMIT #{@page_size} OFFSET #{@page * @page_size}"
    else
        @pagination_expression = ""
    end
end

def conditions_for_cut(cut)
    conditions = []

    case cut
    when PointCut
        conditions = Array.new
        level_index = 0

        #FIXME: use more
        dim = @cube.dimension_object(cut.dimension)
        hier = cut.hierarchy

        if !hier
            hier = dim.default_hierarchy

            if !hier
                raise RuntimeError, "Cut dimension '#{dim.name}' has no default hierarchy defined"
            end
        end

        cut.path.each { |level_value|
            if level_value != :all
                level = hier.levels[level_index]
                quoted_field = quote_field(field_reference(level.key_field))
                quoted_value = quote_value(level_value)

                conditions << "#{quoted_field} = #{quoted_value}"	
            end
            level_index = level_index + 1
        }

        cond_expression = conditions.join(" AND ")

        return cond_expression
    when RangeCut
        range_key = cut.dimension.key_field
        if !range_key
            raise ArgumentError, "Dimension has no key field (required for ranged cuts)"
        end
        ref = quote_field(field_reference(range_key))
        cond_expression = "#{ref} BETWEEN #{cut.from_key} AND #{cut.to_key}"	
        return cond_expression
    when SetCut
        raise "Set cut is not yet implemented"
    else
        raise ArgumentError, "Unknown cube cut class #{cut.class}"
    end
end


def field_reference(field_string)
    if @generated_fields.include?(field_string)
        return field_string
    end
    
    ref = @cube.field_reference(field_string)
    # FIXME: raise exception if there is no such field
    return "#{ref[0]}.#{ref[1]}"
end

def quote_field(field)
    return "\"#{field.to_s}\""
end
    
def create_sql_statement(expressions)
    expressions = expressions.dup
    expressions.delete_if { |e| !e || e == '' }
    return expressions.join(" ")
end

end # class CubeQuery

end # module Brewery
