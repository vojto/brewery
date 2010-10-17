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

# Returns SQL statement for aggregation results
def aggregation_summary_sql(options = {})
    create_aggregation_expression(options)
    return @summary_sql_statement
end

def aggregation_drill_down_sql(options = {})
    create_aggregation_expression(options)
    return @drill_sql_statement
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

def create_aggregation_expression(options = {})
    create_condition_expression

    # FIXME: unify with other selections
    @selected_fields = {}

    ################################################
    # 0. Prepare

    if options[:row_dimension]
        row_dimension = @cube.dimension_object(options[:row_dimension])
    else
        row_dimension = nil
    end

    row_levels = options[:row_levels]

    if row_levels
        @is_drill_down = true
    else
        @is_drill_down = false
    end

    if row_levels && row_levels.class != Array
        raise RuntimeError, "Row levels should be an array"
    end

    ################################################
    # 1. Select aggregations

    selections = Array.new

    if @measure
        if options[:aggregations]
            @aggregations = options[:aggregations]
        else
            @aggregations = [:sum]
        end
    
        aggregated_fields = Hash.new
        @aggregations.each { |agg|
            field = aggregated_field_name(@measure, agg)
            aggregated_fields[agg] = field
            selections << aggregate_field_sql(@measure, agg, field)
            @generated_fields << field
        }
    end
    
    @generated_fields << "record_count"
    selections << "COUNT(1) AS record_count"

    ################################################
    # 2. Select Fields

    # FIXME: Unify with create_select_expression
    row_selections = selections.dup
    if @is_drill_down
        row_levels.each{ |level|
            level_fields = row_dimension.fields_for_level(level)
            level_fields.each { |field|
                row_selections << quote_field(field_reference(field))
            }
        }
    end

    ################################################
    # 3. Grouping and ordering

    group_fields = Array.new
    if @is_drill_down
        row_levels.each { | level_name |
            level = row_dimension.level_with_name(level_name)
            level.level_fields.each { |field| 
                group_fields << quote_field(field_reference(field))
            }
        }

        create_order_by_expression
    end
    group_expression = group_fields.join(', ')

    ################################################
    # 4. Create core SQL SELECT statements: summary and standard

    select_expression = selections.join(', ')
    summary_exprs = Array.new
    summary_exprs << "SELECT #{select_expression}"
    summary_exprs << "FROM #{@view_expression}"
    summary_exprs << @join_expression

    if @condition_expression
        summary_exprs << "WHERE #{@condition_expression}"
    end

    @summary_sql_statement = create_sql_statement(summary_exprs)

    if @is_drill_down
        select_expression = row_selections.join(', ')
        drill_exprs = Array.new
        drill_exprs << "SELECT #{select_expression}"
        drill_exprs << "FROM #{@view_expression}"
        drill_exprs << @join_expression
        if @condition_expression
            drill_exprs << "WHERE #{@condition_expression}"
        end

        drill_exprs << "GROUP BY #{group_expression}"

        if @order_by
            drill_exprs << @order_by_expression
        elsif @is_drill_down
            # FIXME: move to method that creates @order_by_expression
            order_fields = Array.new
            row_levels.each { | level_name |
                level = row_dimension.level_with_name(level_name)
                level.level_fields.each { |field| 
                    # level_key = row_dimension.key_field_for_level(level)
                    order_fields << quote_field(field_reference(field))
                }
            }
            order_expr = order_fields.join(', ')
            drill_exprs << "ORDER BY #{order_expr}"
        end

        # Paginate

        create_pagination_expression
        drill_exprs << @pagination_expression

        @drill_sql_statement = create_sql_statement(drill_exprs)
    end

    
    ################################################
    # 5. Set drill-down limits
    #    Note: we need drill_statement to be able to set limits. The drill down SQL statement
    #          is used as subquery.

    @has_limit = false
    limit = options[:limit]
    if limit
        @has_limit = true
        limit_aggregation = options[:limit_aggregation]
        case limit
        when :top_10
            limit_value = 10
            limit_sort = :top
        else
            limit_value = options[:limit_value]
            limit_sort = options[:limit_sort]
        end
        
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
            # FIXME: is this portable?
            @drill_sql_statement = "SELECT * FROM (#{@drill_statement}) s 
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
        ref = field_reference(range_key)
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

def aggregated_field_name(field, aggregation)
    return "#{field}_#{aggregation}"
end

def aggregate_field_sql(field, operator, alias_name)
    operator = @@sql_operators[operator]

    # FIXME: add this to unit testing
    if !operator
        raise RuntimeError, "Unknown aggregation operator '#{operator}'"
    end
        
    expression = "#{operator}(#{field}) AS #{alias_name}"
    return expression
end

def create_sql_statement(expressions)
    expressions = expressions.dup
    expressions.delete_if { |e| !e || e == '' }
    return expressions.join(" ")
end

end # class CubeViewBuilder

end # module Brewery
