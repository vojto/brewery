module Brewery

# Query denormalized view representing cube data
class CubeQuery

def aggregation_summary
    dataset = Brewery.workspace.execute_sql(@summary_sql_statement)
    return dataset.first
end

# Returns SQL statement for aggregation results
def aggregation_summary_sql(options = {})
    create_aggregation_statements(options)
    return @summary_sql_statement
end

def aggregate_drill_down_rows
    dataset = Brewery.workspace.execute_sql(@drill_sql_statement)

    sum_field_name = aggregated_field_name(@measure, :sum)
    sum_field = sum_field_name.to_sym

    @row_sum = 0
    @rows = Array.new
    dataset.each { |record|
        result_row = record.dup

        # Add computed fields
        if @computed_fields && !@computed_fields.empty?
            @computed_fields.each { |field, block|
                result_row[field] = block.call(result_row)
            }
        end

        # FIXME: use appropriate type (Sequel SQLite returns String)
        value = result_row[sum_field]
        if value.class == String
            value = value.to_f
        end
        @row_sum += value
        @rows << result_row
    }
end

def aggregation_drill_down_sql(options = {})
    create_aggregation_statements(options)
    return @drill_sql_statement
end

def create_aggregation_statements(options = {})
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
            @drill_sql_statement = "SELECT * FROM (#{@drill_sql_statement}) s 
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

private

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

end # class CubeQuery

end # module Brewery
