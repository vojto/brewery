require "set"

module Brewery

# @private
# FIXME: this is quickly written SQL abstraction, requires overall revision
# FIXME: desperately requires refactoring!

class StarQuery

attr_accessor :fact_table
attr_accessor :order_by
attr_accessor :order_direction
attr_accessor :page
attr_accessor :page_size
attr_accessor :computed_fields

attr_reader :is_drill_down
attr_reader :has_limit

# FIXME: remove/refactor these
attr_reader :rows
attr_reader :row_sum

def initialize(cube)
    @cube = cube
    @joins = Hash.new
    @fact_dataset_name = cube.fact_dataset.name
    @fact_table_name = cube.fact_dataset.object_name
    @fact_alias = @fact_dataset_name
    @cuts = []
end

def table_for_dataset(dataset_name)
    dataset = @cube.logical_model.dataset_description_with_name(dataset_name)
    table = dataset.object_name
    return table
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
        puts "==> #{expr}"
    }
    if expressions.empty?
        @join_expression = ""
    else
        @join_expression = expressions.join("\n")
    end
end

def create_select_expression
    @selected_fields = {}
    # FIXME: do this for all fact fields
    selections = ["#{@fact_dataset_name}.id"]

    # 1. cube fields
    # @cube.fact_fields.each { |field|
    @cube.fact_dataset.field_descriptions.each { |field|
        field_name = field.name
        @selected_fields[field_name] = field_name
        selections << "#{@fact_dataset_name}.#{field_name} AS #{field_name}"
    }
    
    # 2. dimension fields
    dimensions.each { |dim|
        array = []
        i = 0
        dim_alias = @dimension_aliases[dim]
        dim.levels.each { |level|
            level.level_fields.each { |field|
                field_alias = "#{dim.name}.#{field}"
                @selected_fields[field_alias] = "#{dim.name}.#{field}"
                selections << "#{dim_alias}.#{field} AS " + quote_field(field_alias)
            }
        }
        field = dim.key_field
                field_alias = "#{dim.name}.#{field}"
        @selected_fields[field_alias] = "#{dim.name}.#{field}"
        selections << "#{dim_alias}.#{field} AS " + quote_field(field_alias)
    }

    @select_expression = selections.join(', ')
end

def record(detail_id)
    statement = sql_for_detail(detail_id)

    # logger.info "detail SQL: #{statement}"

    dataset = Brewery.workspace.execute_sql(statement)
    
    record = dataset.first
    hash = {}
    if record
        @selected_fields.each { |key, value| 
            hash[value] = record[key.to_sym]
        }        
    end
    # FIXME: should not we return nil instead, if there is no record?
    return hash
end

def records
    statement = sql_for_records

    # logger.debug "records SQL: #{statement}"

    dataset = Brewery.workspace.execute_sql(statement)
    
    # FIXME: should not we return nil instead, if there is no record?
    return dataset
end

def add_cut(cut)
    @cuts << cut
end

def create_dimension_aliases
    @dimension_aliases = Hash.new
    i = 0
    dimensions.each { |dim|
        @dimension_aliases[dim] = "d#{i}"
        i += 1
    }
end

def sql_for_detail(detail_id)
    create_join_expression
    create_select_expression    

    # FIXME: sanitize id, make key column name configurable (now it is id)
    exprs = Array.new
    exprs << "SELECT #{@select_expression}"
    exprs << "FROM #{@fact_table_name} AS #{@fact_alias} "
    exprs << @join_expression
    exprs << "WHERE #{@fact_alias}.id = #{detail_id}"
    
    statement = exprs.join("\n")
    return statement
end

# @returns SQL to fetch records within given cuts
def sql_for_records
    create_join_expression
    create_select_expression    
    
    # FIXME: select expression field names are incosistent with field names for one record
    create_condition_expression
    
    exprs = Array.new
    exprs << "SELECT #{@select_expression}"
    exprs << "FROM #{@fact_table_name} AS #{@fact_alias} "
    exprs << @join_expression
    if @condition_expression
        exprs << "WHERE #{@condition_expression}"
    end
    
    if @order_by
        create_order_by_expression
        exprs << @order_by_expression
    end

    if @page
        if !@page_size
            raise ArgumentError, "No page size specified"
        end
        exprs << "LIMIT #{@page_size} OFFSET #{@page * @page_size}"
    end
    
    
    statement = exprs.join("\n")
    return statement
end

def create_order_by_expression
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
        @order_by_expression = "ORDER BY #{field} #{direction}"
    else
        @order_by_expression = ""
    end
end

def prepare_for_aggregation(measure, options = {})
    create_join_expression
    create_condition_expression

    # FIXME: unify with other selections
    @selected_fields = {}

    ################################################
    # 0. Prepare

    @measure = measure

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

    if options[:aggregations]
        @aggregations = options[:aggregations]
    else
        @aggregations = [:sum]
    end
    
    aggregated_fields = Hash.new
    @aggregations.each { |agg|
        field = aggregated_field_name(measure, agg)
        aggregated_fields[agg] = field
        selections << sql_field_aggregate(measure, agg, field)
    }

    selections << "COUNT(1) AS record_count"

    ################################################
    # 2. Select Fields

    # FIXME: Unify with create_select_expression
    row_selections = selections.dup
    if @is_drill_down
        row_levels.each{ |level|
            level_fields = row_dimension.fields_for_level(level)
            level_fields.each { |field|
                # @selected_fields[field_alias] = field
                ref = field_reference(field)
                row_selections << field_reference(field)
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
                # level_key = row_dimension.key_field_for_level(level)
                group_fields << field_reference(field)
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
    summary_exprs << "FROM #{@fact_table_name} AS #{@fact_alias} "
    summary_exprs << @join_expression

    if @condition_expression
        summary_exprs << "WHERE #{@condition_expression}"
    end

    @summary_statement = summary_exprs.join("\n")

    if @is_drill_down
        select_expression = row_selections.join(', ')
        drill_exprs = Array.new
        drill_exprs << "SELECT #{select_expression}"
        drill_exprs << "FROM #{@fact_table_name} AS #{@fact_alias} "
        drill_exprs << @join_expression
        if @condition_expression
            drill_exprs << "WHERE #{@condition_expression}"
        end

        drill_exprs << "GROUP BY #{group_expression}"

        if @order_by
            drill_exprs << @order_by_expression
        elsif is_drill_down
            # FIXME: move to method that creates @order_by_expression
            order_fields = Array.new
            row_levels.each { | level_name |
                level = row_dimension.level_with_name(level_name)
                level.level_fields.each { |field| 
                    # level_key = row_dimension.key_field_for_level(level)
                    order_fields << field_reference(field)
                }
            }
            order_expr = order_fields.join(', ')
            drill_exprs << "ORDER BY #{order_expr}"
        end

        # Paginate
        if @page
            limit_statement = "LIMIT #{@page_size} OFFSET #{@page * @page_size}"
            drill_exprs << limit_statement
        end

        @drill_statement = drill_exprs.join("\n")
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
            @drill_statement = "SELECT * FROM (#{@drill_statement}) s 
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

def aggregation_summary
    puts "SUMMARY SQL: #{@summary_statement}"
    dataset = Brewery.workspace.execute_sql(@summary_statement)
    return dataset.first
end

def aggregate_drill_down_rows
    puts "DRILLDOWN SQL: #{@drill_statement}"
    dataset = Brewery.workspace.execute_sql(@drill_statement)

    sum_field_name = aggregated_field_name(@measure, :sum)
    sum_field = sum_field_name.to_sym

    @row_sum = 0
    @rows = Array.new
    dataset.each { |record|
        result_row = record.dup

#         @aggregations.each { |agg|
#             agg_field = aggregated_field_name(@measure, agg).to_sym
#             result_row[agg_field] = record[agg_field]
#         }
# 
#         @selected_fields.each { |key, value| 
#             result_row[value.to_sym] = record[key.to_sym]
#         }        

        # puts "==> ROW #{result_row}"

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

def dimension_field_alias(dimension, field)
    return "#{dimension.name}.#{field}"
end

def quote_field(field)
    return "\"#{field}\""
end

def sql_fact_field_selection(field_name)
    return "#{@fact_alias}.#{field_name} AS #{field_name}"
end

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

        dim_alias = @dimension_aliases[dimension]
        filters << cut.sql_condition(dimension, dim_alias)
    }

    @condition_expression = filters.join(" AND ")
end
def field_reference(field_string)
    ref = @cube.field_reference(field_string)
    return "#{ref[0]}.#{ref[1]}"
end

def aggregated_field_name(field, aggregation)
    return "#{field}_#{aggregation}"
end

def dimension_alias(dimension)
    return @dimension_aliases[dimension]
end

end # class StarQuery
end # module Brewery
