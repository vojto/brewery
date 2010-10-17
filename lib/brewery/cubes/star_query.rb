require "set"

module Brewery

# @private
# FIXME: this is quickly written SQL abstraction, requires overall revision
# FIXME: desperately requires refactoring!

class StarQuery
include DataObjects::Quoting

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
    @generated_fields = []
end

def create_detail_select_expression
    @selected_fields = {}

    selections = ["#{@fact_dataset_name}.id"]

    # 1. cube fields
    # @cube.fact_fields.each { |field|
    @cube.fact_dataset.field_descriptions.each { |field|
        field_name = field.name
        @selected_fields[field_name] = field_name
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

def record(detail_id)
    statement = sql_for_detail(detail_id)

    # logger.info "detail SQL: #{statement}"

    dataset = Brewery.workspace.execute_sql(statement)
    
    return dataset.first
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
    create_detail_select_expression    

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
    create_detail_select_expression    
    
    # FIXME: select expression field names are incosistent with field names for one record
    create_condition_expression
    
    exprs = Array.new
    exprs << "SELECT #{@select_expression}"
    exprs << "FROM #{@fact_table_name} AS #{@fact_alias} "
    exprs << @join_expression
    if @condition_expression
        exprs << "WHERE #{@condition_expression}"
    end
    
    create_order_by_expression
    exprs << @order_by_expression

    create_pagination_expression
    exprs << @pagination_expression
    
    statement = exprs.join("\n")
    return statement
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
        @order_by_expression = "ORDER BY #{field} #{direction}"
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
        @generated_fields << field
    }
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
                # @selected_fields[field_alias] = field
                ref = field_reference(field)
                row_selections << "#{ref} \"#{ref}\""
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

        create_pagination_expression
        drill_exprs << @pagination_expression

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

def dimension_values_at_path(dimension, path)
    create_join_expression

    ################################################
    # 1. Conditions

    # FIXME: Use more
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
            ref = field_reference(level.key_field)
            quoted_value = quote_value(path[i])
            conditions << "#{ref} = #{quoted_value}"	
        end
    }

    # FIXME: chceck correctness of this:
    ref = field_reference(last_level.key_field)
    conditions << "#{ref} IS NOT NULL"	

    full_levels << last_level

    ################################################
    # 2. Selections 
    selections = []
    full_levels.each { |level|
        level.level_fields.each { |field|
            ref = field_reference(field)
            selections << "#{ref} \"#{ref}\""
        }
    }
    select_expression = selections.join(', ')

    ################################################
    # 3. Groupings

    groupings = []
    full_levels.each { |level|
        level.level_fields.each { |field|
            ref = field_reference(field)
            groupings << field_reference(field)
        }
    }

    group_expression = groupings.join(', ')


    ################################################
    # 4. Create core SQL SELECT statements: summary and standard

    exprs = Array.new
    exprs << "SELECT #{select_expression}"
    exprs << "FROM #{@fact_table_name} AS #{@fact_alias} "
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

    statement = exprs.join("\n")

    puts "DIM VALUES SQL: #{statement}"
    dataset = Brewery.workspace.execute_sql(statement)

    return dataset
end
def create_dimension_field_index(index_table, dimension, level, field)
    create_join_expression
    
    exprs = []
    
    exprs << "INSERT INTO #{index_table}"
    exprs << "(dimension, dimension_id, level, level_id, level_key, field, value)"
    exprs << "SELECT '#{dimension.name}', #{dimension.id}, '#{level.name}', #{level.id}, #{level.key_field}, '#{field}', #{field}"
    exprs << "FROM #{@fact_table_name} AS #{@fact_alias} "
    exprs << @join_expression    
    exprs << "GROUP BY #{level.key_field}, #{field}"
    statement = exprs.join("\n")
    # puts "INDEX SQL: #{statement}"
    dataset = Brewery.workspace.execute_sql_no_data(statement)
end

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

    create_join_expression

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
            exprs << "FROM #{@fact_table_name} AS #{@fact_alias} "
            exprs << @join_expression    
            exprs << "GROUP BY #{path_fields_stmt}, #{field}, #{level.description_field}"
            statement = exprs.join("\n")
            # puts "INDEX SQL: #{statement}"
            dataset = Brewery.workspace.execute_sql_no_data(statement)
        }
    }
end

def dimension_detail_at_path(dimension, path)
    create_join_expression

    ################################################
    # 1. Conditions

    # FIXME: Use more
    hierarchy = dimension.default_hierarchy

    conditions = []
    full_levels = []
    path.each_index { |i|
        value = path[i]
        level = hierarchy.levels[i]
        if ! level
            raise RuntimeError, "No level number #{i} (count: #{hierarchy.levels.count}) in dimension #{dimension.name} hirerarchy #{hierarchy.name}. Path: #{path}"
        end
        
        if value == :all
            full_levels << level 
        else
            ref = field_reference(level.key_field)
            quoted_value = quote_value(path[i])
            conditions << "#{ref} = #{quoted_value}"	
        end
    }

    full_levels << hierarchy.next_level(path)

    ################################################
    # 2. Selections 
    selections = []
    hierarchy.levels.each { |level|
        level.level_fields.each { |field|
            ref = field_reference(field)
            selections << "#{ref} \"#{ref}\""
        }
    }
    select_expression = selections.join(', ')

    ################################################
    # 4. Create core SQL SELECT statements: summary and standard

    exprs = Array.new
    exprs << "SELECT #{select_expression}"
    exprs << "FROM #{@fact_table_name} AS #{@fact_alias} "
    exprs << @join_expression    

    if conditions.count > 0
        condition_expression = conditions.join(' AND ')
        exprs << "WHERE #{condition_expression}"
    end
    
    statement = exprs.join("\n")

    puts "DTETAIL VALUE SQL: #{statement}"
    dataset = Brewery.workspace.execute_sql(statement)

    return dataset.first
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

        filters << filter_condition_for_cut(cut)
    }

    @condition_expression = filters.join(" AND ")
end

def filter_condition_for_cut(cut)
    conditions = []
    
    case cut
    when PointCut
        conditions = Array.new
        level_index = 0
    
        #FIXME: use more
        hier = cut.hierarchy
    
        if !hier
            raise RuntimeError, "Cut or dimension has no hierarchy defined"
        end
    
        cut.path.each { |level_value|
            if level_value != :all
                level = hier.levels[level_index]
                ref = field_reference(level.key_field)
                quoted_value = quote_value(level_value)
    
                conditions << "#{ref} = #{quoted_value}"	
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
        raise "Unhandled set cut"
    else
        raise ArgumentError, "Unknown cube cut class #{cut.class}"
    end
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

def table_for_dataset(dataset_name)
    dataset = @cube.logical_model.dataset_description_with_name(dataset_name)
    table = dataset.object_name
    return table
end

def field_reference(field_string)
    if @generated_fields.include?(field_string)
        return field_string
    end
    
    ref = @cube.field_reference(field_string)
    # FIXME: raise exception if there is no such field
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
