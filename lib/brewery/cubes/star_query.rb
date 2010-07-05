module Brewery

# @private
# FIXME: this is quickly written SQL abstraction, requires overall revision
class StarQuery

attr_accessor :fact_table
attr_accessor :order_by
attr_accessor :order_direction
attr_accessor :page
attr_accessor :page_size

def initialize(cube)
    @cube = cube
    @joins = Hash.new
    @fact_alias = 'ft'
    @fact_table = cube.fact_table
    @cuts = []
end

def join_dimension(dimension, dim_key, fact_key)
    if dimension.class != Dimension
        raise ArgumentError, "Dimension object required for join in star query"
    end
    
    @joins[dimension] = { :dimension => dimension, 
                                :dimension_key => dim_key,
                                :fact_key => fact_key }
    
end

def dimensions
    return @joins.collect { | key, value | value[:dimension] }    
end

def create_dimension_aliases
    @dimension_aliases = Hash.new
    i = 0
    dimensions.each { |dim|
        @dimension_aliases[dim] = "d#{i}"
        i += 1
    }
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

def add_cut(cut)
    @cuts << cut
end

def records
    statement = sql_for_records

    # logger.debug "records SQL: #{statement}"

    dataset = Brewery.workspace.execute_sql(statement)
    
#    record = dataset.first
#    hash = {}
#    if record
#        @selected_fields.each { |key, value| 
#            hash[value] = record[key.to_sym]
#        }        
#    end
    # FIXME: should not we return nil instead, if there is no record?
    return dataset
end

def sql_for_detail(detail_id)
    create_dimension_aliases
    
    join_expression = create_join_expressions
    select_expression = create_select_expression    
    
    # FIXME: sanitize id, make key column name configurable (now it is id)
    exprs = Array.new
    exprs << "SELECT #{select_expression}"
    exprs << "FROM #{@fact_table} AS #{@fact_alias} "
    exprs << join_expression
    exprs << "WHERE #{@fact_alias}.id = #{detail_id}"
    
    statement = exprs.join("\n")
    return statement
end

def sql_for_records
    create_dimension_aliases
    
    # FIXME: select expression field names are incosistent with field names for one record
    join_expression = create_join_expressions
    select_expression = create_select_expression    
    where_expression = create_where_expression
    
    exprs = Array.new
    exprs << "SELECT #{select_expression}"
    exprs << "FROM #{@fact_table} AS #{@fact_alias} "
    exprs << join_expression
    exprs << "WHERE #{where_expression}"
    
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
        exprs << "ORDER BY #{field} #{direction}"
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

def create_join_expressions
    expressions = Array.new
    dimensions.each { |dim|
        dim_alias = @dimension_aliases[dim]
        join = @joins[dim]
        dim_field = join[:dimension_key]
        fact_field = join[:fact_key]
        expr = "JOIN "
        expr << "#{dim.table} #{dim_alias} "
        expr << "ON (#{dim_alias}.#{dim_field} = #{@fact_alias}.#{fact_field})"
        expressions << expr
    }
    if expressions.empty?
        return ""
    else
        return expressions.join("\n")
    end
end

def create_select_expression
    @selected_fields = {}
    # FIXME: do this for all fact fields
    selections = ["#{@fact_alias}.id"]

    # 1. cube fields
    @cube.fact_fields.each { |field|
        field_name = field.name
        @selected_fields[field_name] = field_name
        selections << "#{@fact_alias}.#{field_name} AS #{field_name}"
    }
    
    # 2. dimension fields
    dimensions.each { |dim|
        array = []
        i = 0
        dim_alias = @dimension_aliases[dim]
        dim.levels.each { |level|
            level.level_fields.each { |field|
                # field_alias = "#{dim_alias}_#{i}"
                field_alias = "#{dim.name}.#{field}"
                @selected_fields[field_alias] = "#{dim.name}.#{field}"
                selections << "#{dim_alias}.#{field} AS \"#{field_alias}\""
                i += 1
            }
        }
        field = dim.key_field
                field_alias = "#{dim.name}.#{field}"
        @selected_fields[field_alias] = "#{dim.name}.#{field}"
        selections << "#{dim_alias}.#{field} AS \"#{field_alias}\""
    }

    return selections.join(', ')
end

def create_where_expression
	filters = []
	
	@cuts.each { |cut|
		dimension = @cube.dimension_object(cut.dimension)
		if !dimension
		    raise RuntimeError, "No cut dimension '#{cut.dimension.name}' in cube '#{@cube.name}'"
		end

		dim_alias = @dimension_aliases[dimension]
		filters << cut.sql_condition(dimension, dim_alias)
	}

    return filters.join(" AND ")
end

def field_reference(field_string)
    string = field_string.to_s
    split = string.split(".")
    # raise "#{split.class} : '#{split.join('#')}' : '#{field_string}'"
    if split.count == 2
        # format: dim.field => [0] - dimension name, [1] - field name
        dim = @cube.dimension_with_name(split[0])
        dim_alias = alias_for_dimension(dim)
        ref = "#{dim_alias}.#{split[1]}"
    else
        # format: field => [0] - field name
        ref = "#{@fact_alias}.#{split[0]}"
    end
    return ref
end

def alias_for_dimension(dimension)
    return @dimension_aliases[dimension]
end

end # class StarQuery
end # module Brewery
