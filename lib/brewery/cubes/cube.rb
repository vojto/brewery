require 'brewery/cubes/fact_field'

module Brewery
class Cube
	include DataObjects::Quoting
	include DataMapper::Resource

	property :id, Serial
	property :name, String, :default => 'unnamed cube'
	property :label, String
	property :description, Text
	property :fact_table, String

    has n, :models,  {:through=>DataMapper::Resource}
    has n, :cube_dimension_joins
    has n, :dimensions,  :through => :cube_dimension_joins
    has n, :fact_fields

# FIXME: remove this
attr_accessor :workspace

# @private

def aggregate(measure)
	return @whole.aggregate(measure)
end

def whole
	if !@whole
		@whole = Slice.new(self)
	end
	return @whole
end

# Join dimension to cube
#
# == Parameters:
# dimension::
#   Dimension to be joined.
# fact_key_field::
#   Name of key field in cube dataset/table to be used on join with dimension.
#
def join_dimension(dimension, fact_key_field)
	if !dimension
		raise RuntimeError, "Dimension shoul not be nil"
	end

	join = CubeDimensionJoin.new
	join.fact_key = fact_key_field
	join.dimension_key = dimension.key_field

	self.cube_dimension_joins << join
	dimension.cube_dimension_joins << join
	join.save
end

# Provide dimension join information
def join_for_dimension(dimension)
    # FIXME: flush cache on attribute update
	if !@cached_joins
		@cached_joins = Hash.new
	end
	join = @cached_joins[dimension]
	if !join
		join = cube_dimension_joins.first( :dimension => dimension, :cube => self )
		@cached_joins[dimension] = join
	end
	return join
end

# Return dimension object. If dim is String or Hash then find named dimension.
def dimension_object(dimension)
    case dimension
    when String, Symbol
		obj = dimension_with_name(dimension)
    when Dimension
		obj = dimension
    else
        assert_kind_of "dimension", dimension, Dimension, String, Symbol
    end

	if !obj
		raise RuntimeError, "Cube '#{self.name}' has no joined dimension '#{dimension}'"
	end

	return obj
end

def dimension_with_name(name)
    name = name.to_s
    
	if !@cached_dimensions
		@cached_dimensions = Hash.new
	end
	dim = @cached_dimensions[name]
	if !dim
		dim = dimensions.first( :name => name )
		@cached_dimensions[name] = dim
	end

	return dim
end

def fact(fact_id)
	query = StarQuery.new(self)

	dimensions.each { |dimension|
        join = join_for_dimension(dimension)
		query.join_dimension(dimension, join.dimension_key, join.fact_key)
	}

	return query.record(fact_id)
end

def field_with_name(field_name)
    return fact_fields.first(:name => field_name)
end

def label_for_field(field_name)
    parts = field_name.split('.')
    if parts.count == 1
        field_name = parts[0]
        field = field_with_name(field_name)
        if field
            return field.label
        else
            return nil
        end
    elsif parts.count == 2
        field_name = parts[1]
        dim_name = parts[0]
        dimension = dimension_with_name(dim_name)
        if !dimension
            raise ArgumentError, "No dimension with name '#{dim_name}' in cube '#{name}'"
        end
        return "#{field_name} IN #{dim_name}"
    else
        raise ArgumentError, "Ivnalid field name '#{field_name}' (more than two relationship levels)"
    end
end

end # class
end # module