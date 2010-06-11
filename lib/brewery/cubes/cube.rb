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
    has n, :dimensions,  :through => :cube_dimension_joins
    has n, :cube_dimension_joins

# Dataset containing facts
attr_reader :dataset

attr_accessor :workspace

# @private


def dataset=(dataset)
	@dataset = dataset
end

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

end # class
end # module