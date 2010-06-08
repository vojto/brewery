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
attr_reader :joined_dimensions

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
	return cube_dimension_joins.first( :dimension => dimension, :cube => self )
end

# Return dimension object. If dim is String or Hash then find named dimension.
def dimension_object(dimension)
	# puts "SEARCH DIM #{dimension.class}:#{dimension} (in #{joined_dimensions.keys.count} dims)"
	if dimension.class == String || dimension.class == Symbol
		obj = dimensions.first( :name => dimension )
	else
		obj = dimension
	end

	if !obj
		raise RuntimeError, "Cube '#{self.name}' has no joined dimension '#{dimension}'"
	end
	return obj
end

end # class
end # module