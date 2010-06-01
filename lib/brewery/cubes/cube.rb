module Brewery
class Cube

# Dataset containing facts
attr_reader :dataset
attr_reader :dimensions

# Whole slice
attr_reader :whole

# @private

def initialize
	@dimensions = Hash.new
	@joins = Hash.new
	@whole = Slice.new(self)
end

def dataset=(dataset)
	@dataset = dataset
end

def aggregate(measure, aggregations)
	return @whole.aggregate(measure, aggregations)
end

# Join dimension to cube
#
# == Parameters:
# dimension_name::
#   Name (as symbol) of dimension related to cube.
# dimension_key_field::
#   Name of key field in dimension dataset to be used on join with cube dataset.
# table_key_field::
#   Name of key field in cube dataset/table to be used on join with dimension.
#
def join_dimension(dimension_name, dimension, table_key_field, dimension_key_field)
	@dimensions[dimension_name] = { :dimension => dimension, 
							        :dimension_key => dimension_key_field,
							        :table_key => table_key_field}
end

# Provide dimension join information
def dimension(dimension_name)
	return @dimensions[dimension_name]
end

def slice(dimension, cut_values)
	return @whole.slice(dimension, cut_values)
end

end # class
end # module