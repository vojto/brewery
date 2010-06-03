module Brewery
class Cube

# Dataset containing facts
attr_reader :dataset
attr_reader :joined_dimensions

attr_accessor :workspace

# Whole slice
attr_reader :whole

# @private

def initialize
	@joined_dimensions = Hash.new
	@joins = Hash.new
	@whole = Slice.new(self)
	
	@workspace = Workspace.default_workspace
end

def dataset=(dataset)
	@dataset = dataset
end

def aggregate(measure)
	return @whole.aggregate(measure)
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
def join_dimension(dimension_name, table_key_field, dimension_key_field)
	if !@workspace.dimension(dimension_name)
		raise RuntimeError, "Dimension #{dimension_name} does not exist in workspace"
	end
	
	@joined_dimensions[dimension_name] = { 
							        :dimension_key => dimension_key_field,
							        :table_key => table_key_field}
end

# Provide dimension join information
def dimension_join(dimension_name)
	return @joined_dimensions[dimension_name]
end

def dimension(dimension_name)
	return @joined_dimensions[dimension_name][:dimension]
end

def slice(dimension, cut_values)
	return @whole.slice(dimension, cut_values)
end

end # class
end # module