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
	
	# @workspace = Workspace.default_workspace
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
# dimension::
#   Dimension to be joined.
# fact_key_field::
#   Name of key field in cube dataset/table to be used on join with dimension.
#
def join_dimension(dimension, fact_key_field)
	if !dimension
		raise RuntimeError, "Dimension shoul not be nil"
	end
	
	@joined_dimensions[dimension] = { 
							        :dimension_key => dimension.key_field,
							        :fact_key => fact_key_field}
end

# Provide dimension join information
def dimension_join_info(dimension)
	return @joined_dimensions[dimension]
end

# Return dimension object. If dim is String or Hash then find named dimension.
def dimension_object(dimension)
	# puts "SEARCH DIM #{dimension.class}:#{dimension} (in #{joined_dimensions.keys.count} dims)"
	if dimension.class == String || dimension.class == Symbol
		obj = joined_dimensions.keys.detect { |dim| 
							dim.name == dimension || dim.name == dimension.to_s
						}
		return obj
	else
		return dimension
	end
end

end # class
end # module