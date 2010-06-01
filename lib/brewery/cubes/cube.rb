module Brewery
class Cube

# Dataset containing facts
attr_reader :dataset
attr_reader :dimensions

# Whole slice
attr_reader :whole

# @private
attr_reader :joins

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

def add_dimension(dimension_name, dimension)
	@dimensions[dimension_name] = dimension
end

def remove_dimension(dimension_name)
	@dimensions.delete(dimension_name)
end

def join_dimension(dimension_name, dimension, table_field, dimension_field)
	@joins[dimension_name] = { :dimension => dimension, 
							   :dimension_field => dimension_field,
							   :table_field => table_field}
end

def slice(dimension, cut_values)
	return @whole.slice(dimension, cut_values)
end

end # class
end # module