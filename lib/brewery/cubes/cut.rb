module Brewery
class Cut
attr_accessor :dimension

def initialize(dimension = nil)
    @dimension = dimension
end

# Create a cut by a point within dimension.
def self.point_cut(dimension, path)
    cut = PointCut.new(dimension)
    cut.path = path
    return cut
end

# Create a cut within a range defined by keys. Can be used for ordered dimensions,
# such as date.
def self.range_cut(dimension, from_key, to_key)
    cut = RangeCut.new(dimension)
    cut.from_key = from_key
    cut.to_key = to_key
    return cut
end

# Cut by a set of values
def self.set_cut(dimension, path_set)
    cut = SetCut.new(dimension)
    cut.path_set = path_set
    return cut
end

# Return SQL condition for a cut
# @api private
def sql_condition(dimension_alias)
    raise RuntimeError, "subclasses should override sql_condition"
end

end # class Cut

class PointCut < Cut
include DataObjects::Quoting

attr_accessor :path

# @api private
def sql_condition(dimension, dimension_alias)
	conditions = Array.new
	level = 0

	path.each { |level_value|
		if level_value != :all
			level_name = dimension.hierarchy[level]
			level_column = dimension.key_field_for_level(level_name)
			quoted_value = quote_value(level_value)

			conditions << "#{dimension_alias}.#{level_column} = #{quoted_value}"	
		end
		level = level + 1
	}
	
	cond_expression = conditions.join(" AND ")
	
	return cond_expression
end
end # class PointCut

class RangeCut < Cut
attr_accessor :from_key
attr_accessor :to_key
# @api private
def sql_condition(dimension, dimension_alias)
    dimension_key = dimension.key_field
    condition = "#{dimension_alias}.#{dimension_key} BETWEEN #{from_key} AND #{to_key}"	
	return condition
end
end # class RangeCut

class SetCut < Cut
attr_accessor :path_set
end # class SetCut

end # module
