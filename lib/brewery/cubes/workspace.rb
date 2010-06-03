module Brewery

class Workspace
@@default_workspace = nil

def self.default_workspace
    if !@@default_workspace
        @@default_workspace = self.new
    end
    return @@default_workspace
end

def initialize
    @dimensions = Hash.new
end

def add_dimension(dimension_name, dimension)
    @dimensions[dimension_name] = dimension
end

def delete_dimension(dimension_name)
    @dimensions.delete(dimension_name)
end

def dimension(dimension_name)
    return @dimensions[dimension_name]
end

end # class Workspace

end # module
