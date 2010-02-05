require 'node'

class SelectNode < Node

attr_accessor :condition

def creates_dataset
    return true
end

def created_fields
    return @field_map.output_fields
end

def input_limit
    return 1
end

end
