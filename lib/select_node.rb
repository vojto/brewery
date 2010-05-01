require 'node'

class SelectNode < Node

attr_accessor :condition

################################################################
# Node specification

def creates_dataset
    return true
end

def created_fields
	return fields
end

def update_fields
	return all_input_fields.clone
end

def input_limit
    return 1
end

end
