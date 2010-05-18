require 'node'

class SourceNode < Node
def is_terminal
    return false
end

def creates_dataset
    return true
end

def input_limit
    return 0
end

end
