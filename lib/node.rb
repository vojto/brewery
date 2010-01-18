require 'field'

class Node
attr_accessor :input_node
attr_accessor :fields

def evaluate
    # do nothing by default
end

def records
    return Array.new
end

def each
    records.each { |record|
        yield record
    }
end

def fields
    if !@fields
        return Array.new
    else
        return @fields
    end
end

end
