require 'field'

class Node
attr_accessor :input_node
attr_accessor :fields
attr_writer   :label
attr_reader   :issues

@@node_label_number = 0

def initialize
    @issues = Array.new
end

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

def label
    if !@label
        @label = "Node #{@@node_label_number}"
        @@node_label_number += 1
    end
    return @label
end

end
