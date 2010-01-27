require 'field'
require 'dataset'
require 'class_additions'

class Node

attr_accessor :fields
attr_writer   :label
attr_reader   :generated_fields
attr_reader   :creates_dataset

attr_reader   :input_nodes
attr_reader   :output_node

@@node_label_number = 0

def initialize(hash = {})
    @input_nodes = Array.new
end

def self.node_class_from_type(type)
    class_name =  type.to_s.gsub(/\/(.?)/) { "::#{$1.upcase}" }.gsub(/(?:^|_)(.)/) { $1.upcase }

    class_object = Class.class_with_name(class_name)

    if not class_object
        type = type + "_node"
        class_name =  type.to_s.gsub(/\/(.?)/) { "::#{$1.upcase}" }.gsub(/(?:^|_)(.)/) { $1.upcase }
        class_object = Class.class_with_name(class_name)
        if not class_object
            return nil
        end
    end

    return class_object
end


def self.new_from_hash(hash)
    type = hash["type"]

    class_object = node_class_from_type(type)

    if not class_object.is_kind_of_class(Node)
        raise ArgumentError, "Requested node type object #{class_object} is not kind of Node"
    end
    
    node = class_object.new(hash)
    return node
end

def execute(input_datasets, output_dataset)
    # do nothing by default
end

def fields
    if !@fields
        return Array.new
    else
        return @fields
    end
end

def prepare
    # do nothing
end
def is_terminal
    return false
end
def creates_dataset
    return false
end

def input_limit
    # 2^31-1 - largest 32bit signed integer
    return 2147483647
end

def label
    if !@label
        @label = "Node #{@@node_label_number}"
        @@node_label_number += 1
    end
    return @label
end

def add_input_node(node)
    return if @input_nodes.include?(node)

    if @input_nodes.count >= input_limit
        raise ArgumentError, "No more nodes can be added to this node. Limit #{input_limit}, nodes #{@input_nodes.count}"
    end
    
    @input_nodes << node
end

def remove_input_node(node)
    @input_nodes.delete(node)
end

def set_output_node(node)
    # Check node type
    @output_node = node
end

end
