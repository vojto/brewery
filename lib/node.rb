require 'field'
require 'dataset'
require 'class_additions'
require 'pipe'
require 'field_map'

class Node

attr_writer   :label
attr_reader   :generated_fields
attr_reader   :creates_dataset
attr_accessor :finished

# Stream
attr_reader   :input_pipes
attr_reader   :output_pipe

@@node_label_number = 0

def initialize(hash = {})
    @input_pipes = Array.new
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
    created_fields = self.created_fields
    map = self.field_map

    if map
        stream_fields = map.output_fields
    end

    if not stream_fields and not created_fields
        return nil
    end
    
    stream_fields = Array.new if not stream_fields
    created_fields = Array.new if not created_fields

    fields = stream_fields + created_fields
    
    return fields.compact
end

def is_terminal
    return false
end
def creates_dataset
    return false
end
def created_fields
    return Array.new
end
def field_map
    return field_identity_map
end

def all_input_fields
    fields = Array.new
    for i in 0..input_pipes.count - 1
        pipe = input_pipes[i]
        fields = fields + pipe.fields.collect { |field| [field, i, pipe] }
    end
    return fields
end

def field_identity_map
    input = input_pipe
    if !input
        return nil
    else
        fields = input.fields
        if !fields
            return nil
        else
            return FieldMap.new(fields)
        end
    end
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

def prepare
    # Do nothing, make it node specific
end

def add_input_pipe(pipe)
    return if @input_pipes.include?(pipe)

    if @input_pipes.count >= input_limit
        raise ArgumentError, "No more pipes can be added to this node. Limit #{input_limit}, pipes #{@input_pipes.count}"
    end
    
    @input_pipes << pipe
    
    input_pipe_added(pipe)
    input_pipes_changed
end

def remove_input_pipe(pipe)
    @input_pipes.delete(pipe)
    input_pipe_removed(pipe)
    input_pipes_changed
end

def input_pipe
    if @input_pipes
        return @input_pipes[0]
    else
        return nil
    end
end

def output_pipe=(pipe)
    # Check node type
    @output_pipe = pipe
end

def input_pipe_added(pipe)
    # do nothing
end

def input_pipe_removed(pipe)
    # do nothing
end
def input_pipes_changed
    # do nothing
end

end
