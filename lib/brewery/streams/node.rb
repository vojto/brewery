require 'field'
require 'class_additions'
require 'pipe'
require 'field_map'
require 'datastore_table'

class Node

attr_writer   :label
attr_reader   :generated_fields
attr_reader   :creates_dataset
attr_accessor :finished

attr_reader   :field_map
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

def execute
    # do nothing by default
end

def fields
    map = self.field_map

    if !map
        return Array.new
    end

    return map.output_fields
end

def field_with_name(name)
    # FIXME: check for name uniqueness
    field = self.fields.select { |f| f.name = name }.first
    return field
end

def is_terminal
    return false
end
def creates_dataset
    return false
end

def all_input_fields
    fields = Array.new
    for i in 0..input_pipes.count - 1
        pipe = input_pipes[i]
        fields = fields + pipe.fields.collect { |field| [field, i, pipe] }
    end
    return fields
end

def created_fields
    return @field_map.created_fields
end

def rebuild_field_map
    create_identity_field_map
end

def create_identity_field_map
    @field_map = FieldMap.new

    if input_pipes.count > 1
        raise RuntimeError, "Unable to create default identity map for multiple inputs."
    end

    input = input_pipe
    if input
        fields = input.fields
        if fields
            @field_map.add_fields(input, fields)
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
def update_output_pipe
    @output_pipe.fields = self.fields
end

def setup_output_pipe_table
    # should be called only when node is validated
    # expected:
    #    created_fields
    #    input_pipe (only one)
    #    field map
    #    input pipe table
    
    if not @output_pipe
        return
    end
    
    if @input_pipes.count > 1
        raise NotImplementedError, "Unable to setup output pipe with more than one input. Should be overriden in node subclasses"
    end
    
    @output_pipe.fields = self.fields

    output_map = @output_pipe.column_map
    output_map.clear

    if self.creates_dataset
        table = DatastoreTable.new
    else
        table = self.input_pipe.table
    end

    if not self.creates_dataset
        # Remap column map
        input_map = self.input_pipe.column_map
        input_fields = self.input_pipe.fields
    
        input_fields.each { |input_field|
            output_field = @field_map.field_for_source_field(input_field)
            if output_field
                output_map[output_field] = input_map[input_field]
                # ignore deleted fields
            end
        }
    end    
    
    # Add created column map
    created_fields = self.created_fields
    columns = table.create_columns_for_fields(created_fields)

    for i in 0..created_fields.count - 1
        output_map[created_fields[i]] = columns[i]
    end
    @output_pipe.table = table
    # FIXME: notify that pipe has changed (or not, as this is called for all nodes before execution)
end

def input_pipe_added(pipe)
    # do nothing
end

def input_pipe_removed(pipe)
    # do nothing
end
def input_pipes_changed
    rebuild_field_map
    # do nothing
end

end
