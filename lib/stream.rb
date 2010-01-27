require 'node'
require 'yaml'

class Stream
attr_reader :nodes
attr_reader :named_nodes

def initialize
    @nodes = Array.new
    @named_nodes = Hash.new
end

########################################################################
# Node network creation

def add_node(node, name = nil)
    # check for cycles
    if not @nodes.include?(node)
        @nodes << node
        @named_nodes[name] = node
    end
end

def set_node_name(node, name)
    @named_nodes[name] = node
end

def node_with_name(name)
    return @named_nodes[name]
end

# def bypass_node(node)
# FIXME: Not implemented
# end

def add_node_between_nodes(added_node, input_node, output_node)
    if not nodes_connected?(input_node, output_node)
        raise ArgumentError, "Adding node between nodes that are not connected"
    end
    
    input_node.output_node = added_node
    output_node.remove_input_node(input_node)
    output_node.add_input_node(added_node)
    added_node.output_node(output_node)
    added_node.add_input_node(input_node)
end

def connect_nodes(input_node, output_node)
    # FIXME: check for cycles
    
    if not @nodes.include?(input_node)
        raise ArgumentError, "Input node is not part of the stream"
    end
    if not @nodes.include?(output_node)
        raise ArgumentError, "Output node is not part of the stream"
    end
    limit = output_node.input_limit

    if limit == 0
        raise ArgumentError, "Output node does not accept connections"
    end
    
    if output_node.input_nodes.count > limit
        raise ArgumentError, "Output node has all #{output_node.input_nodes.count} inputs connected."
    end

    output_node.add_input_node(input_node)
    input_node.set_output_node(output_node)
end

def disconnect_nodes(input_node, output_node)
    if not nodes_connected?(input_node, output_node)
        return
    end
    
    input_node.output_node = nil
    output_node.remove_input_node(input_node)
end

def nodes_connected?(input_node, output_node)
    return input_node.output_node == output_node
    # FIXME: check output.input_nodes for consistency
end

def remove_node(node)
    node.input_nodes.each { |input|
        input.set_output_node(nil)
    }

    if node.output_node
        node.output_node.remove_input_node(node)
    end
    
    @nodes.delete(node)
    
    @named_nodes.keys.each {|key|
        if @named_nodes[key] == node
            @named_nodes.delete(key)
        end
    }
end

########################################################################
# Execution

def execute_node(node)
    if not node.kind_of?(TerminalNode)
        raise "Could not execute node #{node.lable}. Only terminal nodes can be executed"
    end
    
    # Assume that there is no cycle

    # Prepare model
    prepare_dataset_for_node(node)
    
    # Prepare working tables for branch
    
    raise NotImplementedError, "Not implemented"
    
end

def prepare_dataset_for_node(node)
end

########################################################################
# Stream from file

def read_nodes_from_file(file)
    stream_desc = YAML.load_file(file)

    # Create nodes
    
    nodes = stream_desc["nodes"]
    
    nodes.keys.each { |node_name|
        node = Node.new_from_hash(nodes[node_name])
        add_node(node)
        set_node_name(node, node_name)
    }

    connections = stream_desc["connections"]
    connections.each { |conn|
        source_name = conn["source"]
        target_name = conn["target"]
        
        source_node = node_with_name(source_name)
        if !source_node
            raise ArgumentError "Unknown source node #{source_name}"
        end
        target_node = node_with_name(target_name)
        if !target_node
            raise ArgumentError "Unknown target node #{target_name}"
        end
        connect_nodes(source_node, target_node)
    }
    
end
end
