require 'node'
require 'yaml'

class Stream
attr_reader :nodes
attr_reader :named_nodes
attr_accessor :datastore

def initialize
    @nodes = Array.new
    @named_nodes = Hash.new
    @datasets = Array.new
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
    
    if output_node.input_pipes.count > limit
        raise ArgumentError, "Output node has all #{output_node.input_nodes.count} inputs connected."
    end

    # Create pipe
    pipe = Pipe.new

    input_node.output_pipe = pipe
    input_node.update_output_pipe
    output_node.add_input_pipe(pipe)
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
def run_node_named(node_name)
    node = node_with_name(node_name)
    run_node(node)
end

def run_node(node)
    if not node.kind_of?(TerminalNode)
        raise "Could not execute node of type #{node.class.name}. Only terminal nodes can be executed"
    end

    if not @datastore
        raise RuntimeError, "No datastore set for stream"
    end
    
    # Assume that there is no cycle
    # FIXME: check for cycles

    # FIXME: use execution context

	# prepare node queue

    # Prepare model
    # prepare_node(node)
    # prepare_datasets

    # Execute nodes
    # execute_node_real(node)
    
	
	
    # raise NotImplementedError, "Not implemented"
    
end

def prepare_node(node)
    if node.output_dataset
        # Already visited and prepared
        return
    end

    # Traverse input nodes
    input_nodes = node.input_nodes
    if input_nodes
        input_nodes.each { |input_node|
            prepare_node(input_node)
        }
    end
    
    if node.creates_dataset
        dataset = Dataset.new
        @datasets << dataset
    else # does not create dataset, should be single dataset node
        if node.input_nodes.count > 1
            raise RuntimeError, "If node does not create dataset it should have only one input node"
        end
        
        dataset = node.input_node.output_dataset
    end
    
    node.prepare

    node.output_dataset = dataset
    # puts "==> IN #{node}"
    fields = node.created_fields
    if fields
        dataset.add_fields(fields)
    end
end

def prepare_datasets
    @datasets.each { |dataset|
        @datastore.prepare_dataset(dataset)
    }
end

def execute_node_real(node)
    puts "==> try real #{node}"

    input_nodes = node.input_nodes
    if input_nodes
        input_nodes.each { |input_node|
            puts "==> input #{input_node}"

            execute_node_real(input_node)
        }
    end

    if node.finished
        return
    else
        puts "==> Executing #{node}"
        node.execute
        node.finished = true
    end
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
