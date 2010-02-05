class FieldMap

# Map: input field, input pipe, output field
attr_reader :mappings

def initialize(new_fields = nil)
    @mappings = Array.new
    
    if new_fields
        self.fields = new_fields
    end
end

def fields= (fields)
    @mappings = Array.new
    fields.each { |field|
        @mappings << { :input => field, :output => field }
    }
end

def make_identity
    @mappings.each { |mapping|
        mapping[:output] = mapping[:input]
    }
end

def output_fields
    fields = @mappings.collect { |mapping| mapping[:output] }
    return fields.compact
end

def is_identity_map
    flag = @mappings.detect { |mapping| mapping[:output] != mapping[:input] }
    return flag
end

def add_mapping(input, output, hash = {})
    @mappings << { :input => input, :output => output, :pipe => hash[:pipe] }
end

def field_for_input_field(input_field)
    mapping = @mappings.select { |mapping| mapping[:input] == input_field }.first
    return mapping[:output]
end

def count
    return @mappings.count
end

def add_pipe_fields(pipe)
    pipe.fields.each { |field|
        @mappings << { :input => field, :output => field, :pipe => pipe }
    }
end

def remove_pipe_fields(pipe)
    @mappings = @mappings.select { |mapping| mapping[:pipe] != pipe }
end

end
