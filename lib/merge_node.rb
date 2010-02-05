require 'node'
require 'field_map'

class MergeNode < Node

attr_reader :key_field_names
attr_reader :field_map

def initialize(hash = {})
    super(hash)
    
    @field_map = FieldMap.new
    @key_field_names = Array.new
end

def creates_dataset
    return true
end

def possible_key_fields
    return all_input_fields
end

def created_fields
    raise NotImplementedError
end

def input_pipes_changed
    rebuild_field_map
end

def field_map
    return @field_map
end

def key_field_names= (fields)
    @key_field_names = fields
    rebuild_field_map
end

def rebuild_field_map
    # FIXME: should retain original map!
    @field_map = FieldMap.new
    first_pipe = input_pipe
    @input_pipes.each { |pipe|
        pipe.fields.each { |field|
            if not (pipe != first_pipe and @key_field_names.include?(field.name))
                @field_map.add_mapping(field, field, :pipe => pipe)
            else
            end
        }
    }
end

def sql_statement
    i = 0
    tables = Array.new
    fields = Array.new
    input_pipes.each { |pipe|
        i += 1
        tables << "t#{i}"
    }

    @field_map.mappings.each { |mapping|
        if mapping[:output]
            inf = mapping[:input].name
            outf = mapping[:output].name
            pipe = mapping[:pipe]
            i = input_pipes.index(pipe)
            fields << "#{tables[i]}.#{inf} #{outf}"
        end
    }

    statement = "SELECT #{fields.join(',')} FROM #{tables.join(',')}"
end

end
