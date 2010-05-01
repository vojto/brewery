require 'node'
require 'field_map'

class MergeNode < Node

attr_reader :key_field_names
attr_accessor :field_filter

def initialize(hash = {})
    super(hash)
    
    @key_field_names = Array.new
	@input_tags = Hash.new

	# FIXME: implement field filters per input/input tag
end

################################################################
# Node specification

def creates_dataset
    return true
end
# FIXME: continue here
def created_fields
	fields = FieldSet.new
	
	first_pipe = input_pipe

	# Other fields
    @input_pipes.each { |pipe|
        pipe.fields.each { |field|
            if pipe == first_pipe or not @key_field_names.include?(field.name)
				# not first and not key
				fields << field
            end
        }
    }
	return fields
end

def update_fields
	return created_fields
end


################################################################
# Node properties

def key_field_names= (fields)
    @key_field_names = fields
	fields_changed
end

def possible_keys
	
	keys = input_pipe.fields.field_names
			
	input_pipes.each { |pipe|
		keys = keys & pipe.fields.field_names
	}

    return keys
end

def set_tag_for_input(input, tag)
	@input_tags[input] = tag
end

def tag_for_input(input)
	return @input_tags[input]
end

################################################################
# Execution

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
