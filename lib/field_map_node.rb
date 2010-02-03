require 'processing_node'

class FieldMapNode < FieldOperationNode

def initialize(hash = {})
    super(hash)

    # Check for hash types

    if hash[:rename]
        @renamed_fields = hash[:rename]
    else
        @renamed_fields = Hash.new
    end

    if hash[:delete]
        @deleted_fields = hash[:delete]
    else
        @deleted_fields = Array.new
    end
end

def set_field_name(input_field, output_field)
    @renamed_fields[input_field] = output_field    
end

def reset_field_name(input_field)
    @renamed_fields.delete(input_field)
end

def set_field_action(field, action)
    if action == :delete and not @deleted_fields.include?(field)
        @deleted_fields << field
    else # action == :keep or nil
        @deleted_fields.delete(field)
    end
end

def field_map
    input = input_pipe
    if !input
        return nil
    else
        fields = input.fields
        return nil if !fields
    end

    map = Array.new
    
    fields.each { |field| 
        if @deleted_fields.include?(field.name)
            new_field = nil
        else
            new_name = @renamed_fields[field.name]
            if new_name
                new_field = field.clone
                new_field.name = new_name
            else
                new_field = field
            end
        end
        map << [field, new_field]
    }
    
    return map
end

end
