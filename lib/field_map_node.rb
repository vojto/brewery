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

    map = FieldMap.new
    
    fields.each { |source_field| 
        if @deleted_fields.include?(source_field.name)
            target_field = nil
        else
            new_name = @renamed_fields[source_field.name]
            target_field = source_field.clone
            if new_name
                target_field.name = new_name
            end
        end
        mapping = FieldMapping.new(input, source_field, target_field)
        map.add_mapping(mapping)
    }
    return map
end

end
