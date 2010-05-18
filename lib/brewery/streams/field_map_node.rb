require 'processing_node'

class FieldMapNode < FieldOperationNode

def set_field_name(field, new_name)
    if not field
        raise ArgumentError, "Could not set name of nil field"
    end

    mapping = @field_map.mapping_for_source_field(field)
    mapping.target_field.name = new_name
end

def reset_field_name(field)
    if not field
        raise ArgumentError, "Could not reset nil field"
    end

    mapping = @field_map.mapping_for_source_field(field)
    mapping.target_field.name = mapping.source_field.name
end

def set_field_action(field, action)
    if not field
        raise ArgumentError, "Could not set action to nil field"
    end

    if action == :delete 
        mapping = @field_map.mapping_for_source_field(field)
        mapping.target_field = nil
    elsif action == :keep
        mapping = @field_map.mapping_for_source_field(field)
        mapping.target_field = mapping.source_field.clone
    end
end

end
