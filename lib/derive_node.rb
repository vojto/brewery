require 'field_operation_node'

class DeriveNode < FieldOperationNode

attr_accessor :derived_field_name
attr_accessor :derived_field_type
attr_accessor :derived_value_type # formula, set
attr_accessor :derived_value

def rebuild_field_map
    create_identity_field_map

    field = Field.new(@derived_field_name,
                            :field_type => :default)
    mapping = FieldMapping.new_created(input_pipe, field)
    @field_map.add_mapping(mapping)
    fields = @field_map.mappings.collect { |mapping| mapping.target_field }
end

end
