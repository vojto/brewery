require 'field_operation_node'

class DeriveNode < FieldOperationNode

attr_accessor :derived_field_name
attr_accessor :derived_field_type
attr_accessor :derived_value_type # formula, set
attr_accessor :derived_value

def prepare_fields
    @field = Field.new(@derived_field_name,
                            :field_type => :default)
end

def created_fields
    if !@field
        prepare_fields
    end
    return [@field]
end

end
