require 'field_operation_node'

class DeriveNode < FieldOperationNode

attr_accessor :derived_field_name
attr_accessor :derived_field_type
attr_accessor :derived_value_type # formula, set
attr_accessor :derived_value

################################################################
# Node specification

def created_fields
	fields = FieldSet.new
    fields << Field.new(@derived_field_name,
                            :field_type => @derived_field_type)
	return fields
end

def fields
	fields = FieldSet.new
	fields.add_fields_from_fieldset(all_input_fields)
	fields.add_fields_from_fieldset(created_fields)

	return fields
end

end
