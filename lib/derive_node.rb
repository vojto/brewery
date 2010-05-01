require 'field_operation_node'

class DeriveNode < FieldOperationNode

attr_reader :derived_field_name
attr_reader :derived_field_type
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

def update_fields
	@fields = FieldSet.new
	@fields.add_fields_from_fieldset(all_input_fields)
	@fields.add_fields_from_fieldset(created_fields)

	return fields
end

def derived_field_name=(name)
	@derived_field_name = name
	fields_changed
end

def derived_field_type=(type)
	@derived_field_type = type
	fields_changed
end

end
