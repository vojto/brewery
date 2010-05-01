require 'field_operation_node'
require 'field_filter'

class FieldFilterNode < FieldOperationNode
attr_accessor :field_filter

################################################################
# Node specification

def initialize(hash = {})
	super(hash)
	@field_filter = FieldFilter.new
end

def instantiate_fields
	@input_fields = all_input_fields
end

def update_fields
	if !@input_fields
		return nil
	end

	fields = @input_fields.filtered_set(@field_filter)

	return fields
end

################################################################
# Node properties

end
