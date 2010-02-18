# field_filter.rb
# brewery
#
# Created by Stefan Urbanek on 18.2.2010.
# Copyright 2010 Knowerce. All rights reserved.


class FieldFilter
def initialize
	@field_names = Hash.new
	@deleted_fields = Array.new
end

def filter_field(field)
	if @deleted_fields.include?(field.name)
		return nil
	end
	
	new_field = field.clone
	new_name = @field_names[field.name]

	if new_name
		new_field.name = new_name
	end
	
	return new_field
end

def set_field_name(input_name, new_name)
	@field_names[input_name] = new_name
end

def reset_field_name(input_name)
	@field_names.delete(input_name)
end

def set_field_action(input_name, action)
    if action == :delete 
		@deleted_fields << input_name
    elsif action == :keep
		@deleted_fields.delete(input_name)
    end
end

end
