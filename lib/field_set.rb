# field_set.rb
# brewery
#
# Created by Stefan Urbanek on 18.2.2010.
# Copyright 2010 Knowerce. All rights reserved.

require 'field_filter'

class FieldSet
attr_reader :fields

def initialize(array = nil)
	if array
		@fields = array.clone
	else
		@fields = Array.new
	end
end

def << (field)
	@fields << field
end

def [](ref)
	return @fields[ref]
end

def field_with_name(field_name)
	selection = @fields.select { |field| field.name == field_name }
	return selection.first
end

def each
	@fields.each { |field| yield(field) }
end

def count
	return @fields.count
end

def add_fields_from_fieldset(fieldset)
	@fields += fieldset.fields
end

def filtered_set(filter)
	if not filter
		return self.clone
	end

	filtered_fields = FieldSet.new
	
	@fields.each { |field|
		new_field = filter.filter_field(field)

		if new_field
			filtered_fields << new_field
		end
	}

	return filtered_fields
end

def field_names
	names = @fields.collect { |field| field.name }
	return names
end

end
