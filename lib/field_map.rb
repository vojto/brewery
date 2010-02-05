require 'field_mapping'

class FieldMap

# Map: input field, input pipe, output field
attr_reader :mappings

def initialize
    @mappings = Array.new
end

def add_fields(source, fields)
    fields.each { |field|
        @mappings << FieldMapping.new_identity(source, field)
    }
end

def add_mapping(mapping)
    @mappings << mapping
end

def output_fields
    fields = @mappings.collect { |mapping| mapping.target_field }
    return fields.compact
end

def is_identity_map
    flag = @mappings.detect { |mapping| mapping.target_field != mapping.source_field }
    return flag
end

def field_for_source_field(source_field)
    mapping = @mappings.select { |mapping| mapping.source_field == source_field }.first
    return mapping.target_field
end

def count
    return @mappings.count
end

end
