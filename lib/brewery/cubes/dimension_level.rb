module Brewery

class DimensionLevel
attr_accessor :name
attr_accessor :fields
attr_accessor :label
attr_accessor :field_labels

def initialize(hash = {})
    # FIXME: check types and validity
    # FIXME: convert to symbols
    @fields = hash[:fields]
    @label = hash[:label]
    @field_labels = hash[:field_labels]
    @name = hash[:name]
end

def key_field
    return fields[0]
end

end

end # module