class Field
@@storage_types = [:unknown, :string, :integer, :float, :boolean, :date]
@@field_types = [:default, :typeless, :flag, :discrete, :range, 
                :set, :ordered_set]

# FIXME: make mutable/non-mutable versions of fields
attr_accessor :name
attr_reader :storage_type, :field_type
attr_reader :missing_values

def initialize(name, hash = {})
    @name = name.to_s
    @storage_type = hash[:storage_type]
    @field_type = hash[:field_type]
    @missing_values = hash[:missing_values]
end


def self.storage_types
    return @@storage_types
end
def self.field_types
    return @@field_types
end

end
