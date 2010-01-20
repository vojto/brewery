class Field
@@storage_types = [:string, :integer, :float, :boolean, :date]
@@data_types = [:default, :typeless, :flag, :discrete, :range, 
                :set, :ordered_set]

attr_accessor :name
attr_accessor :storage_type, :data_type
attr_accessor :missing_values

def self.storage_types
    return @@storage_types
end
def self.data_types
    return @@data_types
end

end
