module Brewery

# Represents a dataset field 

class Field
@@storage_types = [:unknown, :string, :text, :integer, :float, :boolean, :date]
@@field_types = [:default, :typeless, :flag, :discrete, :range, 
                :set, :ordered_set]

# Field identifier (column name in a table)
attr_reader :name
# Normalized data storage type. The data storage type is abstracted.
# @todo explain difference between storage type and database data type 
attr_reader :storage_type
# Data store/database dependent storage type - this is the real name of data type as used in
# database where the fields comes from or where the field is going to be created.
attr_reader :concrete_storage_type
# Type of the field from analytical perspective
attr_reader :field_type
# Array of values that represent missing values in the dataset for given field
attr_reader :missing_values

# Create a field from hash. Hash keys are attributes of Field class: name, storage_type, field_type,
# missing_values
def initialize(hash = {})
    hash = hash.hash_by_symbolising_keys
    @name = hash[:name]
    
    if !@name
        raise ArgumentError, "No name specified for new field"
    end
    
    @storage_type = hash[:storage_type]
    @storage_type = @storage_type ? @storage_type.to_sym : :unknown
    
    @field_type = hash[:field_type]
    @field_type = @field_type ? @storage_type.to_sym : :default

    @missing_values = hash[:missing_values]
end

# Return list of known storage types
def self.storage_types
    return @@storage_types
end

# Return list of known field types
def self.field_types
    return @@field_types
end

end # class Field

end # module Brewery

