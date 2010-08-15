require 'brewery/core/field_description'

module Brewery
class DatasetDescription
	include DataObjects::Quoting
	include DataMapper::Resource

	property :id, Serial

    # Name of dataset
	property :name, String

    # Name of a table, file or any other object containing structured data
	property :object_name, String

    # Human readable label. If label is not defined, dataset name is used instead.
	property :label, String
	
	# Description of a dataset.
	property :description, Text
	
    # Optional name of datastore where table is stored, can be database name, directory, ...
	property :data_store_name, String
	
    has n, :field_descriptions

    has n, :logical_models, {:through=>DataMapper::Resource}
    
def self.new_from_file(path)
	hash = YAML.load_file(path)
	if !hash
		return nil
	end
	return self.new_from_hash(hash)
end

def self.new_from_hash(hash)
	hash = hash.hash_by_symbolising_keys

    desc = self.new
    desc.name = hash[:name]
    desc.object_name = hash[:object_name]
    desc.label = hash[:label]
    desc.description = hash[:description]
    desc.data_store_name = hash[:data_store_name]
    
    array = hash[:fields]
    if array
        array.each { |f|
            field = FieldDescription.new_from_hash(f)
            desc.field_descriptions << field
        }
    end
    return desc
end

def label
    string = attribute_get(:label)
    if !string
        return name
    end
    return string
end
    
def field_with_name(field_name)
	return field_descriptions.first( :name => field_name )
end

end # class
end # module
