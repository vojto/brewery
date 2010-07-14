module Brewery

class FieldDescription
	include DataMapper::Resource

	property :id, Serial
	property :name, String
	property :label, String
	property :description, Text

	property :storage_type, String
	property :format, String
	property :format_arg1, String
	property :format_arg2, String

    belongs_to :dataset_description
    is :list, :scope => :dataset_description_id

def self.new_from_hash(hash)
	hash = hash.hash_by_symbolising_keys

    desc = self.new
    desc.name = hash[:name]
    desc.label = hash[:label]
    desc.description = hash[:description]
    desc.storage_type = hash[:storage_type]
    desc.format = hash[:format]
    desc.format_arg1 = hash[:format_arg1]
    desc.format_arg2 = hash[:format_arg2]
    return desc
end

def label
    string = attribute_get(:label)
    if !string
        return name
    end
    return string
end

end # class FieldDescription

end # module
