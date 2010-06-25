module Brewery
class FactField
	include DataObjects::Quoting
	include DataMapper::Resource

	property :id, Serial
	property :name, String
	property :label, String
	property :description, Text
	property :is_key, String
	property :field_type, String
	
    belongs_to :cube
    is :list, :scope => :cube_id
    
end # class FactField
end # module Brewery
