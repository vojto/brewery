module Brewery
class DatasetJoin
	include DataObjects::Quoting
	include DataMapper::Resource

	property :id, Serial
	property :master_dataset_name, String
	property :master_key, String
	property :detail_dataset_name, String
	property :detail_key, String

	belongs_to :cube
end # class
end # module