module Brewery
class CubeDimensionJoin
	include DataObjects::Quoting
	include DataMapper::Resource

	property :id, Serial
	property :fact_key, String
	property :dimension_key, String

	belongs_to :dimension
	belongs_to :cube
end # class
end # module