module Brewery

#class DimensionLevelField
#    include DataMapper::Resource
#    property :id, Serial
#    property :name, String
#end

class DimensionLevel
include DataMapper::Resource
    property :id, Serial
    property :name, String
    property :level_fields, CommaSeparatedList, :length => 250
    property :label, String
    
    belongs_to    :dimension
    has        n, :hierarchy_levels
#    has        n, :hierarchy_levels, :through => Resource

def key_field
    return level_fields[0]
end

end

end # module