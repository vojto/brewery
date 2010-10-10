module Brewery

# Level of dimension hierarchy
class DimensionLevel
    include DataMapper::Resource
    property :id, Serial

    # Level name
    property :name, String
    # Fields in level. First field is considered level key field.
    property :level_fields, CommaSeparatedList, :length => 250
    # Label used for user display
    property :label, String
    # Dimension field containing long level description (for example: country_name containing value "United Kingdom")
    property :description_field, String
    # Dimension field containing short level description (for example: country_code containing value "UK")
    property :short_description_field, String
    
    belongs_to    :dimension
    has        n, :hierarchy_levels

def description_field
    field = attribute_get(:description_field)

    if !field
        field = level_fields[1]
    end

    if !field
        field = level_fields[0]
    end

    return field
end

def short_description_field
    field = attribute_get(:short_description_field)

    if !field
        field = self.description_field
    end
    return field
end
def key_field
    return level_fields[0]
end

end

end # module