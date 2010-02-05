require 'node'

class AggregateNode < Node

attr_accessor :extensions_as_prefix
attr_accessor :name_extension
attr_accessor :count_field_name
attr_reader   :include_count

def initialize(hash = {})
    super(hash)
    @agg_fields = Hash.new
    @group_fields = Array.new
end

def creates_dataset
    return true
end

def set_field_aggregations(input_field, aggregations)
    # FIXME: check input field type to be numeric
    
    if not input_field
        raise ArgumentError, "Could not set aggregations to nil field"
    end
    
    @agg_fields[input_field] = aggregations

    rebuild_field_map
end

def include_count= (flag)
    if flag != @include_count
        @include_count = flag
        rebuild_field_map
    end
end

def group_fields= (fields)
    @group_fields = fields
    rebuild_field_map
end

def remove_aggregation(output_field)
    @agg_fields.delete[output_field]
    rebuild_field_map
end

def rebuild_field_map
    @field_map = FieldMap.new

    @group_fields.each { |field|
        mapping = FieldMapping.new_created(self, field)
        @field_map.add_mapping(mapping)
    }

    ext = @name_extension

    @agg_fields.each_key {|field|
        aggs = @agg_fields[field]
        aggs.each { |agg|
            if @extension_as_prefix
                ext = "#{ext}_" if ext
                name = "#{agg}#{ext}#{field.name}"
            else
                ext = "_#{ext}" if ext
                name = "#{field.name}#{ext}#{agg}"
            end

            field = Field.new(name, :storage_type => :float,
                                    :field_type => :range)
            mapping = FieldMapping.new_created(self, field)
            @field_map.add_mapping(mapping)
        }
    }

    if include_count
        if count_field_name
            name = count_field_name
        else
            name = "record_count"
        end
        
        field = Field.new(name, :storage_type => :integer,
                                :field_type => :range)
        mapping = FieldMapping.new_created(self, field)
        @field_map.add_mapping(mapping)
    end

end

def created_fields
    return @field_map.output_fields
end

end
