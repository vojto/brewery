require 'node'

class AggregateNode < Node

def initialize(hash = {})
    super(hash)
    @aggregations = Array.new
    @group_fields = Array.new
end

def add_aggregation(aggregation, input_field, output_field_name)
    agg = { :output => output_field_name,
            :input => input_field,
            :aggregation => aggregation }
            
    @aggregations << agg
    rebuild_field_map
end


def group_fields= (fields)
    @group_fields = fields
    rebuild_field_map
end

def remove_aggregation(output_field)
    @aggregations = @aggregations.reject { |a| a[:output] = output_field }
    rebuild_field_map
end

def rebuild_field_map
    @field_map = FieldMap.new

    @group_fields.each { |field|
        mapping = FieldMapping.new_created(self, field)
        @field_map.add_mapping(mapping)
    }

    @aggregations.each {|a|
        input = a[:input]
        out_name = a[:output]
        agg = a[:aggregation]
        in_storage = input.storage_type
        in_type = input.field_type
        
        out_type = in_type
        if [:sum, :average].include?(agg)
            out_type = :range
        end

        if [:min, :max].include?(agg) and [:integer, :float].include(in_storage)
            out_type = :range
        end
        
        field = Field.new(out_name, :storage_type => in_storage, :field_type => out_type)
        mapping = FieldMapping.new_created(self, field)
        @field_map.add_mapping(mapping)
    }
end


end
