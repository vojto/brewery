require 'node'

class AggregationNode < Node

def initialize
    @aggregations = Array.new
end

def add_aggregation(output_field, input_field, aggregation)
    agg = Hash.new
    agg[:output] = output_field
    agg[:input] = input_field
    agg[:aggregation] = aggregation
    @aggregations << agg
end

def remove_aggregation(output_field)
    @aggregations = @aggregations.reject { |a| a[:output] = output_field }
end

def fields
    fields = Array.new
    @aggregations.each { |agg|
        field = Field.new
        field.name = agg[:output]
        # FIXME: add field types
        fields << field
    }
    return fields
end

def evaluate
    # Ruby evaluation
    
    @records = Array.new
    agg_values = Hash.new
    count = 0
    input_node.each { |record|
        @aggregations.each { |agg|
            input = agg[:input]
            output = agg[:output]
            value = record[input]
            prev_value = agg_values[output]

            case agg[:aggregation]
            when :sum, :average
                if !prev_value
                    prev_value = 0
                end
                agg_values[output] = prev_value + value.to_f
            when :min
                agg_values[output] = [agg_values[output], value].min
            when :max
                agg_values[output] = [agg_values[output], value].max
            else
                # invalid aggregation
            end
        }
        count = count + 1
    }
    
    @aggregations.each { |agg|
        if agg[:aggregation] == :average
            output = agg[:output]
            agg_values[output] = agg_values[output] / count
        end
    }
    
    record = Record.new
    record.fields = self.fields
    
    values = Array.new
    @aggregations.each { |agg|
        values << agg_values[agg[:output]]
    }
    record.values = values

    @records = Array.new
    @records << agg_values
end

def records
    return @records
end

end
