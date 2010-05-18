class Dataset
attr_accessor :table_name
attr_accessor :datastore
attr_reader :fields
attr_reader :is_cache
attr_reader :map

def initialize
    @map = Hash.new
    @fields = Array.new
    @columns = Array.new
    @field_counter = 0
end

def table
    return @datastore.connection[@table_name.to_sym]
end

def add_fields(fields)
    existing_fields = @map.keys
    if fields.detect { |f| existing_fields.include?(f.name) }
        raise ArgumentError, "dataset already contains field #{f.name}"
    end
    
    @fields = @fields + fields
    
    fields.each { |field|
        column = create_column_name
        @map[field.name] = column
        puts "--> MAPPING #{field.name} to #{column}"
    }    
end

def create_column_name
    @field_counter = @field_counter + 1
    return "field_#{@field_counter}".to_sym
end
end

