require 'source_node'
require 'record'
require 'csv'

class FileSourceNode < SourceNode
attr_accessor :file_fields
attr_accessor :filename
attr_accessor :reads_field_names
attr_accessor :skiped_header_lines_count
attr_accessor :null_value
attr_accessor :field_separator


def initialize(hash = {})
    super(hash)
    @filename = hash["filename"]
    @reads_field_names = hash["reads_field_names"]

end
    
def prepare
    if @reads_field_names
        read_field_names
    end
    
    rebuild_field_map
end

def read_field_names
    @reader = CSV.open(filename, 'r', @field_separator)

    skip_header_lines
    
    # read header
    header = @reader.shift
    
    # Create fields
    @file_fields = Array.new
    
    header.each { |field_name|
        field = Field.new(field_name, :storage_type => :string, :data_type => :default)
        @file_fields << field
    }

    # FIXME: guess types
    
    @reader.close
    
    rebuild_field_map
end

def creates_dataset
    return true
end

def rebuild_field_map
    @field_map = FieldMap.new

    if @file_fields
        @file_fields.each {|field|
            mapping = FieldMapping.new_created(self, field)
            @field_map.add_mapping(mapping)
        }
    end
end

def set_storage_type_for_field(field_name, storage_type)
    field = fields.detect { |f| f.name.to_s == field_name.to_s }
    if field
        field.storage_type = storage_type
    else
        raise "Unknown field #{field_name}"
    end
end

def skip_header_lines
    if @skiped_header_lines_count and @skiped_header_lines_count > 0
        for i in (1..@skiped_header_lines_count)
            reader.shift    
        end
    end
end

def execute
    @reader = CSV.open(filename, 'r', @field_separator)
    skip_header_lines
    
    if @reads_field_names
        @reader.shift
    end

    table = output_pipe.table
    columns = Array.new
    for i in (0..@fields.count-1)
        column[i] = output_pipe.table_column_for_field(@fields[i])
    end


    @reader.each { |line|
        record = Hash.new
    
        # FIXME: map fields and correct storage types
        # FIXME: Add null values
        for i in (0..@fields.count-1)
            record[column[i]] = line[i]
        end
        table.insert(record)
    }
end

def hash_from_row(row)

    return hash
end

def record_from_row(row)
    record = Record.new

    # FIXME: map fields and correct storage types
    # FIXME: Add null values
    record.values = row.collect { |v| v.to_s }
    record.fields = @fields

    return record
end

end
