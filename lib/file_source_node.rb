require 'source_node'
require 'record'
require 'csv'

class FileSourceNode < SourceNode
attr_accessor :filename
attr_accessor :reads_field_names
attr_accessor :skiped_header_lines_count
attr_accessor :null_value

def initialize(hash = {})
    super(hash)
    @filename = hash["filename"]
    @reads_field_names = hash["reads_field_names"]

end
    
def prepare
    if @reads_field_names
        read_field_names
    end
end

def read_field_names
    @reader = CSV.open(filename, 'r')

    skip_header_lines
    
    # read header
    header = @reader.shift
    
    # Create fields
    @fields = Array.new
    
    header.each { |field_name|
        field = Field.new
        field.name = field_name
        @fields << field
    }

    # FIXME: guess types
    
    @reader.close
end

def creates_dataset
    return true
end

def created_fields
    return @fields
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

def execute(inputs, output)
    @reader = CSV.open(filename, 'r')
    skip_header_lines
    
    # skip header
    
    if @reads_field_names
        @reader.shift
    end

    table = output.table

    @reader.each { |row|
        table.insert(hash_from_row(row))
    }
end

def hash_from_row(row)
    hash = Hash.new

    # FIXME: map fields and correct storage types
    # FIXME: Add null values
    for i in (0..@fields.count-1)
        field = @fields[i].name
        puts "--> #{i} #{field}=#{row[i]}"
        hash[field] = row[i]
    end

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
