require 'node'
require 'record'
require 'csv'

class FileSourceNode < Node
attr_accessor :filename
attr_accessor :reads_field_names
attr_accessor :skiped_header_lines_count

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

def skip_header_lines
    if @skiped_header_lines_count and @skiped_header_lines_count > 0
        for i in (1..@skiped_header_lines_count)
            reader.shift    
        end
    end
end

def each
    @reader = CSV.open(filename, 'r')
    skip_header_lines
    
    # skip header
    
    if @reads_field_names
        @reader.shift
    end

    @reader.each { |row|
        yield record_from_row(row)
    }
end

def record_from_row(row)
    record = Record.new

    # FIXME: map fields and correct storage types
    record.values = row.collect {|v| v.to_s }
    record.fields = @fields

    return record
end

end
