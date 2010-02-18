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
    
################################################################
# Node specification

def creates_dataset
    return true
end

def created_fields
	return fields
end

def fields
	fields = FieldSet.new(@file_fields)
	return fields	
end


################################################################
# Node properties

def prepare
    if @reads_field_names
        read_field_names
    end
end

def read_field_names
    @reader = CSV.open(@filename, 'r', @field_separator)

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
end

def skip_header_lines
    if @skiped_header_lines_count and @skiped_header_lines_count > 0
        for i in (1..@skiped_header_lines_count)
            reader.shift    
        end
    end
end



end
