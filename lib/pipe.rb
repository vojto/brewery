class Pipe
attr_accessor :fields
attr_accessor :table
attr_reader :column_map

def initialize
    @column_map = Hash.new
end

def field_with_name(name)
    # FIXME: check for name uniqueness
    field = @fields.select { |f| f.name = name }.first
    return field
end

end
