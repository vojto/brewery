require 'rubygems'
require 'sequel'

class DatastoreTable
attr_accessor :table_name
attr_reader :columns

def initialize
    @columns = Array.new
    @field_counter = 0
end

def table
    return @datastore.connection[@table_name.to_sym]
end

def create_columns_for_fields(fields)
    columns = Array.new
    fields.each { |field|
        column = create_column(field)
        columns << column
    }    
    return columns
end

def create_column(field)
    @field_counter = @field_counter + 1
    column_name = "f#{@field_counter}".to_sym
    columns << [column_name, field.storage_type]
    return column_name
end
end

