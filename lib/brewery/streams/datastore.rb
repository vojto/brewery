# = datastore.rb - Database datastore for datasets
#
# Copyright (c)2009 Stefan Urbanek, Knowerce, s.r.o.
#

require 'rubygems'
require 'sequel'
require 'dataset'

class Datastore
attr_accessor :connection

@@system_tables = [:brew_datasets]

@@datasets_table = :brew_datasets

@@default_datastore = nil

def self.default_datastore
    # FIXME: use some configuration to get default datastore connection

    if !@@default_datastore
        # Create memory datastore
        connection = Sequel.connect('sqlite:/')
        @@default_datastore = Datastore.new(connection)
    end
    
    return @@default_datastore
end

def initialize(connection)
    @connection = connection
end

def setup
    # Warning: This function is destructive

    if @connection.table_exists?(@@datasets_table)
        @connection.drop_table(@@datasets_table)
    end
    
    @connection.create_table(@@datasets_table) do
        primary_key :id
        string      :table_name
        integer     :is_cache
    end
end

def cleanup
    @connection[@@datasets_table].each { |row|
        if not row[:is_cache]
            table_name = dataset[:table_name]
            @connection.drop_table(dataset[:table_name])
            r = @connection[@@datasets_table].filter(:table_name=>dataset[:table_name])
            r.delete
        end
    }
end

def dataset_table_info(table_name)
    return @connection[@@datasets_table].filter(:table_name => table_name).first
end

def prepare_dataset(dataset)
    info = dataset_table_info(dataset.table_name)
    if info
        return
    end
    
    table_name = create_dataset_table(dataset)
    dataset.table_name = table_name
    dataset.datastore = self
end

def create_dataset_table(dataset)

    puts "==> create table"
    id = @connection[@@datasets_table].insert( :is_cache => false )
    table_name = "brew_dataset_#{id}"
    @connection[@@datasets_table].filter(:id=>id).update(:table_name => table_name)

    table = table_name.to_sym
    if @connection.table_exists?(table)
        @connection.drop_table(table)
    end

    puts "--> new table: #{table}"

    
    map = dataset.map
    fields = dataset.fields
    if fields.count == 0
        raise ArgumentError, "Field count should not be 0"
    end
        
	@connection.create_table(table) do
		for i in (0..fields.count-1)
		    field = fields[i]
		    col_name = map[field.name]
            puts "--> column: #{field} - #{col_name} (field.storage_type)"
			column col_name, field.storage_type
		end
	end
	
	return table_name
end

end
