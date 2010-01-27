require 'rubygems'
require 'sequel'
require 'dataset'

class Datastore
attr_accessor :connection

@@system_tables = [:brew_datasets]

@@datasets_table = :brew_datasets
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

def datasets
    array = Array.new
    @connection[@@datasets_table].each { |row|
        array << dataset_with_id(row[:id])
    }
    return array
end

def cleanup
    datasets.each { |dataset|
        if not dataset.is_cache
            @connection.drop_table(dataset.table_name)
            r = @connection[@@datasets_table].filter(:table_name=>dataset.table_name)
            r.delete
        end
    }
end

def dataset_with_id(id)
    row = @connection[@@datasets_table].first(:id=>id)
    return Dataset.new(self, row)
end

def crete_temporary_dataset(fields)
    id = @connection[@@datasets_table].insert( :is_cache => false )
    table_name = "brew_dataset_#{id}"
    @connection[@@datasets_table].filter(:id=>id).update(:table_name => table_name)

    ds = dataset_with_id(id)

    table = table_name.to_sym
    if @connection.table_exists?(table)
        @connection.drop_table(table)
    end
	@connection.create_table(table) do
		for i in (0..fields.count-1)
		    field = fields[i]
		    col_name = "f#{i}".to_sym
			column col_name, field.storage_type
		end
	end
	
	return ds
end

end
