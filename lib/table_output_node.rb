require 'node'
require 'csv'

class TableOutputNode < Node
attr_accessor :table_name
attr_accessor :mode # :append, :overwrite
attr_accessor :create_table
attr_accessor :connection

def check_field_storage_types
    field = fields.detect { |field|
                                not field.storage_type \
                                or not Field.storage_types.include?(field.storage_type) }
    if field
        return false
    else
        return true
    end
end

def prepare
    @fields = input_node.fields
end

def create_or_replace_table
    if @connection.table_exists?(@table_name)
        @connection.drop_table(@table_name)
    end

    fields = @fields
	@connection.create_table(@table_name) do
		fields.each do |field|
			column field.name, field.storage_type
		end
	end
end

def evaluate
    # require: table
    # require: fields
    # optional: mode
    
    if create_table
        if !check_field_storage_types
            raise "field storage types are unknown or invalid"
        end
        create_or_replace_table
    end    
    # Create table if necessary

    table = @connection[table_name.to_sym]

    input_node.each do |record|
        values = record.values
        table.insert(values)
    end
end

end
