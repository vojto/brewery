require 'terminal_node'
require 'csv'

class TableOutputNode < TerminalNode
attr_accessor :table_name
attr_accessor :mode # :append, :overwrite
attr_accessor :create_table
attr_accessor :connection

def initialize(hash = {})
    super(hash)
    @table_name = hash["table_name"]
    @mode = hash["mode"]
    @mode = @mode.to_sym if @mode
    @create_table = hash["create_table"]
end

def check_field_storage_types
    c = fields.collect { |field| field.name }
    puts "--> NAMES #{c.join(',')}"
    c = fields.collect { |field| field.storage_type }
    puts "--> STORAGE #{c.join(',')}"
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
    
    if !fields or fields.count == 0
        raise RuntimeError, "No fields for table output"
    end
    
	@connection.create_table(@table_name) do
		fields.each do |field|
			column field.name, field.storage_type
		end
	end
end

def execute

    if !@connection
        raise RuntimeError, "No connection specified for table output"
    end
    
    input_dataset = input_node.output_dataset

    if create_table
        puts "CREATING TABLE"
        if !check_field_storage_types
            raise "field storage types are unknown or invalid"
        end
        create_or_replace_table
    end    
    # Create table if necessary

    table = @connection[table_name.to_sym]

    dataset_table = input_dataset.table

    if mode == :replace
        puts "REPLACE"
        table.delete
    else
        puts "APPEND"
    end

    puts "==> RECORDS FROM #{dataset_table}"
    dataset_table.each do |record|
        puts "--> #{record}"
        values = record.values
        table.insert(values)
    end
end

def input_nodes_changed
    @fields = input_node.fields
end

end
