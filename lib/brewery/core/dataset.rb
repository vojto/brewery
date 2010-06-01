module Brewery

# Object representing a dataset. Currently used only for dataset metadata, 
# such as fields and field types.
#
# @abstract
class Dataset
def self.dataset_from_database_table(table)
	return SequelDataset.new(table)
end

end # class

class SequelDataset < Dataset
attr_reader :table_name
attr_reader :connection

def initialize(table)
	@table = table
	@connection = table.db
	@table_name = table.first_source
end

def fields
	return @connection.schema(@table_name).collect { |f| f[0] }
end

def type_of_field(field_name)
	field_name = field_name.to_sym
	f = @connection.schema(@table_name).detect { |f| f[0] == field_name }
	if f
		return f[1][:type]
	end
	return nil
end

def count
	return @table.count
end

# FIXME: do not expose this, provide query functions instead. this is temporary because of Sequel
def table
	return @table
end

def select(selection)
	if selection.class == Hash
		rehash = Hash.new
		
		# They have to be kidding me ... expressions as keys and column names as values??
		selection.keys.each { |key|
			rehash[selection[key].to_s.lit] = key
		}
		
		return @table.select(rehash)
	else
		return @table.select(selection)
	end
end

end # class

end # module
