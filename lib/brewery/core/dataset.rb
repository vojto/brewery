module Brewery

# Object representing a dataset. Currently used only for dataset metadata, such as fields and field types.
class Dataset
def self.dataset_from_database_table(table)
	return SequelDataset.new(table)
end

end # class

class SequelDataset < Dataset
def initialize(table)
	@table = table
	@connection = table.db
	@table_name = table.first_source
	# puts "T: #{@table} C:#{@connection} TN:#{@table_name}"
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
	if selection.class != Hash
		raise RuntimeError, "SequelDataset select accepts only hash (form: field=>expression)"
	end

	rehash = Hash.new
	
	selection.keys.each { |key|
		rehash[selection[key].to_s.lit] = key
	}
	
	return @table.select(rehash)
end

end # class

end # module
