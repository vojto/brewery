module Brewery

class TableDataQualityAuditor
attr_accessor :connection
attr_accessor :table_name
attr_accessor :audit_tests
# 

@@string_types = [ :string, :text ]
@@numeric_types = [ :integer, :decimal, :numeric ]

def initialize(connection = nil)
	@data_quality_table_name = :brdq_data_quality
	@distinc_values_table_name = :brdq_distinct_values
	@connection = connection
	
	@audit_tests = [
				 	:data_type,
				 	:total_count,
					:null_count,
					:null_percent,
					:not_null_count,
					:not_null_percent,
					:empty_count,
					:empty_percent,
					:distinct_values,
					:value_min,
					:value_max,
					:value_avg,
					:value_sum
				   ]
end

def create_quality_audit_tables
	if @connection.table_exists?(@data_quality_table_name)
		@connection.drop_table(@data_quality_table_name)
	end
	
	@connection.create_table(@data_quality_table_name) do
		primary_key :id
		String  :table_name
		String  :field
		Integer :total_count
		Integer :not_null_count
		Integer :null_count
		Integer :empty_count
		Integer :distinct_values
		Integer :duplicate_values
	end
	
	if @connection.table_exists?(@distinc_values_table_name)
		@connection.drop_table(@distinc_values_table_name)
	end
	
	@connection.create_table(@distinc_values_table_name) do
		primary_key :id
		String  :table_name
		String  :field
		String  :value
		Integer :value_count
	end
end

# Return all fields in audited table
def all_table_fields
	return @connection.schema(@table_name).collect { |f| f[0] }
end

def field_type(field_name)
	field_name = field_name.to_sym
	f = @connection.schema(@table_name).detect { |f| f[0] == field_name }
	if f
		return f[1][:type]
	end
	return nil
end

def audit_all_fields
	return audit_fields(all_table_fields)
end

def audit_fields(fields)
	result = Array.new
	fields.each { |field| 
		result << audit_field(field)
	}
	return result
end

def audit_field(field)
	# ...
	# return hash with keys: total_count, null_count, not_null_count, 
	# distinct_count, duplicates_count
	if !@connection.table_exists?(@table_name)
		raise RuntimeError, "Table #{@table_name} does not exist"
	end

	table = @connection[@table_name]

	data_type = field_type(field)
	if @@string_types.include?(data_type)
		type = :string
	elsif @@numeric_types.include?(data_type)
		type = :numeric
	end

	record = Hash.new
	record[:table_name] = @table_name.to_s
	record[:field] = field.to_s
	record[:data_type] = data_type
	
	selection = {
		"COUNT(1)".lit => :total_count,
		"SUM(CASE WHEN #{field} IS NULL THEN 1 ELSE 0 END)".lit => :null_count,
		"SUM(CASE WHEN #{field} IS NOT NULL THEN 1 ELSE 0 END)".lit => :not_null_count,
		"COUNT(DISTINCT #{field})".lit => :distinct_values
	}
	
	string_selection = {
		"SUM(CASE WHEN #{field} = '' THEN 1 ELSE 0 END)".lit => :empty_count,
		"MIN(LENGTH(#{field}))".lit => :value_min,
		"MAX(LENGTH(#{field}))".lit => :value_max
	}
	
	numeric_selection = {
		"MIN(#{field})".lit => :value_min,
		"MAX(#{field})".lit => :value_max,
		"AVG(#{field})".lit => :value_avg,
		"SUM(#{field})".lit => :value_sum
	}
	
	if type == :string
		selection = selection.merge(string_selection)
	elsif type == :numeric
		selection = selection.merge(numeric_selection)
	end

	result = table.select(selection).first

	record = record.merge(result)

	count = result[:total_count].to_f

	if count > 0
		record[:null_percent] = record[:null_count] / count * 100.0
		record[:not_null_percent] = record[:not_null_count] / count * 100.0
		empty_count = record[:empty_count]
		if empty_count
			record[:empty_percent] = record[:empty_count] / count * 100.0
		end
	end
	
	# convert from big decimal to something more useable
	# FIXME: is this kosher?
	if type == :numeric
		for f in [ :value_min, :value_max, :value_avg, :value_sum ]
			record[f] = record[f].to_f
		end
	end

	return record
end

def duplicates_for_field(value_field, key_field)
	# return: array of hashes: field_value, keys => [...]
	#

end

end # class
end # module