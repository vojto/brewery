module Brewery

class TableDataQualityAuditor
attr_accessor :connection
attr_accessor :table_name
# 

def initialize
	@data_quality_table_name = :brdq_data_quality
	@distinc_values_table_name = :brdq_distinct_values
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

def analyze_all_fields
	fields = 
end

def analyze_field(field)
	# ...
	# return hash with keys: total_count, null_count, not_null_count, 
	# distinct_count, duplicates_count
	table = @connection[@table_name]
	
	record = Hash.new
	record[:field] = field.to_s
	record[:table_name] = @table_name.to_s
	
	selection = {
		"COUNT(1)".lit => :total_count,
		"SUM(CASE WHEN #{field} IS NULL THEN 1 ELSE 0 END)".lit => :null_count,
		"SUM(CASE WHEN #{field} IS NOT NULL THEN 1 ELSE 0 END)".lit => :not_null_count,
#		"SUM(CASE WHEN #{field} = '' THEN 1 ELSE 0 END)".lit => :empty_count,
		"COUNT(DISTINCT #{field})".lit => :distinct_values
	}
	
	
	result = table.select(selection).first

	record = record.merge(result)
	return record
end

def duplicates_for_field(value_field, key_field)
	# return: array of hashes: field_value, keys => [...]
	#

end

end # module