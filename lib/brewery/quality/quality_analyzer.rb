module Brewery

class TableQualityAnalyzer
attr_accessor :connection
attr_accessor :table
# 
def analyze_field(field)
	# ...
	# return hash with keys: total_count, null_count, not_null_count, 
	# distinct_count, duplicates_count
end

def duplicates_for_field(value_field, key_field)
	# return: array of hashes: field_value, keys => [...]
	#

end

end # module