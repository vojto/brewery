module Brewery
class Cube
attr_reader :table

def dataset=(dataset)
	@dataset = dataset
end

def aggregate(measure, aggregations)
	agg_to_sql = {:sum => "SUM", :count => "COUNT", :average => "AVG", :min => "MIN", :max => "MAX"}

	expressions = Array.new
	selections = Hash.new
	
	# FIXME: This is Sequel-dependent (which is wrong)

	i = 0
	aggregations.each { |agg|
		sql_aggregation = agg_to_sql[agg]
		# FIXME: add this to unit testing
		if !sql_aggregation
			raise RuntimeError, "Unknown aggregation '#{agg}'"
		end
		
		# expression = "#{sql_aggregation}(#{measure}) AS #{agg}_#{i}"
		field = "agg_#{i}".to_sym
		expression = "#{sql_aggregation}(#{measure})"

		selections[field] = expression
		
		i = i+1
	}
	
	selections[:record_count] = "COUNT(1)"
	selection = @dataset.select(selections).first

	i = 0
	result = Hash.new
	
	aggregations.each { |agg|
		field = "agg_#{i}".to_sym
		value = selection[field]

		# FIXME: use appropriate type (Sequel SQLite returns String)
		if value.class == String
			value = value.to_f
		end
		result[agg] = value
	
		i = i+1
	}
	
	# FIXME: use appropriate type (Sequel returns String)
	value = selection[:record_count]
	if value.class == String
		value = value.to_f
	end
	result[:record_count] = value

	return result
end

end # class
end #module