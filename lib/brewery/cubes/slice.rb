require 'data_objects'

module Brewery

class Slice
include DataObjects::Quoting

# FIXME: validate
attr_accessor :cut_dimension
attr_accessor :cut_values

def initialize(cube, parent)
	@cube = cube
	@parent = parent
end
def dataset
	@cube.dataset
end

def aggregate2(measure, aggregations)
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
	data = dataset.select(selections)
	
	# Prepare join
	# FIXME: this is highly Sequel dependent
	if @cut_dimension
		puts "CUT: #{@cut_dimension}"
		join_info = @cube.joins[@cut_dimension]
		dimension = join_info[:dimension]
		dim_table = dimension.dataset.table_name
	
		dimension_field = join_info[:dimension_field]
		table_field = join_info[:table_field]
		data = data.join_table(:inner, dim_table, {dimension_field => table_field})
	end
	
	puts "SQL: #{data.sql}"
	
	selection = data.first
	
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

def aggregate(measure, aggregations)
	agg_to_sql = {:sum => "SUM", :count => "COUNT", :average => "AVG", :min => "MIN", :max => "MAX"}


	# FIXME: This is Sequel-dependent (which is wrong)
	i = 0
	expressions = Array.new
	aggregations.each { |agg|
		sql_aggregation = agg_to_sql[agg]
		# FIXME: add this to unit testing
		if !sql_aggregation
			raise RuntimeError, "Unknown aggregation '#{agg}'"
		end
		
		expression = "#{sql_aggregation}(#{measure}) AS agg_#{i}"
		expressions << expression
		
		i = i+1
	}
	
	expressions << "COUNT(1) AS record_count"

	select_expression = expressions.join(', ')
	
	fact_table = @cube.dataset.table_name
	statement = "SELECT #{select_expression} FROM #{fact_table} f"

	if @cut_dimension
		puts "CUT: #{@cut_dimension}"
		join_info = @cube.joins[@cut_dimension]
		dimension = join_info[:dimension]
		dim_table = dimension.dataset.table_name

		# FIXME: fix this name
		dim_alias = "d"

		sql_join = dimension.sql_join_expression(join_info[:dimension_field],
				                                 join_info[:table_field],
												 dim_alias)

		sql_condition = dimension.sql_condition(@cut_values, dim_alias)

		statement = statement +" #{sql_join} WHERE #{sql_condition}"
	end
	
	
	puts "SQL: #{statement}"
	
	# FIXME: This is very ugly hack, Sequel dependent
	selection = @cube.dataset.connection[statement].first

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

end # class Slice
end # module Brewery