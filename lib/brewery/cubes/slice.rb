require 'data_objects'

module Brewery

class Slice
include DataObjects::Quoting

# FIXME: validate
attr_accessor :cuts

# Initialize slice instance as part of a cube
def initialize(cube)
	@cube = cube
	@cuts = Hash.new
end

# Copying contructor, called for Slice#dup
def initialize_copy(*)
  @cuts = @cuts.dup
end

def add_cut(dimension, values)
	@cuts[dimension] = values
end

def remove_cut(dimension)
	@cuts.delete(dimension)
end

def slice(dimension, values)
	slice = self.dup
	slice.add_cut(dimension, values)
	return slice
end

def dataset
	@cube.dataset
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

	joins = Array.new
	conditions = Array.new

	i = 0
	@cuts.keys.each { |cut_dimension|
		puts "CUT: #{cut_dimension}"
		dim_alias = "d#{i}"
		i += 1

		cut_values = @cuts[cut_dimension]

		join_info = @cube.joins[cut_dimension]
		dimension = join_info[:dimension]
		dim_table = dimension.dataset.table_name

		sql_join = dimension.sql_join_expression(join_info[:dimension_field],
				                                 join_info[:table_field],
												 dim_alias)

		joins << sql_join
		
		sql_condition = dimension.sql_condition(cut_values, dim_alias)
		conditions << sql_condition
	}

	if @cuts.keys.count > 0
		join_expression = joins.join(' ')
		condition_expression = conditions.join(' AND ')
		statement = statement +" #{join_expression} WHERE #{condition_expression}"
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

def drill_down_aggregate(drill_dimension, level, measure, aggregations)
	dim = @cube.joins[drill_dimension][:dimension]
	cut_path = @cuts[drill_dimension]
	level_fields = dim.levels[level]

	agg_to_sql = {:sum => "SUM", :count => "COUNT", :average => "AVG", :min => "MIN", :max => "MAX"}

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

	# Insert dimension values here
	
	
	select_expression = expressions.join(', ')
	
	fact_table = @cube.dataset.table_name

	joins = Array.new
	conditions = Array.new

	drill_dim_alias = nil

	i = 0
	@cuts.keys.each { |cut_dimension|
		puts "CUT: #{cut_dimension}"
		dim_alias = "d#{i}"
		i += 1

		if cut_dimension == drill_dimension
			drill_dim_alias = dim_alias
		end

		cut_values = @cuts[cut_dimension]

		join_info = @cube.joins[cut_dimension]
		dimension = join_info[:dimension]
		dim_table = dimension.dataset.table_name

		sql_join = dimension.sql_join_expression(join_info[:dimension_field],
				                                 join_info[:table_field],
												 dim_alias)

		joins << sql_join
		
		sql_condition = dimension.sql_condition(cut_values, dim_alias)
		conditions << sql_condition
	}

	if !drill_dim_alias
		join_info = @cube.joins[drill_dimension]
		drill_dim_alias = "d#{i}"
		sql_join = dim.sql_join_expression(join_info[:dimension_field],
				                                 join_info[:table_field],
												 drill_dim_alias)
		joins << sql_join
	end

	if level_fields && ! level_fields.empty?
		
		level_select_fields = level_fields.collect { |level|
				"#{drill_dim_alias}.#{level}"
		}
	
		dim_select = level_fields.join(', ')
	else
		level_select_fields = []
	end

	if level_select_fields.count > 0
		level_select_str = level_select_fields.join(',')
		select_expression = "#{select_expression}, #{level_select_str}"
	end
	condition_expression = nil	
	statement = "SELECT #{select_expression} FROM #{fact_table} f"
	if joins.count > 0
		join_expression = joins.join(' ')
	end
	if conditions.count > 0
		condition_expression = "WHERE " + conditions.join(' AND ')
	end	
	statement = "#{statement} #{join_expression} #{condition_expression}"
	
	if level_select_fields.count > 0
		group_expression = "GROUP BY #{level_select_str}"
		order_expression = "ORDER BY #{level_select_str}"
		statement = statement +" #{group_expression} #{order_expression}"
	end
	
	puts "DRILL SQL: #{statement}"
	# FIXME: This is very ugly hack, Sequel dependent
	selection = @cube.dataset.connection[statement]
	results = Array.new
	selection.each { |record|
	
		i = 0
		result = Hash.new
		
		aggregations.each { |agg|
			field = "agg_#{i}".to_sym
			value = record[field]
	
			# FIXME: use appropriate type (Sequel SQLite returns String)
			if value.class == String
				value = value.to_f
			end
			result[agg] = value
		
			i = i+1
		}
		
		level_fields.each { |level|
			result[level] = record[level]

		}
		value = record[:record_count]
		if value.class == String
			value = value.to_f
		end
		result[:record_count] = value

		results << result
	}
	
	# FIXME: use appropriate type (Sequel returns String)

	return results
end

end # class Slice
end # module Brewery