require 'rubygems'
require 'sequel'

module Brewery

class Dimension

attr_reader :dataset

attr_accessor :hierarchy

def dataset=(dataset)
	@dataset = dataset
end

def values_at_path(path)
	# 2009 -> all months
	# 2009, 2 -> all days
	# 2009, :all -> all days for all months
	level = 0
	conditions = Array.new
	
	path.each { |level_value|
		level_column = hierarchy[level]
		conditions << {:column =>level_column, :value => level_value}	
	
		puts "LEVEL #{level}: #{level_column} = #{level_value}"

		level = level + 1
	}
	extracted_column = hierarchy[level]
	
	data = @connection[@table]
	
	conditions.each { |cond|
		if cond[:value] != :all
			data = data.filter(cond[:column] => cond[:value])
		end
	}
	
	data = data.group(extracted_column).order(extracted_column)

	values = Array.new

	# puts "SQL: #{data.sql}"
	data.select(extracted_column).each { |row|
		values << row[extracted_column]
	}
	
	return values
			
end

end

end # Module
