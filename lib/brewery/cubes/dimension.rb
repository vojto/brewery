require 'rubygems'
require 'sequel'

module Brewery

# Represents dataset or fact dimension.
# 
# == Date dimension example
# Assume that we have a table representing date dimension with fields:
# year, month, month_name, month_short_name, day, weekday, weekday_name
# 
# Define dimension and assign dataset containing dimension values:
#  dim = Dimension.new
#  dim.dataset = dataset
# Define hierarchy:
#  dim.hierarchy = [:year, :month, :day]
#
# Define levels:
#  dim.levels = { :year => [:year],
#                 :month => [:month, :month_name, :month_sname],
#                 :day => [:day, :weekday, :weekday_name] }
class Dimension

# Dataset (table) that contains dimension values
attr_reader :dataset

# Dimension hierarchy - array of field names that define hierarchy.
attr_accessor :hierarchy

# Hash of hierarchy levels in the form: level => [fields]
attr_accessor :levels

def dataset=(dataset)
	@dataset = dataset
end

# 
def values_at_path(path)
	# 2009 -> all months
	# 2009, 2 -> all days
	# 2009, :all -> all days for all months
	level = 0
	conditions = Array.new
	
	path.each { |level_value|
		level_column = hierarchy[level]
		conditions << {:column =>level_column, :value => level_value}	
		# puts "LEVEL #{level}: #{level_column} = #{level_value}"
		level = level + 1
	}
	extracted_column = hierarchy[level]
	
	# FIXME: this is valid only while there is only Sequel implementatin of datasets
	data = @dataset.table
	
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

# Returns values of hierarchy level which follows the last level in given path.
#
# == Example:
# @example Get all months:
#   date_dimension.drill_down_path([2009])
#   # Result:
#   { :month => 1, :month_name => "January", :month_short_name => "Jan" }
#   { :month => 2, :month_name => "February", :month_short_name => "Feb" }
#   ...
#
# == Parameters:
# path::
#   Array of values for corresponding dimension hierarchy level. If `:all` is used, 
#   then every value for that level is considered.
#
# == Returns:
# Array of records as hashes.
#
def drill_down_path(path)
	# 2009 -> all months
	# 2009, 2 -> all days
	# 2009, :all -> all days for all months
	level = 0
	conditions = Array.new
	
	path.each { |level_value|
		level_column = hierarchy[level]
		conditions << {:column =>level_column, :value => level_value}	
		# puts "LEVEL #{level}: #{level_column} = #{level_value}"
		level = level + 1
	}

	level_name = hierarchy[level]
	
	# FIXME: this is valid only while there is only Sequel implementatin of datasets
	data = @dataset.table
	
	conditions.each { |cond|
		if cond[:value] != :all
			data = data.filter(cond[:column] => cond[:value])
		end
	}
	
	values = Array.new
	
	# FIXME: limit selected columns (do not select all, only those required by level)

	if @levels
	
		level_fields = @levels[level_name]
		#str = level_fields.collect{|f| f.to_s.lit}.join(',')
		# data = data.group(level_fields).order(level_fields)
		data = data.clone(:group => level_fields)
		data = data.clone(:order => level_fields)
		data = data.clone(:select => level_fields)
		# puts "SQL: #{data.sql}"
		data.each { |row|
			record = Hash.new
			level_fields.each { |field|
				record[field] = row[field]
			}

			values << record
		}

	else
		data = data.group(level_name).order(level_name)
		data.each { |row|
			values << { level_name => row[level_name] }
		}
	end

	return values
			
end
end

end # Module
