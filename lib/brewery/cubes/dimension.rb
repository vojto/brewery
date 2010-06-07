require 'rubygems'
require 'sequel'
require 'data_objects'

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
include DataObjects::Quoting

# Dimension label
# @todo Make localizable
attr_accessor :label

# Dimension hierarchy - array of field names that define hierarchy.
attr_accessor :hierarchy

# Hash of hierarchy levels in the form: level => [fields]
# attr_accessor :levels

# More detailed description of the dimension
attr_accessor :description

# Dataset (table) that contains dimension values
attr_reader :dataset
# Key field
attr_accessor :key_field

def self.dataset_from_file(path)
	hash = YAML.load_file(path)
	if !hash
		return nil
	end
	
	dim = Dimension.new
	dim.label = hash[:label]
	dim.description = hash[:description]
	levels = hash[:levels]
	if levels
		levels.keys.each { |key|
			level = levels[:key]
#			...
		}
	end
end

def initialize
	@key_field = :id
	@levels = Hash.new
end

def dataset=(dataset)
	@dataset = dataset
end

# Returns values of hierarchy level which follows the last level in given path.
#
# == Example:
# @example Get all months:
#   date_dimension.drill_down_values([2009])
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
def drill_down_values(path)
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

	level_fields = fields_for_level(level_name)
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

	return values
			
end

# Return dimension key for given path. If path is not complete, returns min key
# for most matching path.
def key_for_path(path)
	level = 0
	conditions = Array.new
	
	path.each { |level_value|
		level_column = hierarchy[level]
		conditions << {:column =>level_column, :value => level_value}	
		# puts "LEVEL #{level}: #{level_column} = #{level_value}"
		level = level + 1
	}

	level_name = hierarchy[level-1]
	
	# FIXME: this is valid only while there is only Sequel implementatin of datasets
	data = @dataset.table
	
	conditions.each { |cond|
		if cond[:value] != :all
			data = data.filter(cond[:column] => cond[:value])
		end
	}
	
	values = Array.new
	
	# FIXME: limit selected columns (do not select all, only those required by level)
	fields = [@key_field]
	fields.concat(fields_for_level(level_name))

	data = data.clone(:order => [@key_field])
	data = data.clone(:select => fields)
	first = data.first
	if first
		return first[@key_field]
	else
		return nil
	end
end

# Return path which is one level below given path. If no path is provided, return path to
# first level. If path at last level is provided, return same path.
def drill_down_level(path)
	# FIXME: check validity of path
	# validate_path(path)
	
	if !path
		return @hierarchy[0]
	end
	
	next_level = path.count
	if next_level >= @hierarchy.count
		return @hierarchy.last
	end
	
	return @hierarchy[next_level]
end

def path_levels(path)
	if !path || path.count == 0
		return []
	end
	
	return @hierarchy[0..(path.count-1)]
end

def add_level(level_name, level)
	@levels[level_name] = level
end

# Return all fields that represent @level
def fields_for_level(level)
	return @levels[level].fields
end

# Return name of key field for level @level
def key_field_for_level(level)
	return @levels[level].key_field
end

# Return description of a level. See {DimensionLevel}
def level_description(level)
	return @levels[level]
end

# Returns path in hierarchy which is one level higher than given path
#
# == Example:
# @example Roll up from month to year:
#   date_dimension.roll_up_path([2009, 1])
#   # Result: [2009]
#
# == Parameters:
# path::
#   Array of values for corresponding dimension hierarchy level. 
#
# == Returns:
# Path represented by array of values
#
def roll_up_path(path)
	if !path or path == []
		return []
	end
	
	up_path = path.dup
	up_path.delete_at(-1)
	return up_path
end

# FIXME: temporary SQL functions
def sql_join_expression(dimension_field, fact_field, dimension_alias, table_alias)
	# FIXME: may cause issues with schemas
	table_name = dataset.table_name

	join_expression = "JOIN #{table_name} #{dimension_alias} " +
						"ON (#{dimension_alias}.#{dimension_field} = #{table_alias}.#{fact_field})"

	return join_expression
end


def to_hash
	hash = Hash.new
	
	hash[:type] = :dimension
	hash[:name] = @name

	# FIXME: check
	hash[:levels] = @levels
	hash[:hierarchy] = @hierarchy
	
	return hash
end

def to_yaml
	return self.to_hash.to_yaml
end

end # class

end # Module
