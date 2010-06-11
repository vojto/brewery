require 'rubygems'
require 'sequel'
require 'data_objects'
require 'brewery/cubes/dimension_level'

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
	include DataMapper::Resource

	property :id, Serial
	property :name, String
	property :label, String
	property :description, Text
	property :key_field, String, {:default => "id"}
	property :table, String
	property :default_hierarchy_name, String

    has n, :levels, { :model => DimensionLevel }
	has n, :hierarchies #, {:through=>DataMapper::Resource} # default hierarchy
    has n, :models,  {:through=>DataMapper::Resource}

    has n, :cubes, :through => :cube_dimension_joins
    has n, :cube_dimension_joins

# Dimension label
# @todo Make localizable
# attr_accessor :label

# Dimension hierarchy - array of field names that define hierarchy.
# attr_accessor :hierarchy

# Hash of hierarchy levels in the form: level => [fields]
# attr_accessor :levels

# More detailed description of the dimension
# attr_accessor :description

# Dataset (table) that contains dimension values
attr_reader :dataset
# Key field
attr_accessor :key_field

def self.new_from_file(path)
	hash = YAML.load_file(path)
	if !hash
		return nil
	end
	return self.new_from_hash(hash)
end

def self.new_from_hash(from_hash)
	
	hash = from_hash.hash_by_symbolising_keys

	dim = Dimension.new
	dim.name = hash[:name]
	dim.label = hash[:label]
	dim.description = hash[:description]
	dim.table = hash[:table]

	new_levels = hash[:levels]
	if new_levels.class != Hash
	    raise RuntimeError, "Hierarchy levels in hash/file should be a hash"
	end
	
	if new_levels
    	new_levels = new_levels.hash_by_symbolising_keys
		new_levels.each { |level_name, level_info|
		    # puts "READING LEVEL #{level_name}"
			level_info = level_info.hash_by_symbolising_keys
			level = DimensionLevel.new
			level.label = level_info[:label]
            # puts "FIELDS: #{level_info[:fields]}"
			level.level_fields = level_info[:fields]
			level.name = level_name
			dim.levels << level
		}
	end
	
	dim.save
	
	hiers = hash[:hierarchies]

	if hiers
    	hiers = hiers.hash_by_symbolising_keys
        hiers.each { |hier_name, hier_info|
            hier_info = hier_info.hash_by_symbolising_keys
            hier = dim.create_hierarchy(hier_name)
            hier.levels = hier_info[:levels]
            hier.save
        }
    end
	
	return dim
end

def create_hierarchy(name)
    hier = Hierarchy.new( { :name => name } )
    hierarchies << hier
    hier.save
    return hier
end

def default_hierarchy
    # FIXME: flush cache on attribute update
    if @default_hierarchy
       return @default_hierarchy
    end
    
    hier = nil
    name = default_hierarchy_name
    if !name
        name = :default
    end
    
    hier = hierarchies( :name => name ).first

    if !hier
        hier = hierarchies.first
    end

    @default_hierarchy = hier
    
    return hier
end
    

# def initialize
#	@key_field = :id
# 	@levels = Hash.new
#end

def dataset=(dataset)
	@dataset = dataset
	self.table = dataset.table_name
end

# Returns values of hierarchy level which follows the last level in given path.
#
# == Example:
# @example Get all months:
#   date_dimension.list_of_values([2009])
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
def list_of_values(path)
	# 2009 -> all months
	# 2009, 2 -> all days
	# 2009, :all -> all days for all months
	level_index = 0
	conditions = Array.new
	
	path.each { |level_value|
    	level = default_hierarchy.levels[level_index]
	
		conditions << {:column =>level.key_field.to_sym, :value => level_value}	
		# puts "LEVEL #{level}: #{level_column} = #{level_value}"
		level_index += 1
	}

	level = default_hierarchy.levels[level_index]

	# FIXME: this is valid only while there is only Sequel implementatin of datasets
	data = @dataset.table
	
	conditions.each { |cond|
		if cond[:value] != :all
			data = data.filter(cond[:column] => cond[:value])
		end
	}
	
	values = Array.new
	
	# FIXME: limit selected columns (do not select all, only those required by level)

    level_fields = level.level_fields.collect {|f| f.to_sym }
	level_key = level.key_field.to_sym
	#str = level_fields.collect{|f| f.to_s.lit}.join(',')
	# data = data.group(level_fields).order(level_fields)
	data = data.clone(:group => [level_key])
	data = data.clone(:order => [level_key])
	data = data.clone(:select => level_fields)
    # puts "==> PATH: #{path}"
	# puts "--- SQL: #{data.sql}"
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
	level_index = 0
	conditions = Array.new
    # puts "==> GET KEY FOR PATH: #{path}"
    hierarchy = default_hierarchy
    levels = hierarchy.levels
    
	path.each { |level_value|
		level_column = levels[level_index].key_field.to_sym
		conditions << {:column =>level_column, :value => level_value}	
		# puts "LEVEL #{level_index}: #{level_column} = #{level_value}"
		level_index = level_index + 1
	}

	level_name = levels[level_index-1].name
	
	# FIXME: this is valid only while there is only Sequel implementatin of datasets
	data = @dataset.table
	
	conditions.each { |cond|
		if cond[:value] != :all
			data = data.filter(cond[:column] => cond[:value])
		end
	}
	
	values = Array.new
	
	# FIXME: limit selected columns (do not select all, only those required by level)
    if key_field
    	key = key_field.to_sym
	else
	    key = :id
	end
	fields = [key].concat(fields_for_level(level_name))

	data = data.clone(:order => [key])
	data = data.clone(:select => fields)
	# puts "==> SQL: #{data.sql}"
	# puts "--- fields: #{fields}"
	first = data.first
	if first
		return first[key]
	else
		return nil
	end
end

# Return path which is one level below given path. If no path is provided, return path to
# first level. If path at last level is provided, return same path.
def drill_down_level(path)
	# FIXME: check validity of path
	# validate_path(path)

	hier = default_hierarchy
	if !path || path.empty?
		return hier.levels[0]
	end
	
	next_level = path.count
	if next_level >= hier.levels.count
		return hier.levels.last
	end
	
	return hier.levels[next_level]
end

def path_levels(path)
	if !path || path.count == 0
		return []
	end
    hierarchy = default_hierarchy
	return hierarchy.levels[0..(path.count-1)]
end

# Return all fields that represent @level
def fields_for_level(level)
    case level#.class
    when String, Symbol
    	level = level_with_name(level)
    when DimensionLevel
        # nothing, we are OK
    else
        assert_kind_of "level", level, DimensionLevel, String, Symbol
    end
    
	if level
		return level.level_fields
    else
        raise "Level '#{level_name}' does not exist in dimension '#{name}'"
	end
	return nil
end

# Return name of key field for level @level
def key_field_for_level(level)
    case level#.class
    when String, Symbol
    	level = level_with_name(level)
    when DimensionLevel
        # nothing, we are OK
    else
        assert_kind_of "level", level, DimensionLevel, String, Symbol
    end

	return level.key_field
end

# Return a level with given name. See {DimensionLevel}
def level_with_name(level_name)
	return levels.first( :name => level_name )
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
	table_name = table

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
