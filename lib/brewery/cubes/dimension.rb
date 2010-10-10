require 'rubygems'
require 'sequel'
require 'data_objects'
require 'brewery/cubes/dimension_level'
require 'brewery/core/dataset_description'

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

    # Human-readable dimension label
    # @todo Make localizable
	property :label, String

    # Dimension key field used for ranged cuts (historical remainder)
    # @todo Remove, see http://github.com/Stiivi/brewery/issues#issue/10
	property :key_field, String

    # More detailed description of the dimension
	property :description, Text
	property :default_hierarchy_name, String

    has n, :levels, { :model => DimensionLevel }
	has n, :hierarchies #, {:through=>DataMapper::Resource} # default hierarchy
    has n, :cubes, {:through=>DataMapper::Resource}
    belongs_to :logical_model


def initialize_from_file(path)
	hash = YAML.load_file(path)
	return initialize_from_hash(hash)
end

def initialize_from_hash(from_hash)
	
	hash = from_hash.hash_by_symbolising_keys

	self.name = hash[:name]
	self.label = hash[:label]
	self.description = hash[:description]

    self.key_field = hash[:key_field]

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
			level.description_field = level_info[:description_field]
			level.name = level_name
			self.levels << level
		}
	end
	
	if !self.save
	    raise "Unable to save dimension"
	end
	
	hiers = hash[:hierarchies]

	if hiers
    	hiers = hiers.hash_by_symbolising_keys
        hiers.each { |hier_name, hier_info|
            hier_info = hier_info.hash_by_symbolising_keys
            hier = self.create_hierarchy(hier_name)
            hier.levels = hier_info[:levels]
            hier.save
        }
    end
	
	return self
end

def create_hierarchy(name)
    hier = hierarchies.new( { :name => name } )
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
    
# == Returns:
# Level containing attribute
def level_for_attribute(attribute)
    levels.each { |level|
        if level.level_fields.include?(attribute)
            return level
        end
    }
    return nil
end
        

# Return path which is one level below given path. If no path is provided, return path to
# first level. If path at last level is provided, return same path.
def next_level(path)
	# FIXME: check validity of path
	# validate_path(path)

	hier = default_hierarchy
	if !path || path.empty?
		return hier.levels[0]
	end
	
	next_level = path.count
	if next_level >= hier.levels.count
		return nil
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

def all_fields
    fields = []
    # FIXME: Do not use default hierarchy, pass hierarchy as argument
    default_hierarchy.levels.each { |level|
        fields.concat(level.level_fields)
    }
    
    return fields
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

# Return a hierarchy with given name. See {Hierarchy}
def hierarchy_with_name(hier_name)
	return hierarchies.first( :name => hier_name )
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

def to_hash
	hash = Hash.new
	
	hash[:type] = :dimension
	hash[:name] = @name

	# FIXME: check
	hash[:levels] = @levels
	hash[:hierarchy] = @hierarchy
	
	return hash
end

def self.date_key(date)
    return date.strftime('%Y%m%d').to_i
end

def to_yaml
	return self.to_hash.to_yaml
end

end # class

end # Module
