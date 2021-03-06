module Brewery

# @private
class HierarchyLevel
    include DataMapper::Resource

    property       :id,    Serial
    property       :order, Integer

    belongs_to     :hierarchy
    belongs_to     :dimension_level
end

class Hierarchy

    include DataMapper::Resource
    
    property :id, Serial
    property :name, String

    belongs_to    :dimension #, {:through=>DataMapper::Resource}
    has        n, :hierarchy_levels, :order => [ :order.asc ] #, :constraint => :destroy

def levels=(array)
    self.save
    self.hierarchy_levels.destroy
    @levels = Array.new

    array.each_index { |i|
        obj = array[i]
        
        if obj.class == String || obj.class == Symbol
            level = dimension.levels(:name => obj.to_s).first
            if !level
                raise RuntimeError, "Level '#{obj.to_s}' does not exist in hierarchy '#{name}' of dimension '#{dimension.name}'"
            end
        else        
            level = obj
        end

        hl = HierarchyLevel.new
        hl.order = i
        self.hierarchy_levels << hl
        level.hierarchy_levels << hl
        @levels << level
    }
end

def level_names
    return levels.collect { |level| level.name }
end

def levels
    # FIXME: flush cache on attribute update
    if !@levels
        ordered = hierarchy_levels.all(:order => [ :order.asc ])
        @levels = ordered.collect { |level| level.dimension_level }
    end
    
    return @levels
end

def next_level(path)
	# FIXME: check validity of path
	# validate_path(path)

	if !path || path.empty?
		return levels[0]
	end
	
	next_level = path.count
	if next_level >= levels.count
		return nil
	end
	
	return levels[next_level]
end

end # class Hierarchy

end # module
