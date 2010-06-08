module Brewery

class HierarchyLevel
    include DataMapper::Resource

    property       :id, Serial
    property       :order, Integer

    belongs_to     :hierarchy
    belongs_to     :dimension_level
end

class Hierarchy

    include DataMapper::Resource
    
    property :id, Serial
    property :name, String

    belongs_to    :dimension #, {:through=>DataMapper::Resource}
    has        n, :hierarchy_levels, :order => [ :order.asc ]

def levels=(array)
    self.hierarchy_levels.destroy
    
    array.each_index { |i|
        obj = array[i]
        
        if obj.class == String || obj.class == Symbol
            level = dimension.levels(:name => obj.to_s).first
        else        
            level = obj
        end

        hl = HierarchyLevel.new
        hl.order = i
        self.hierarchy_levels << hl
        level.hierarchy_levels << hl
    }
end

def level_names
    return levels.collect { |level| level.name }
end

def levels
    ordered = hierarchy_levels.all(:order => [ :order.asc ])
    return ordered.collect { |level| level.dimension_level }
end

end # class Hierarchy

end # module