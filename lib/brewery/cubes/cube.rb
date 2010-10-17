require 'brewery/cubes/dataset_join'
require 'brewery/core/dataset_description'

module Brewery
class Cube
	include DataObjects::Quoting
	include DataMapper::Resource

	property :id, Serial
	property :name, String, :default => 'unnamed cube'
	property :label, String
	property :description, Text
	property :fact_dataset_name, String

    belongs_to :logical_model
    has n, :dimensions, {:through=>DataMapper::Resource} #, :constraint => :destroy}
    has      n, :joins, { :model => DatasetJoin } #, :constraint => :destroy }

    attr_accessor :view

def validate
    results = []

    if !fact_dataset_name
        results << [:error, "No fact dataset name specified for cube '#{name}'"]
    end
    
    if !fact_dataset
        results << [:error, "Unable to find fact dataset '#{fact_dataset_name}' for cube '#{name}'"]
    end

    dimensions.each { | dim |
        dim.levels.each { |level|
            level.level_fields.each { |field|
                ref = field_reference(field)
                ds = logical_model.dataset_description_with_name(ref[0])
                if !ds
                    results << [:error, "Unknown dataset '#{ref[0]}' for field '#{field}', dimension '#{dim.name}', level '#{level.name}', cube '#{name}'"]
                else
                    fd = ds.field_with_name(ref[1])
                    if !fd
                        results << [:error, "Unknown dataset field '#{ref[0]}.#{ref[1]}' specified in dimension '#{dim.name}', level '#{level.name}', cube '#{name}'"]
                    end
                end
            }
        }
    }

    return results
end
    
def field_reference(field)
    split = field.split('.')
    if split.count == 1
        ref = [fact_dataset_name, field]
    else
        # FIXME: get dataset name from dimension in split[0]
        ref = [split[0], split[1]]
    end
    return ref
end

def aggregate(measure)
	return @whole.aggregate(measure)
end

def whole
	if !@whole
		@whole = Slice.new(self)
	end
	return @whole
end

# Return dataset description representing fact
def fact_dataset
    # FIXME: rename this to fact_dataset_description
    if !@fact_dataset
        @fact_dataset = logical_model.dataset_description_with_name(fact_dataset_name)
    end
    return @fact_dataset
end

# Return dimension object. If dim is String or Hash then find named dimension.
def dimension_object(dimension)
    case dimension
    when String, Symbol
		obj = dimension_with_name(dimension)
    when Dimension
		obj = dimension
    else
        assert_kind_of "dimension", dimension, Dimension, String, Symbol
    end

	if !obj
		raise RuntimeError, "Cube '#{self.name}' has no dimension '#{dimension}'"
	end

	return obj
end

def dimension_with_name(name)
    name = name.to_s
    
	if !@cached_dimensions
		@cached_dimensions = Hash.new
	end
	dim = @cached_dimensions[name]
	if !dim
		dim = dimensions.first( :name => name )
		@cached_dimensions[name] = dim
	end

	return dim
end

def fact(fact_id)    
	query = create_query

	return query.record(fact_id)
end

def create_query
    if !@view
        raise RuntimeError, "No cube view specified"
    end

    query = CubeQuery.new(self, view)
    return query
end

def create_star_query
	query = StarQuery.new(self)

    return query
end

def field_with_name(field_name)
    return dataset_description.field_with_name(field)
#    return fact_fields.first(:name => field_name)
end

def field_description(field)
    return dataset_description.field_with_name(field)
end

def label_for_field(field_name)
    parts = field_name.split('.')
    if parts.count == 1
        field_name = parts[0]
        field = field_with_name(field_name)
        if field
            return field.label
        else
            return nil
        end
    elsif parts.count == 2
        field_name = parts[1]
        dim_name = parts[0]
        dimension = dimension_with_name(dim_name)
        if !dimension
            raise ArgumentError, "No dimension with name '#{dim_name}' in cube '#{name}'"
        end
        return "#{field_name} IN #{dim_name}"
    else
        raise ArgumentError, "Ivnalid field name '#{field_name}' (more than two relationship levels)"
    end
end
def all_fields
    all_fields = []
    fact_dataset.field_descriptions.each { |field|
        all_fields << field.name.to_sym
    }
    dimensions.each { |dimension|
        all_fields.concat(dimension.all_fields)
    }
    return all_fields
end

end # class
end # module