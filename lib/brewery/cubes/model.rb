module Brewery
require 'brewery/cubes/fact_field'
require 'brewery/core/dataset_description'
require 'brewery/core/field_description'

class Model
    include DataMapper::Resource

    property :id,   Serial
    property :name, String
    property :description, Text

    has      n, :dataset_descriptions, {:through=>DataMapper::Resource}
    has      n, :dimensions,  {:through=>DataMapper::Resource}
    has      n, :cubes, {:through=>DataMapper::Resource}

    @@model_search_paths = [
                        './models',
                        '~/.brewery/models'
                    ]

def self.model_from_path(path)
    model = Model.new
    model.load_from_path(path)
	return model
end

# Load model from a directory specified by _path_. The directory should contain:
# * model.yml - model information
# * dataset_*.yml - dataset description
# * dim_*.yml - dimension specifications
# * cube_*.yml - cube specifications
# @param [String, Pathname] Directory with model files
def load_from_path(path)
    path = Pathname.new(path)
    model_file = path + 'model.yml'
    
	hash = YAML.load_file(model_file)
	hash = hash.hash_by_symbolising_keys
	
	files = hash[:files]
	
	self.name = hash[:name]
	self.description = hash[:description]
	
	ds_files = Array.new
	dim_files = Array.new
	cube_files = Array.new
	
	path.children.each { | file |
	    prefix = file.basename.to_s.split('_')[0]
	    case prefix
        when "dataset"
            ds_files << file
	    when "dim", "dimension"
	        dim_files << file
        when "cube"
            cube_files << file
	    end
	}

    ds_files.each {|file|
        ds = DatasetDescription.new_from_file(file)
        dataset_descriptions << ds
    }

    dim_files.each {|file|
        dim = Dimension.new_from_file(file)
        dimensions << dim
    }
    self.save

    cube_files.each {|file|
        load_cube_from_file(file)
    }
    
    self.save

    # Assign datasets
    cubes.each { |cube|
        ds_name = cube.dataset_name
        dataset = dataset_descriptions.first(:name => ds_name)
        cube.dataset_description = dataset
#        cube.save
    }   
end

def load_cube_from_file(file)
	hash = YAML.load_file(file)
	hash = hash.hash_by_symbolising_keys
    puts "==> cube from #{file}"
    # puts "--> dims: #{dimensions.class}"
    cube = Cube.new

	cube.name = hash[:name]
	cube.label = hash[:label]
	cube.description = hash[:description]
	cube.fact_table = hash[:fact_table]
	cube.dataset_name = hash[:dataset_name]

    cubes << cube
    
    dim_joins = hash[:dimensions]

    dim_joins.each { |dim_name, join_info|
        join_info = join_info.hash_by_symbolising_keys
        dim = dimensions.first( :name => dim_name)
        # puts "--> join: #{dim_name}(#{dim_name.class}) - #{dim.class}"
        if !dim
            raise RuntimeError, "Unknown dimension '#{dim_name}' in model '#{self.name}' file '#{file}'"
        end
        cube.join_dimension(dim, join_info[:fact_key])
    }
    
    # FIXME: depreciate - replace with dataset description/field description
    fact_fields = hash[:fields]

    if fact_fields    
        fact_fields.each { |field_info|
            field = cube.fact_fields.new(field_info)
            # puts "Added field: #{field.name}"
        }
    end
end

# Returns model with given name
def self.model_with_name(name)
    model = self.first(:name => name)
    if !model
        raise ArgumentError, "Unable to find model with name '#{name}'"
    end
    return model
end

# Returns cube with given name
def cube_with_name(name)
    return cubes.first( :name => name )
end

# Returns dataset description with given name
def dataset_description_with_name(name)
    return dataset_descriptions.first( :name => name )
end

end # class
end # module