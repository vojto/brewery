module Brewery
require 'brewery/cubes/dataset_join'
require 'brewery/core/dataset_description'
require 'brewery/core/field_description'

# Logical Model represents mapping between human point of view on analysed facts and their physical
# database representation. Main objects of a model are datasets (physical objects) and multidimensioanl
# cubes (logical objects). For more information see {Cube}.
# @see Cube
# @see Dimension
# @see Hierarchy
class LogicalModel
    include DataMapper::Resource

    property :id,   Serial

    # Model identifier - unique within brewery data store
    # @todo Check for uniqueness
    property :name, String

    # Human readable model name
    property :label, String

    # Human readable model description
    property :description, Text

    has      n, :dataset_descriptions, {:through=>DataMapper::Resource} #, :constraint => :destroy}
    has      n, :dimensions # , {:constraint => :destroy}
    has      n, :cubes # , {:constraint => :destroy} #,  {:through=>DataMapper::Resource}

    @@model_search_paths = [
                        './models',
                        '~/.brewery/models'
                    ]

# Create a model from a directory specified by _path_. The directory should contain:
# * model.yml - model information
# * dataset_*.yml - dataset description
# * dim_*.yml - dimension specifications
# * cube_*.yml - cube specifications
# If model with given name already exists method fails, unless option :replace is set to true.
# @param [String, Pathname] Directory with model files
# @option options [Boolean] :replace if true create model if already exists
# @todo change model file naming
def self.create_model_from_path(path, options = {})
    replace_flag = options[:replace] ? true : false

    path = Pathname.new(path)
    model_file = path + 'model.yml'
    
	hash = YAML.load_file(model_file)
	hash = hash.hash_by_symbolising_keys
	
	model_name = hash[:name]
	
	model = LogicalModel.model_with_name(model_name)
	if model
        if !replace_flag
	        raise "Model #{model_name} already exists"
	    else
	        model.destroy
	    end
	end

    model = LogicalModel.new
	
	files = hash[:files]
	
	model.name = hash[:name]
	model.description = hash[:description]
		
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

    ################################################################
    # 1. Dataset descriptions

    ds_files.each {|file|
        ds = DatasetDescription.new_from_file(file)
        model.dataset_descriptions << ds
        # puts "--> Loaded dataset: #{ds.name}"
    }

    if !model.save
        raise "Unable to save model"
    end

    ################################################################
    # 2. Dimensions

    dim_files.each {|file|
        dim = model.dimensions.new
        dim.initialize_from_file(file)
    }

    if !model.save
        raise "Unable to save model"
    end

    ################################################################
    # 2. Cubes
    cube_files.each {|file|
        model.load_cube_from_file(file)
    }

    if !model.save
        raise "Unable to save model"
    end

    return model
end

def load_cube_from_file(file)
	hash = YAML.load_file(file)
	hash = hash.hash_by_symbolising_keys

    cube = self.cubes.new

	cube.name = hash[:name]
	cube.label = hash[:label]
	cube.description = hash[:description]
	cube.fact_dataset_name = hash[:fact_dataset]

    ################################################################
    # 2. Joins
    
    if hash[:joins]
        hash[:joins].each { | join_info |
            master = join_info['master'].split('.')
            detail = join_info['detail'].split('.')
            join = cube.joins.new
            join.master_dataset_name = master[0]
            join.master_key = master[1]
            join.detail_dataset_name = detail[0]
            join.detail_key = detail[1]

            if ! dataset_description_with_name(join.master_dataset_name)
                raise ArgumentError, "Unknown master dataset '#{join.master_dataset_name}'"
            end
            if ! dataset_description_with_name(join.detail_dataset_name)
                raise ArgumentError, "Unknown detail dataset '#{join.detail_dataset_name}'"
            end
        }
	end
	
	if hash[:dimensions]
	    hash[:dimensions].each { |dim_name|
            dim = dimension_with_name(dim_name)
            cube.dimensions << dim
	    }
	end

	if !cube.save
        raise "Unable to save cube"
    end

    #     joins = hash[:dimensions]
    # 
    #     dim_joins.each { |dim_name, join_info|
    #         join_info = join_info.hash_by_symbolising_keys
    #         dim = dimensions.first( :name => dim_name)
    #         # puts "--> join: #{dim_name}(#{dim_name.class}) - #{dim.class}"
    #         if !dim
    #             raise RuntimeError, "Unknown dimension '#{dim_name}' in model '#{self.name}' file '#{file}'"
    #         end
    #         cube.join_dimension(dim, join_info[:fact_key])
    #     }
end

def validate
    results = []
    
    ################################################################
    # 1. Chceck existence of ds,cubes
    
    if dataset_descriptions.count == 0
        results << [:warning, "No dataset descriptions specified"]
    end
    
    ################################################################
    # 2. Chceck dimensions

    dimensions.each { |dim|
        if !dim.default_hierarchy
            results << [:warning, "No default hirerarchy specified for dimension '#{dim.name}'"]
        end
    }


    if cubes.count == 0
        results << [:warning, "No cubes defined"]
    else
        cubes.each { | cube |
            results.concat(cube.validate)
        }
    end

    return results
end

# Returns model with given name
def self.model_with_name(name)
    model = self.first(:name => name)
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

def dimension_with_name(name)
    return dimensions.first( :name => name )
end

def field_reference(field)
    return field.split('.')
end

end # class
end # module