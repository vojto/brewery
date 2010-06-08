module Brewery

class Model
    include DataMapper::Resource

    property :id,   Serial
    property :name, String
    property :description, Text

    has      n, :dimensions,  {:through=>DataMapper::Resource}
    # has      n, :cubes

    @@model_search_paths = [
                        './models',
                        '~/.brewery/models'
                    ]

def self.model_from_path(path)
    model = Model.new
    model.load_from_path(path)
	return model
end

def load_from_path(path)
    path = Pathname.new(path)
    model_file = path + 'model.yml'
    
	hash = YAML.load_file(model_file)
	hash = hash.hash_by_symbolising_keys
	
	files = hash[:files]
	
	self.name = hash[:name]
	self.description = hash[:description]
	
	path.children.each { | file |
	    prefix = file.basename.to_s.split('_')[0]
	    # puts "CREATING #{prefix} from #{file}"
	    case prefix
	    when "dim", "dimension"
	        dim = Dimension.new_from_file(file)
	        dimensions << dim
	    end
	}
end

end # class
end # module