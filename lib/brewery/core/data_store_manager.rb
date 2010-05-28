# Data Store Manager
#
# Copyright:: (C) 2010 Stefan Urbanek
# 
# Author:: Stefan Urbanek
# Date:: May 2010
#

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


module Brewery

class DataStoreManager

@@default_manager = nil
@@default_files = [
					'/etc/brewery/data-stores.yml',
					'~/.brewery/data-stores.yml',
					'./config/brewery-data-stores.yml'
				]

def self.default_manager
	if !@@default_manager
		@@default_manager = self.new
		@@default_manager.add_stores_in_default_files
	end
	return @@default_manager
end

def initialize
	@data_stores = Hash.new
	@file_data_stores = Hash.new
	@files = Array.new
	
	@connections = Hash.new
	@watched_files = Array.new
end

def add_stores_in_default_files
	for file in @@default_files
		path = Pathname.new(file).expand_path
		if path.exist? and path.file?
			add_stores_in_file(file)
		end
	end

end

def add_stores_in_file(path)
	hash = YAML.load_file(path)
	
	symbolised_hash = Hash.new
	hash.keys.each { |key|
		symbolised_hash[key.to_sym] = hash[key]
	}
	
	@file_data_stores[path] = symbolised_hash
	
	if not @files.include?(path)
		@files << path
	end
end

def remove_stores_in_file(path)
	@file_data_stores.delete(path)
	@files.delete(path)
end

def add_stores_from_hash(hash)
	# Do not use merge, we need to convert keys to symbols
	hash.keys.each { |key|
		add_store(key, hash[key])
	}
end

def add_data_store(name, description)
	@data_stores[name.to_sym] = description
end

def data_store(name)
	name = name.to_sym

	store = @data_stores[name]

	if store
		return store
	end

	@files.each { |file|
		file_stores = @file_data_stores[file]
		store = file_stores[name]

		if store
			break
		end
	}

	return store
end

def available_data_stores
	stores = Array.new
	stores << @data_stores.keys

	@files.each { |file|
		file_stores = @file_data_stores[file]
		stores << file_stores.keys
	}

	return stores.uniq
end

def create_connection(store_name, identifier = nil)
	# FIXME: rename to create_named_connection
	store = data_store(store_name)
	if store
		connection = Sequel.connect(store)
		
		if identifier
			add_named_connection(connection, identifier)
		end
		
		return connection
	end
	return nil
end

def add_named_connection(connection, identifier)
	@connections[identifier] = connection
end

def remove_named_connection(identifier)
	@connections.delete(identifier)
end

def named_connection(name)
	return @connections[name]
end

end # class

end # module
