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

# Class for managing data stores and their connections.
# @example Get data store  information
#   store_manager = Brewery::data_store_manager
#   store_info = store_manager("my_database")
# @example Create a connection to development database
#   store_manager = Brewery::data_store_manager
#   connection = store_manager.create_connection("shop_development")
# @example Load database info from Rails application and connect to a database:
#   rails_dbs_file = Rails.root / 'config' / 'database.yml'
#   store_manager = Brewery::data_store_manager
#   store_manager.add_stores_in_file(rails_dbs_file)
#   connection = store_manager.create_connection("production")
# == Limitations:
# * Currently data store manager supports only database data stores.
# * All connections are Sequel connections
#
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

# Load information about data stores in default files. The default files are:
# * /etc/brewery/data-stores.yml
# * ~/.brewery/data-stores.yml
# * ./config/brewery-data-stores.yml
def add_stores_in_default_files
	for file in @@default_files
		path = Pathname.new(file).expand_path
		if path.exist? and path.file?
			add_stores_in_file(path)
		end
	end

end

# Add data store information from file specified by path. The file should be a
# YAML file containing hash where keys are store names and values are store
# information.
# @param [Pathname] path Path to file with data store information
# @see DataStoreManager#add_data_store
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

# Add data stores contained in hash
# @param [Hash] hash Hash of data store information where keys are data store names and
#   values are data store specifications.
# @see #add_data_store
def add_stores_from_hash(hash)
	# Do not use merge, we need to convert keys to symbols
	hash.keys.each { |key|
		add_data_store(key, hash[key])
	}
end

# Add data store information from description
# @param [Symbol, String] name Name (identifier) of data store
# @param [Hash, String] description containing data store connection information. Can be string URI
#   or hash with keys such as: :database, :adapter, :user, :host, :password, ...
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


# Establish a database connection to given store
# @param [String, Symbol] store_name Name of data store to be connected
# @param [String, Symbol] identifier Identifier of new connection for possible further
#   reuse by {#named_connection}
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

# Add existing connection
# @param connection Established database connection
# @param [String, Symbol] identifier Identifier of the connection
# @see #named_connection
def add_named_connection(connection, identifier)
	@connections[identifier] = connection
end

def remove_named_connection(identifier)
	@connections.delete(identifier)
end

# @param [String, Symbol] name Name of established connection
# @return connection
# @see #create_connection
def named_connection(name)
	return @connections[name]
end

end # class

end # module
