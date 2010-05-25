# Data Source Manager
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

class DataSourceManager

@@default_manager = nil
@@default_files = [
					'/etc/brewery/data-sources.yml',
					'~/.brewery/data-sources.yml',
					'./config/brewery-data-sources.yml'
				]

def self.default_manager
	if !@@default_manager
		@@default_manager = self.new
		@@default_manager.add_sources_in_default_files
	end
	return @@default_manager
end

def initialize
	@data_sources = Hash.new
	@file_data_sources = Hash.new
	@files = Array.new
	
	@connections = Hash.new
	@watched_files = Array.new
end

def add_sources_in_default_files
	for file in @@default_files
		path = Pathname.new(file).expand_path
		if path.exist? and path.file?
			add_sources_in_file(file)
		end
	end

end

def add_sources_in_file(path)
	hash = YAML.load_file(path)
	
	symbolised_hash = Hash.new
	hash.keys.each { |key|
		symbolised_hash[key.to_sym] = hash[key]
	}
	
	@file_data_sources[path] = symbolised_hash
	
	if not @files.include?(path)
		@files << path
	end
end

def remove_sources_in_file(path)
	@file_data_sources.delete(path)
	@files.delete(path)
end

def add_sources_from_hash(hash)
	# Do not use merge, we need to convert keys to symbols
	hash.keys.each { |key|
		add_source(key, hash[key])
	}
end

def add_source(name, description)
	@data_sources[name.to_sym] = description
end

def source(name)
	name = name.to_sym

	src = @data_sources[name]

	if src
		return src
	end

	@files.each { |file|
		file_sources = @file_data_sources[file]
		src = file_sources[name]

		if src
			break
		end
	}

	return src
end

def create_connection(src_name, identifier = nil)
	# FIXME: rename to create_named_connection
	src = source(src_name)
	if src
		connection = Sequel.connect(src)
		
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
