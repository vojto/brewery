# Repository Manager
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

class RepositoryManager
attr_accessor :search_path

@@default_manager = nil

def self.default_manager
	if !@@default_manager
		@@default_manager = self.new
	end
	return @@default_manager
end

def initialize
	@repositories = Hash.new
	@connections = Hash.new
	@search_path = Array.new
end

def add_repositories_from_file(path)
	hash = YAML.load_file(path)
	add_repositories_from_hash(hash)
end

def add_repositories_from_hash(hash)
	@repositories.merge!(hash)
end

def add_repository(name, rep)
	@repositories[name] = rep
end

def repository(name)
	repo = @repositories[name]
	if not repo
		repo = @repositories[name.to_s]
	end

	if repo
		return repo
	end

	file_repos = Hash.new
	if search_path
		@search_path.each { |search_path|
			path = Pathname.new(search_path)
			if path.exist?
				path.children.each { |file|
					hash = YAML.load_file(file)
					file_repos.merge!(hash)
				}
			end
		}
	end
	
    return file_repos[name]
end

def create_connection(repo_name, identifier = nil)
	# FIXME: rename to create_named_connection
	repo = repository(repo_name)
	if repo
		connection = Sequel.connect(repo)
		
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

end

end
