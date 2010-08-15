# ETL - main ETL header
#
# Copyright:: (C) 2010 Knowerce, s.r.o.
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

require 'rubygems'
require 'sequel'
require 'dm-core'
require 'dm-types'
require 'dm-migrations'
require 'dm-is-list'
require 'data_objects'

require 'brewery/core_ext/class'
require 'brewery/core_ext/hash'
require 'brewery/core_ext/numeric'
require 'brewery/core_ext/string'

require 'brewery/core/hierarchy_tree'
require 'brewery/core/dataset'
require 'brewery/core/data_store_manager'
require 'brewery/core/data_table'
require 'brewery/core/dataset_description'
require 'brewery/core/field_description'

require 'brewery/core/downloader'
require 'brewery/core/download_batch'
require 'brewery/core/download_manager'

# ETL

require 'brewery/etl/etl_job_bundle'
require 'brewery/etl/etl_job'
require 'brewery/etl/etl_manager'
require 'brewery/etl/etl_defaults'

# Quality
require 'brewery/quality/quality_auditor'

# Cubes
require 'brewery/cubes/aggregation_result'
require 'brewery/cubes/cube'
require 'brewery/cubes/dimension'
require 'brewery/cubes/dimension_level'
require 'brewery/cubes/cube_dimension_join'
require 'brewery/cubes/slice'
require 'brewery/cubes/cut'
require 'brewery/cubes/workspace'
require 'brewery/cubes/logical_model'
require 'brewery/cubes/hierarchy'
require 'brewery/cubes/star_query'

# require 'brewery-streams'

# == Boom:
# This is module description. An another sentence.
# @author boo
module Brewery

@@default_configuration_files = [
					'./config/brewery.yml',
					'~/.brewery/config.yml',
					'/etc/brewery/config.yml'
				]
@@configuration = nil
@@logger = nil
@@debug = false

# Get default data store manager. Short-cut for {DataStoreManager#default_manager}
def self.data_store_manager
	return DataStoreManager::default_manager
end

# Load default brewery configuration from files.
# The files are being read in the following order:
# * ./config/brewery.yml - configuration for current project
# * ~/.brewery.yml - user's configuration
# * /etc/brewery.yml - system-wide configuration
#
# First file found is loaded, the others are ignored
def self.load_default_configuration
	file = @@default_configuration_files.detect { |file|
			path = Pathname.new(file).expand_path
			path.exist? && path.file?
		}

	if file
		self.load_configuration_from_file(file)
	end
end

# FIXME: Check for Rails existence
def self.load_rails_configuration
  config_dir = Pathname.new(Rails.root) + "config"

    Brewery::load_configuration_from_file(config_dir + "brewery.yml")

    store_manager = Brewery::data_store_manager
    ['brewery-data-stores', 'database'].each do |file|
      if (config_dir+file).exist?
        store_manager.add_stores_in_file(config_dir+file)
      end
    end
end

# Configure brewery from a YAML file.
# @see Brewery#configure_from_hash
def self.load_configuration_from_file(file)
	path = Pathname.new(file).expand_path

	begin
	    config = YAML.load_file(path)
	rescue => exception
		raise RuntimeError, "Unable to read brewery configuration file #{file}."
	end
	
	self.configure_from_hash(config)
end

# Configure brewery using hash.
def self.configure_from_hash(config)
	log_file = config["log_file"]
	if log_file == "STDERR"
	    self.log_file = STDERR
	else
		self.log_file = log_file
	end

    path = config["job_search_path"]
	if path.is_kind_of_class(String)
		ETLJobBundle.job_search_path = [path]
	elsif path.is_kind_of_class(Array)
		ETLJobBundle.job_search_path = path
	elsif path
		# FIXME: Use log
		puts 'Unknown job search path type (should be string or array)'
	end

    files = config["data_store_files"]
	if files.is_kind_of_class(Array)
		files.each { |file|
			DataStoreManager.default_manager.add_stores_in_file(file)
		}
	elsif files
		# FIXME: use log
		puts 'Unknown data_store_files value type (should be array)'
	end

    data_store = config["brewery_data_store"]
	if data_store.is_kind_of_class(String)
		self.set_brewery_datastore(data_store)
	elsif data_store
		# FIXME: use log
		puts 'Unknown brewery_data_store value type (should be array)'
	end

	@@configuration = config
end

# @returns default workspace
def self.workspace
	return Workspace.default_workspace
end

# Creates a new workspace and set it as default.
# @see Workspace#initialize
# @see DataStoreManager#create_connection
def self.create_default_workspace(connection)
    workspace = Brewery::Workspace.new(connection)
    workspace.set_default
end

# Set datastore which will be used for brewery
# @param [String, Symbol] name - name of the datastore. See: {DataStoreManager#data_store}
def self.set_brewery_datastore(name)
	datastore = DataStoreManager.default_manager.data_store(name)
	if !datastore
		raise "Datastore '#{name}' not found"
	end

	DataMapper.setup(:default, datastore)
end

# Initialize datastore structures (database tables) used by brewery. If no tables exist, they will
# be created. If there are already Brewery structures in the datastore (database), they will
# be destroyed.
#
# Warning: This is destructive operation
def self.initialize_brewery_datastore
	# FIXME: this is destructive!
    DataMapper.finalize
	DataMapper.auto_migrate!
end

# Upgrade datastore structures (database tables) used by brewery to match current brewery structure
def self.upgrade_brewery_datastore
    DataMapper.finalize
	DataMapper.auto_upgrade!
end

# Get Brewery configuration hash. See also: {Brewery#configure_from_file}
def self.configuration
	return @@configuration
end


def self.log_file=(file)
    @@logger = Logger.new(file)
    @@logger.formatter = Logger::Formatter.new
    @@logger.datetime_format = '%Y-%m-%d %H:%M:%S '
    if @@debug
        @@logger.level = Logger::DEBUG
    else
        @@logger.level = Logger::INFO
    end
end

def self.logger
	return @@logger
end

def logger
	return Brewery::logger
end

end # module

