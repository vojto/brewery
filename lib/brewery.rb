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
require 'brewery/core/repository_manager'
require 'brewery/core/class_additions'
require 'brewery/core/string_additions'
require 'brewery/core/hierarchy_tree'
require 'brewery/core/dataset'

require 'brewery/core/downloader'
require 'brewery/core/download_batch'
require 'brewery/core/download_manager'

# ETL
require 'sequel'
require 'dm-core'

require 'brewery/core/data_store_manager'
require 'brewery/core/class_additions'

require 'brewery/etl/etl_job_bundle'
require 'brewery/etl/etl_job'
require 'brewery/etl/etl_manager'
require 'brewery/etl/etl_defaults'

# Quality
require 'brewery/quality/quality_auditor'

# Cubes
require 'brewery/cubes/cube.rb'
require 'brewery/cubes/dimension.rb'
require 'brewery/cubes/slice.rb'
require 'brewery/cubes/cut.rb'
require 'brewery/cubes/workspace.rb'

# require 'brewery-streams'

module Brewery

@@default_configuration_files = [
					'./config/brewery.yml',
					'~/.brewery/config.yml',
					'/etc/brewery/config.yml'
				]
@@configuration = nil
@@logger = nil
@@debug = false


# Get default data store manager. Short-cut for [Brewery::DataStoreManager.default_manager]
def self.data_store_manager
	return DataStoreManager::default_manager
end

# Load default brewery configuration from files in the following order:
# # ./config/brewery.yml - configuration for current project
# # ~/.brewery.yml - user's configuration
# # /etc/brewery.yml - system-wide configuration
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

def self.load_configuration_from_file(file)
	path = Pathname.new(file).expand_path

	begin
	    config = YAML.load_file(path)
	rescue => exception
		raise RuntimeError, "Unable to read brewery configuration file #{file}."
	end
	
	self.configure_from_hash(config)
end

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
			DataSourceManager.default_manager.add_stores_in_file(file)
		}
	elsif files
		# FIXME: use log
		puts 'Unknown data_store_files value type (should be array)'
	end

	@@configuration = config
end

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
end # module

