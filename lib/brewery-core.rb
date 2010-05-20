# Brewery - core shared funcionality
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

require 'brewery/core/downloader'
require 'brewery/core/download_batch'
require 'brewery/core/download_manager'

module Brewery

# Get default repository manager. Short-cut for [Brewery::RepositoryManager.default_manager]
def self.repository_manager
	return RepositoryManager.default_manager
end

end # module

