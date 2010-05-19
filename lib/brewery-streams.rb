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

require 'brewery/streams/node'
require 'brewery/streams/datastore'
require 'brewery/streams/stream'

# Nodes
# Input Nodes
require 'brewery/streams/file_source_node'
# Record Processing Nodes
require 'brewery/streams/merge_node'
require 'brewery/streams/aggregate_node'
require 'brewery/streams/select_node'
# Field Nodes
require 'brewery/streams/derive_node'
require 'brewery/streams/field_map_node'
# require 'type_node'
# Output Nodes
require 'brewery/streams/table_output_node'
require 'brewery/streams/file_output_node'