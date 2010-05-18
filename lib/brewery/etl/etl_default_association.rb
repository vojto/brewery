# Dictionary Association for ETL Defaults
#
# Copyright (C) 2009 Stefan Urbanek
# 
# Author:: Stefan Urbanek
# Date:: November 2009
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

class ETLDefaultAssociation
	include DataMapper::Resource

	property :id,            Serial
	property :domain,        String
	property :default_key,   String, :field => "key"
	property :default_value, Text, :field => "value"
end # class

end # module
