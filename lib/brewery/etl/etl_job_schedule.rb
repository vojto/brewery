# ETL Job Schedule
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

class ETLJobSchedule
	include DataMapper::Resource

	property :id,          Serial
	property :job_name,    String
	property :argument,    String
	property :is_enabled,  Integer
	property :schedule,    String
	property :force_run,   Integer
	property :run_order,   Integer
end # class

end # module
