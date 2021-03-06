# ETL Manager 
#
# Copyright:: (C) 2010 Knowerce, s.r.o.
# 
# Author:: Stefan Urbanek
# Date:: Oct 2010
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

require 'brewery/etl/etl_job_schedule'
require 'brewery/etl/etl_job_status'

require 'logger'

module Brewery

class ETLManager
attr_reader :connection
attr_reader :log
attr_reader :etl_files
attr_accessor :debug

@@__default_manager = nil

def self.default_manager
	if !@@__default_manager
		@@__default_manager = self.new
	end
	return @@__default_manager
end

def initialize
	@job_search_path = Array.new
    @log = Brewery::logger
	# FIXME: document this
	if Brewery.configuration
		path = Brewery.configuration["etl_files_path"]
		if path
			@etl_files_path = Pathname.new(path)
		else
			@etl_files_path = Pathname.new("/tmp/brewery-etl-files")
		end
	end
	
	# check_etl_schema
end

################################################################
# Initialization

def create_etl_manager_structures(options = {})
	@log.info "Creating ETL manager structures"
	if options[:force] == true
		DataMapper.auto_migrate!
	else
		DataMapper.auto_upgrade!
	end
end

def log_file=(logfile)
    @log = Logger.new(logfile)
    @log.formatter = Logger::Formatter.new
    @log.datetime_format = '%Y-%m-%d %H:%M:%S '
    if @debug
        @log.level = Logger::DEBUG
    else
        @log.level = Logger::INFO
    end
end

################################################################
# Jobs

def all_schedules
	return ETLJobSchedule.all
end

def planned_schedules(schedule = nil)

	if !schedule
		date = Date.today
		week_days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
		schedule = week_days[date.wday]
	end
	
	# Only daily/weekly schedules work at the moment

	expr = "is_enabled AND (force_run = 1 OR schedule = ? OR schedule = 'daily')"
	schedules = ETLJobSchedule.all(:conditions => [expr, schedule], :order => [:run_order])

    return schedules
end

def forced_schedules
	conds = {:is_enabled => 1, :force_run => 1}
	schedules = ETLJobSchedule.all(:conditions => conds, :order => [:run_order])

    return schedules
end

################################################################
# Job running

def run_scheduled_jobs
	jobs = planned_schedules
	@log.info "Running scheduled jobs (#{jobs.count})"
	run_schedules(jobs)
end

def run_forced_jobs
	jobs = forced_schedules
	@log.info "Running forced jobs (#{jobs.count})"
	run_schedules(jobs)
end

def run_schedules(schedules)
	if schedules.nil? or schedules.empty?
        @log.info "No schedules to run"
	end	

	schedules.each { |schedule|
	    # @log.info "Schedule #{schedule.id}: #{schedule.job_name}(#{schedule.argument})"
		run_named_job(schedule.job_name, schedule.argument)
	}
end

def run_named_job(name, argument = nil)

	# FIXME: reset force run flag
	bundle = ETLJobBundle.bundle_with_name(name)
	if not bundle
		@log.error "Job #{name} does not exist"
		return
	end
	
	if not bundle.is_loaded
		@log.info "Loading bundle for job #{name}"
		bundle.load
	end
	
	job = bundle.job_class.new(self, bundle)

	self.run_job(job, argument)
end

# FIXME: continue here
def run_job(job, argument)
	error = false

    @log.info "Running job '#{job.name}' with argument '#{argument}'"

    job_start_time = Time.now

	# FIXME: instantiate for each run (keep class not instance)

	# Prepare job status
	options = Hash.new
	
	if @debug
		options[:debug] = true
	end
	
	job.launch_with_argument(argument, options)
end


################################################################
# Defaults

def defaults_for_domain(domain)
	defaults = ETLDefaults.new(self, domain)
	return defaults
end

# Other
def etl_files_path=(path)
    @etl_files_path = Pathname.new(path)
end

def files_directory_for_job(job)
	domain = job.defaults_domain
	if !domain
		domain = job.name
	end
	
	return @etl_files_path + domain
	
end

end # class
end # module