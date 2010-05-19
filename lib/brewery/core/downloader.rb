# Downloader
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

require 'typhoeus'
require 'monitor'
require 'pathname'

module Brewery

module DownloaderDelegate
def download_failed(downloader, exception)
    puts "ERROR: Download failed: #{exception.message}"
    puts exception.backtrace.join("\n")
end

def download_response_processing_failed(downloader, exception, response)
    puts "ERROR: Processing failed: #{exception.message}"
    puts exception.backtrace.join("\n")
end

def download_process_response(downloader, response)
    # do nothing by default
end

def download_did_finish(downloader)
	# do nothing
end
end

class Downloader
attr_accessor :download_directory
attr_accessor :requests
attr_accessor :delegate
attr_accessor :concurrency

def initialize
    @download_directory = Pathname.new(".")
end

def download_directory=(path)
	@download_directory = Pathname(path)
end

def download(options = {})
    @processing_queue = Array.new

    # Prepare thread controll variables
    @mutex = Monitor.new
    @lock = @mutex.new_cond
    @download_finished = false

	# Spawn download thread
	
	download_thread = Thread.new do
		download_thread_core
	end
	
    processing_thread = Thread.new do
        processing_thread_core
    end
    
    # Wait for downloads to finish
    download_thread.join
    
    @download_finished = true
	
    @mutex.synchronize do
        @lock.broadcast
    end

    processing_thread.join
end
def download_thread_core
	# puts "==> DOWNLOAD THREAD STARTED"
	begin
		download_hydra
	rescue => exception
		@delegate.download_failed(self, exception)
	end
	@delegate.download_did_finish(self)
end

def processing_thread_core
	# puts "==> PROCESSING THREAD STARTED"
    loop do
        response = nil
        # puts "--> SYNC"
        @mutex.synchronize do
            break if @download_finished and @processing_queue.empty?

            @lock.wait_while { @processing_queue.empty? && (! @download_finished) }

            if not @processing_queue.empty?
                response = @processing_queue.shift
            end
        end
        break unless response

        begin
        	# puts "==> PROCESS REQUEST #{request[:url]}"
			@delegate.downloader_process(self, response)
		rescue => exception
			@delegate.download_response_processing_failed(self, exception, response)
	    end

    end
	# puts "==> PROCESSING THREAD FINISHED"
end

def process_download(url_info, path, response)
	file = File.new(path, "wb")
	file.puts response.body
	file.close

	hash = {
		:url => url_info[:url],
		:file => path,
		:status_code => response.code,
		:user_info => url_info[:user_info]
	}
	# puts "ADDING TO QUEUE #{url_info[:url]}"
	@mutex.synchronize do
		 @processing_queue << hash
		 @lock.broadcast
	end
end

def download_hydra
	if ! @downloads
		@downloads = Array.new
	end

	max_concurrency = (@concurrency) ? @concurrency : 20
	hydra = Typhoeus::Hydra.new(:max_concurrency => max_concurrency)

	@requests.each { |request_info|
		url = request_info[:url]
		filename = request_info[:filename]

		if !filename || filename == ""
			filename = url.split(/\?/).first.split(/\//).last
		end
			
		path = @download_directory + filename

		reqest = Typhoeus::Request.new(url)

		reqest.on_complete do | response |
			process_download(request_info, path, response)
		end
		
		hydra.queue(reqest)
	}

	hydra.run	
end


def on_success(&block)
  @on_success_block = block
end

def on_success=(block)
  @on_success_block = block
end

end # class

end # module