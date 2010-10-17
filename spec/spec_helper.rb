require 'rspec'
require 'brewery'
require 'pathname'

SPEC_ROOT = Pathname(__FILE__).dirname.expand_path

Pathname.glob((SPEC_ROOT + '{lib,support,*/shared}/**/*.rb').to_s).each { |file| require file }

RSpec.configure do |config|
    Brewery::Test.initialize_for_test
end
