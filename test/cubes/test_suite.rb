# require 'test/unit/testsuite'
require 'test/unit'
require 'cubes_test'

class TS_BreweryCubesTests

def self.suite
	suite = Test::Unit::TestSuite.new
 	suite << BreweryCubesTest.suite
end

end
