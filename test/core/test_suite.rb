require 'test/unit/testsuite'
require 'core_test'

class TS_BreweryCoreTests

def self.suite
	suite = Test::Unit::TestSuite.new
 	suite << BreweryCoreTest.suite
end

end
