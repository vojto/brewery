require 'test/unit/testsuite'
require 'node_test'

# require 'datastore_test'
# require 'node_streaming_test'

class TS_BreweryTests

def self.suite
     suite = Test::Unit::TestSuite.new
    suite << NodeTest.suite

#    suite << DatastoreTest.suite
#    suite << NodeStreamingTest.suite
end

end
