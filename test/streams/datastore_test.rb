require 'test/unit'
require 'brewery'
require 'rubygems'
require 'sequel'

class TestNodes < Test::Unit::TestCase
def setup
    # Memory database
    # @connection = Sequel.connect('sqlite:/')
    @connection = Sequel.connect('sqlite://datastore.sqlite')

    @fields = Array.new

    field = Field.new
    field.name = 'name'
    field.storage_type = :string
    @fields << field

    field = Field.new
    field.name = 'surname'
    field.storage_type = :string
    @fields << field

    field = Field.new
    field.name = 'amount'
    field.storage_type = :integer
    @fields << field
end

def test_datastore
    datastore = Datastore.new(@connection)
    
    datastore.setup
    datastore.cleanup
    assert_equal(0, datastore.datasets.count)
    
    dataset = datastore.crete_temporary_dataset(@fields)
    dataset = datastore.crete_temporary_dataset(@fields)
    dataset = datastore.crete_temporary_dataset(@fields)
    assert_equal(3, datastore.datasets.count,3)

    datastore.cleanup
    assert_equal(0, datastore.datasets.count, "testing clean datastore")
end

end