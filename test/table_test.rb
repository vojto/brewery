require 'test/unit'
require 'brewery'
require 'rubygems'
require 'sequel'

class TestNodes < Test::Unit::TestCase
def setup
    @test_file = 'test/test.csv'
end

def test_file_to_table
    input = FileSourceNode.new
    input.filename = @test_file
    input.reads_field_names = true
    input.prepare

    assert_not_nil(input.fields)
    assert_equal(input.fields.count, 3)

    input.set_storage_type_for_field("name", :string)
    input.set_storage_type_for_field("surname", :string)
    input.set_storage_type_for_field("amount", :integer)

    output = TableOutputNode.new
    output.table_name = 'test_table'
    output.mode = :replace
    output.create_table = true
    connection = Sequel.connect('sqlite://test/test.sqlite')
    output.connection = connection
    output.input_node = input

    output.prepare
    output.evaluate
    
    table = connection[:test_table]
    assert_equal(table.count, 4)
end
end