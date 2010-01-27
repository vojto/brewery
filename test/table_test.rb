require 'test/unit'
require 'brewery'
require 'rubygems'
require 'sequel'

class TestNodes < Test::Unit::TestCase
def setup
    @test_file = 'test/test.csv'
    @out_file = 'test/out.csv'
    @test_table = 'test_table'
    
    @connection = Sequel.connect('sqlite://test/test.sqlite')
    ds_connection = Sequel.connect('sqlite://test/datastore.sqlite')
    @datastore = Datastore.new(ds_connection)
    @datastore.setup
    @datastore.cleanup
    
    setup_nodes
end

def setup_nodes
    # Prepare file input node
    @file_in = FileSourceNode.new
    @file_in.filename = @test_file
    @file_in.reads_field_names = true

    # Prepare table output node
    @table_out = TableOutputNode.new
    @table_out.table_name = @test_table
    @table_out.mode = :replace
    @table_out.create_table = true
    @table_out.connection = @connection

    if @connection.table_exists?(@test_table.to_sym)
        @connection.drop_table(@test_table.to_sym)
    end

#    @file_out = FileOutputNode.new
#    @file_out.filename = @out_file
#    @file_out.mode = :replace
#    @file_out.create_table = true
#    @file_out.connection = @connection
end
def test_file_to_table

    @file_in.prepare
    @file_in.set_storage_type_for_field("name", :string)
    @file_in.set_storage_type_for_field("surname", :string)
    @file_in.set_storage_type_for_field("amount", :integer)

    prepare_stream(@file_in)
end

def prepare_stream(node)
    if node.creates_dataset
        dataset = @datastore.crete_temporary_dataset(node.fields)
    else
        if node.input_limit
            
end

    @file_in.execute



    assert_not_nil(@file_in.fields)
    assert_not_nil(@file_in.created_fields)
    assert_equal(@file_in.fields, @file_in.created_fields)
    assert_equal(true, @file_in.creates_dataset)
    assert_equal(@file_in.fields.count, 3)

    @table_out.prepare

    # Fake stream, execute nodes:
    dataset = @datastore.crete_temporary_dataset(@file_in.fields)

    @file_in.execute(nil, dataset)
    @table_out.fields = @file_in.fields
    puts "==> DS #{dataset.class}"
    @table_out.execute([dataset], nil)
    
    assert_equal(true, @connection.table_exists?(@test_table.to_sym))

    table = @connection[:test_table]
    assert_equal(4, table.count)
end
end