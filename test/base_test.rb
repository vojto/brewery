require 'test/unit'
require 'brewery'
require 'rubygems'
require 'sequel'

class TestStream < Test::Unit::TestCase
def setup
    @test_file = 'test/test.csv'
    @out_file = 'test/out.csv'
    @test_table = 'test_table'
    @stream_file = 'test/stream.yml'
    
    @connection = Sequel.connect('sqlite://test/test.sqlite')
    ds_connection = Sequel.connect('sqlite://test/datastore.sqlite')
    @datastore = Datastore.new(ds_connection)
    @datastore.setup
    @datastore.cleanup
    
    setup_nodes
    setup_fields
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

end

def setup_fields
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

def test_stream_creation
    stream = Stream.new

    assert_raise ArgumentError do
        stream.connect_nodes(@file_in, @table_out)
    end

    stream.add_node(@file_in)
    stream.add_node(@table_out)

    assert_equal(2, stream.nodes.count)

    assert_raise ArgumentError do
        stream.connect_nodes(@table_out, @file_in)
    end

    assert_nothing_raised do
        stream.connect_nodes(@file_in, @table_out)
    end

    stream.set_node_name(@file_in, "foo")
    assert_equal(@file_in, stream.node_with_name("foo"))

    stream.set_node_name(@table_out, "foo")
    assert_equal(@table_out, stream.node_with_name("foo"))

    stream.remove_node(@file_in)
    stream.remove_node(@table_out)

    assert_equal(0, stream.nodes.count)
    assert_equal(nil, stream.node_with_name("foo"))
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

def test_node_from_hash
    file_in_hash = {:type => "string"}
    assert_raise ArgumentError do
        node = Node.new_from_hash(file_in_hash)
    end

    file_in_hash = {"type" => "file_source",
                    "filename" => "test.csv",
                    "reads_field_names" => true }

    node = Node.new_from_hash(file_in_hash)
    assert_equal("test.csv", node.filename)
    assert_equal(true, node.reads_field_names)

    table_out_hash = {"type" => "table_output",
                    "table_name" => "output_table",
                    "mode" => "replace",
                    "create_table" => true }

    node = Node.new_from_hash(table_out_hash)

    assert_equal("output_table", node.table_name)
    assert_equal(:replace, node.mode)
    assert_equal(true, node.create_table)
end

def test_stream_from_file
    stream = Stream.new
    stream.read_nodes_from_file(@stream_file)
    
    input = stream.node_with_name("file_in")
    assert_not_nil(input, "node with name file_in does not exist")
    assert_equal(FileSourceNode, input.class)

    assert_equal("test.csv", input.filename)
    assert_equal(true, input.reads_field_names)

    output = stream.node_with_name("table_out")
    assert_equal(TableOutputNode, output.class)

    assert_equal("output_table", output.table_name)
    assert_equal(:replace, output.mode)
    assert_equal(true, output.create_table)
end

end
