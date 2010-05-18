require 'test/unit'
require 'brewery'
require 'rubygems'
require 'sequel'
require 'datastore_table'

class NodeStreamingTest < Test::Unit::TestCase
def setup
    @test_file = 'data/customers.csv'
end

def test_real_stream
    stream = Stream.new
    
    ##############################
    # NODE 1: file source
    #

    node1 = FileSourceNode.new
    node1.filename = @test_file
    node1.field_separator = ';'
    node1.reads_field_names = true
    node1.prepare
    stream.add_node(node1, "file_input")

    ##############################
    # NODE 2: derive
    #
    node2 = DeriveNode.new
    node2.derived_field_name = "full_name"

    stream.add_node(node2, "derive_name")
    stream.connect_nodes(node1, node2)

    ##############################
    # NODE 3: map
    #
    node3 = FieldMapNode.new

    stream.add_node(node3, "map")
    stream.connect_nodes(node2, node3)
    field = node3.field_with_name("id")
puts "==> #{field.name}"
    node3.set_field_name(field, "customer_id")

    field = node3.field_with_name("name")
    node3.set_field_action(field, :delete)

    field = node3.field_with_name("surname")
    node3.set_field_action(field, :delete)
    
    ##############################
    # NODE 4: aggregate
    #
    node4 = AggregateNode.new
   
    field = node4.field_with_name("id")
    node4.set_field_aggregations(field, [:sum, :avg])

    field = node4.field_with_name("city")
    node4.group_fields = [field]
    node4.include_count = true
end

def test_fake_stream
    ##############################
    # NODE 1: file source
    #

    node1 = FileSourceNode.new
    node1.filename = @test_file
    node1.field_separator = ';'
    node1.reads_field_names = true
    node1.prepare
    fields = node1.created_fields

    pipe1 = Pipe.new

    # We require table now

    node1.output_pipe = pipe1
    node1.setup_output_pipe_table
    table1 = node1.output_pipe.table
    
    assert_not_equal(nil, table1)
    assert_equal(node1.fields, pipe1.fields)
    assert_equal(node1.fields.count, table1.columns.count)

    ##############################
    # NODE 2: derive
    #
    node2 = DeriveNode.new
    node2.derived_field_name = "full_name"
    node2.add_input_pipe(pipe1)

        
    pipe2 = Pipe.new
    node2.output_pipe = pipe2
    node2.setup_output_pipe_table

    table2 = node2.output_pipe.table
    assert_equal(table1, table2)
    assert_equal(node1.fields.count + 1, table2.columns.count)

    ##############################
    # NODE 3: map
    #
    node3 = FieldMapNode.new
    node3.add_input_pipe(pipe2)

    field = pipe2.field_with_name("id")
    assert_not_equal(nil, field)
    node3.set_field_name(field, "customer_id")

    field = pipe2.field_with_name("name")
    node3.set_field_action(field, :delete)
    field = pipe2.field_with_name("surname")
    node3.set_field_action(field, :delete)


    pipe3 = Pipe.new
    node3.output_pipe = pipe3
    node3.setup_output_pipe_table

    table3 = node3.output_pipe.table
    assert_equal(table1, table3)
    assert_equal(node1.fields.count + 1, table2.columns.count)
    
    ##############################
    # NODE 4: aggregate
    #
    node4 = AggregateNode.new
    node4.add_input_pipe(pipe3)
    
    field = pipe3.field_with_name("id")
    node4.set_field_aggregations(field, [:sum, :avg])

    field = pipe3.field_with_name("city")
    node4.group_fields = [field]

    pipe4 = Pipe.new
    node4.output_pipe = pipe4
    node4.setup_output_pipe_table

    table4 = node4.output_pipe.table
    assert_not_equal(table1, table4)
    assert_equal(3, table4.columns.count)
end

end
