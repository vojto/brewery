require 'test/unit'
require 'brewery'
require 'rubygems'
require 'sequel'
require 'datastore_table'

class NodeStreamingTest < Test::Unit::TestCase
def setup
    @test_file = 'data/customers.csv'
    @test_output = 'data/output.csv'
end

def test_file_to_file
    stream = Stream.new
    
    ##############################
    # NODE 1: file source
    #

    source_node = FileSourceNode.new
    source_node.filename = @test_file
    source_node.field_separator = ';'
    source_node.reads_field_names = true
    stream.add_node(source_node, "source_node")

    target_node = FileOutputNode.new
    target_node.filename = @test_output
    target_node.field_separator = ';'
    target_node.include_field_names = true
    stream.add_node(target_node, "target_node")
	
	stream.run_node(target_node)
end

def _test_real_stream
	#
	# 1 file --> 2 derive --> 3 filter -+-> ( 5 output )
	#                                   |
	#                                   +-> 4 agregate --> 6 output
	#

    stream = Stream.new
    
    ##############################
    # NODE 1: file source
    #

    node1 = FileSourceNode.new
    node1.filename = @test_file
    node1.field_separator = ';'
    node1.reads_field_names = true
    stream.add_node(node1, "file_input")

    ##############################
    # NODE 2: derive
    #
    node2 = DeriveNode.new
    node2.derived_field_name = "full_name"

    stream.add_node(node2, "derive_name")
    stream.connect_nodes(node1, node2)

    ##############################
    # NODE 3: Filter
    #
    node3 = FieldFilterNode.new
	filter = node3.field_filter

    stream.add_node(node3, "filter")
    stream.connect_nodes(node2, node3)

    filter.set_field_name("id", "customer_id")
    filter.set_field_action("name", :delete)
    filter.set_field_action("sunrname", :delete)
    
    ##############################
    # NODE 4: aggregate
    #
    node4 = AggregateNode.new
   
    node4.set_field_aggregations("amount", [:sum, :avg])

    node4.group_fields = ["city"]
    node4.include_count = true
	
	
    node1.prepare

	storage_type_match("filter", "customer_id", :integer);
	storage_type_match("filter", "full_name", :string);
	storage_type_match("aggregate", "amount_sum", :numeric);
	
	source_match("derive", "customer_id", "file_input");
	source_match("derive", "full_name", "derive");
	source_match("filter", "customer_id", "file_input");
	source_match("aggregate", "city", "file_input");
	source_match("aggregate", "amount_sum", "aggregate");
	
end

end
