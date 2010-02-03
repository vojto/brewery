require 'test/unit'
require 'brewery'


class TestNodes < Test::Unit::TestCase
def setup
    @test_file = 'test/test.csv'

    # Set-up some fields
    @fields = Array.new
    @fields << Field.new("name", :storage_type => :string)
    @fields << Field.new("surname", :storage_type => :string)
    @fields << Field.new("amount", :storage_type => :integer)

    @pipe = Pipe.new
    @pipe.fields = @fields

    @pipe2 = Pipe.new
    @pipe2.fields = @fields
end

def test_file_input_node
    node = FileSourceNode.new
    node.filename = @test_file
    node.reads_field_names = false
    node.file_fields = @fields
    node.prepare
    
    fields = node.fields
    assert_equal(3, fields.count, "field count in the file does not match")
    assert_equal(true, node.creates_dataset)
    assert_nil(node.field_map)
end

def test_derive_node
    node = DeriveNode.new
    node.derived_field_name = "full_name"
    
    assert_equal(1, node.fields.count, "field count in derive node does not match")

    node.add_input_pipe(@pipe)
    assert_equal(4, node.fields.count, "field count in derive node does not match")

    assert_equal("name", node.fields[0].name)
    assert_equal("full_name", node.fields[3].name)

    assert_raises ArgumentError do
        node.add_input_pipe(@pipe2)
    end
end

def test_field_map_node
    node = FieldMapNode.new
    assert_equal(nil, node.field_map)

    node.add_input_pipe(@pipe)
    map = node.field_map
    assert_not_equal(nil, map)
    
    # Check whether the map is identity map
    flag = map.detect { |assoc| assoc[0] != assoc[1] }
    assert_not_equal(false, flag)
    
    node.set_field_name("amount", "salary")
    node.set_field_action("name", :delete)

    assert_equal(2, node.fields.count, "field count after map should be 2")

    map = node.field_map

    assoc = map.select { |assoc| assoc[0].name == "amount" }.first
    assert_equal("salary", assoc[1].name, "field 'amount' should be renamed to 'salary'")

    assoc = map.select { |assoc| assoc[0].name == "name" }.first
    assert_equal(nil, assoc[1], "field 'name' should be deleted")

    node.reset_field_name("amount")
    node.set_field_action("name", :keep)

    map = node.field_map
    assoc = map.select { |assoc| assoc[0].name == "amount" }.first
    assert_equal("amount", assoc[1].name, "field 'salary' should be renamed back to 'amount'")

    assoc = map.select { |assoc| assoc[0].name == "name" }.first
    assert_not_equal(nil, assoc[1], "field 'name' should be kept")
end

end
