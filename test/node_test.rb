require 'test/unit'
require 'brewery'


class TestNodes < Test::Unit::TestCase
def setup
    @test_file = 'test/test.csv'

    # Set-up some fields

    @field_id = Field.new("customer_id", :storage_type => :integer, :field_type => :set)
    @field_name = Field.new("name", :storage_type => :string, :field_type => :set)
    @field_surname = Field.new("surname", :storage_type => :string, :field_type => :set)
    @field_amount = Field.new("amount", :storage_type => :integer, :field_type => :range)

    @fields = [@field_id, @field_name, @field_surname, @field_amount]

    @pipe = Pipe.new
    @pipe.fields = @fields

    fields = Array.new
    fields << Field.new("invoice_id", :storage_type => :integer, :field_type => :set)
    fields << Field.new("customer_id", :storage_type => :integer, :field_type => :set)
    fields << Field.new("invoice_date", :storage_type => :date, :field_type => :range)
    fields << Field.new("invoice_sum", :storage_type => :numeric, :field_type => :range)

    @pipe2 = Pipe.new
    @pipe2.fields = fields
end

def test_file_input_node
    node = FileSourceNode.new
    node.filename = @test_file
    node.reads_field_names = false
    node.file_fields = @fields
    node.prepare
    
    fields = node.fields
    assert_equal(4, fields.count, "field count in the file does not match")
    assert_equal(true, node.creates_dataset)
    assert_nil(node.field_map)
end

def test_derive_node
    node = DeriveNode.new
    node.derived_field_name = "full_name"
    
    assert_equal(1, node.fields.count, "field count in derive node does not match")

    node.add_input_pipe(@pipe)
    assert_equal(5, node.fields.count, "field count in derive node does not match")

    assert_equal("name", node.fields[1].name)
    assert_equal("full_name", node.fields[4].name)

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
    assert_not_equal(false, map.is_identity_map)
    
    node.set_field_name("amount", "salary")
    node.set_field_action("name", :delete)

    assert_equal(3, node.fields.count, "field count after map")

    map = node.field_map

    field = map.field_for_input_field(@field_amount)
    assert_equal("salary", field.name, "field 'amount' should be renamed to 'salary'")

    field = map.field_for_input_field(@field_name)
    assert_equal(nil, field, "field 'name' should be deleted")

    node.reset_field_name("amount")
    node.set_field_action("name", :keep)

    map = node.field_map
    field = map.field_for_input_field(@field_amount)
    assert_equal("amount", field.name, "field 'salary' should be renamed back to 'amount'")

    field = map.field_for_input_field(@field_name)
    assert_not_equal(nil, field, "field 'name' should be kept")
end

def test_join_node
    node = MergeNode.new
    

    node.add_input_pipe(@pipe)
    node.add_input_pipe(@pipe2)
    
    # assert_raise ArgumentError do
    #     node.field_map
    # end
    assert_equal(8, node.possible_key_fields.count)

    node.key_field_names = ["customer_id"]
    
    assert_equal(7, node.field_map.count)
    
    # puts node.sql_statement
end

end
