require 'test/unit'
require 'brewery'


class TestNodes < Test::Unit::TestCase
def setup
    @test_file = 'test/test.csv'

    # Set-up some fields

    @field_id = Field.new("customer_id", :storage_type => :integer, :field_type => :set)
    @field_name = Field.new("name", :storage_type => :string, :field_type => :set)
    @field_surname = Field.new("surname", :storage_type => :string, :field_type => :set)
    @field_city = Field.new("city", :storage_type => :string, :field_type => :set)
    @field_amount = Field.new("amount", :storage_type => :integer, :field_type => :range)

    @fields = [@field_id, @field_name, @field_surname, @field_city, @field_amount]

    @input_count = @fields.count

    @pipe = Pipe.new
    @pipe.fields = @fields

    fields = Array.new
    fields << Field.new("invoice_id", :storage_type => :integer, :field_type => :set)
    fields << Field.new("customer_id", :storage_type => :integer, :field_type => :set)
    fields << Field.new("invoice_date", :storage_type => :date, :field_type => :range)
    fields << Field.new("invoice_sum", :storage_type => :numeric, :field_type => :range)

    @pipe2 = Pipe.new
    @pipe2.fields = fields

    @input2_count = fields.count
end

def test_file_source_node
    node = FileSourceNode.new
    node.filename = @test_file
    node.reads_field_names = false
    node.file_fields = @fields
    node.prepare
    
    fields = node.fields
    assert_equal(@input_count, fields.count, "field count in the file does not match")
    assert_equal(true, node.creates_dataset)

    assert_equal(@input_count, node.created_fields.count, "created fields")
end

def test_derive_node
    node = DeriveNode.new
    node.derived_field_name = "full_name"
    
    assert_equal(0, node.fields.count, "field count in derive node does not match")

    node.add_input_pipe(@pipe)
    assert_equal(@input_count + 1, node.fields.count, "field count in derive node does not match")
    assert_equal(1, node.created_fields.count, "created fields")

    assert_equal("name", node.fields[1].name)
    assert_equal("full_name", node.fields[@input_count].name)

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
    
    node.set_field_name(@field_amount, "salary")
    node.set_field_action(@field_name, :delete)

    assert_equal(@input_count - 1, node.fields.count, "field count after map")
    assert_equal(0, node.created_fields.count, "created fields")

    map = node.field_map

    field = map.field_for_source_field(@field_amount)
    assert_equal("salary", field.name, "field 'amount' should be renamed to 'salary'")

    field = map.field_for_source_field(@field_name)
    assert_equal(nil, field, "field 'name' should be deleted")

    node.reset_field_name(@field_amount)
    node.set_field_action(@field_name, :keep)

    map = node.field_map
    field = map.field_for_source_field(@field_amount)
    assert_equal("amount", field.name, "field 'salary' should be renamed back to 'amount'")

    field = map.field_for_source_field(@field_name)
    assert_not_equal(nil, field, "field 'name' should be kept")
    assert_equal(0, node.created_fields.count, "created fields")
end

def test_merge_node
    node = MergeNode.new
    

    node.add_input_pipe(@pipe)
    node.add_input_pipe(@pipe2)
    assert_equal(@input_count + @input2_count, node.possible_key_fields.count)

    node.key_field_names = ["customer_id"]
    assert_equal(@input_count + @input2_count - 1, node.field_map.count)
    assert_equal(@input_count + @input2_count - 1, node.created_fields.count, "created fields")
    
    node.remove_input_pipe(@pipe)
    assert_equal(@input2_count, node.field_map.count)
    assert_equal(@input2_count, node.created_fields.count, "created fields")

    # puts node.sql_statement
end

def test_select_node
    node = SelectNode.new

    node.add_input_pipe(@pipe)
    
    assert_equal(@input_count, node.fields.count, "total output fields")
    assert_equal(@input_count, node.created_fields.count, "created fields")
end

def test_aggregate_node
    node = AggregateNode.new

    node.add_input_pipe(@pipe)
    node.add_aggregation(:sum, @field_amount, "amount_sum")
    node.add_aggregation(:average, @field_amount, "amount_avg")
    
    assert_equal(2, node.fields.count, "total output fields")
    assert_equal(2, node.created_fields.count, "created fields")
    
    node.group_fields = [@field_city]
    assert_equal(3, node.fields.count, "total output fields")
end
end
