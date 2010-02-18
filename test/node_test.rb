require 'test/unit'
require 'brewery'


class TestNodes < Test::Unit::TestCase
def setup
    @test_file = 'data/customers.csv'

    # Set-up some fields
    @field_id = Field.new("customer_id", :storage_type => :integer, :field_type => :set)
    @field_name = Field.new("name", :storage_type => :string, :field_type => :set)
    @field_surname = Field.new("surname", :storage_type => :string, :field_type => :set)
    @field_city = Field.new("city", :storage_type => :string, :field_type => :set)
    @field_amount = Field.new("amount", :storage_type => :integer, :field_type => :range)

	array = [@field_id, @field_name, @field_surname, @field_city, @field_amount]
    @fields = FieldSet.new(array)

    @input_count = @fields.count
    @file_input_count = 4
    
    @pipe = Pipe.new
    @pipe.fields = @fields

    fields = FieldSet.new
    fields << Field.new("invoice_id", :storage_type => :integer, :field_type => :set)
    fields << Field.new("customer_id", :storage_type => :integer, :field_type => :set)
    fields << Field.new("invoice_date", :storage_type => :date, :field_type => :range)
    fields << Field.new("invoice_sum", :storage_type => :numeric, :field_type => :range)

    @pipe2 = Pipe.new
    @pipe2.fields = fields

    @input2_count = fields.count
end

def test_aggregate_node
    node = AggregateNode.new

	# Configure node
    assert_raises ArgumentError do
        node.set_field_aggregations(nil, [:sum, :avg])
    end

	node.set_field_aggregations("amount", [:sum, :avg])

    assert_equal(2, node.fields.count, "total output fields")
    assert_equal(2, node.created_fields.count, "created fields")

    node.group_fields = ["city"]
    assert_equal(3, node.fields.count, "total output fields with group by")
    

    node.include_count = true
    assert_equal(4, node.fields.count, "total output fields with count")

	field = node.fields.field_with_name("city")
	assert_equal(:unknown, field.storage_type)
	assert_equal(:unknown, field.field_type)

    node.add_input_pipe(@pipe)

	field = node.fields.field_with_name("city")
	assert_equal(:string, field.storage_type)
	assert_equal(:set, field.field_type)
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

    node.reads_field_names = true
    node.field_separator = ';'
    node.prepare
    
    fields = node.fields
    assert_equal(@file_input_count, fields.count, "field count in the file does not match")
    assert_equal(@file_input_count, node.created_fields.count, "created fields")
end

def test_derive_node
    node = DeriveNode.new
    node.derived_field_name = "full_name"
    
    assert_equal(1, node.fields.count, "field count in derive node does not match")

    node.add_input_pipe(@pipe)
    assert_equal(@input_count + 1, node.fields.count, "field count in derive node does not match")
    assert_equal(1, node.created_fields.count, "created fields")

    assert_equal("name", node.fields[1].name)
    assert_equal("full_name", node.fields[@input_count].name)

    assert_raises ArgumentError do
        node.add_input_pipe(@pipe2)
    end
end

def test_field_filter_node
    node = FieldFilterNode.new
	filter = node.field_filter
	
    assert_equal(nil, node.created_fields)
    assert_equal(nil, node.fields)

    node.add_input_pipe(@pipe)
	node.instantiate_fields
	
    assert_not_equal(nil, node.fields)
    assert_equal(@input_count, node.fields.count, "field count after connection")

	fields = node.fields
    assert_not_equal(nil, fields.field_with_name("amount"))
    assert_equal(nil, fields.field_with_name("salary"))
    assert_not_equal(nil, fields.field_with_name("name"))
    
	# create filter

    filter.set_field_name("amount", "salary")
    filter.set_field_action("name", :delete)
    assert_equal(nil, node.created_fields, "created fields")

    assert_equal(@input_count - 1, node.fields.count, "field count after map")

	fields = node.fields
    assert_equal(nil, fields.field_with_name("amount"))
    assert_not_equal(nil, fields.field_with_name("salary"))
    assert_equal(nil, fields.field_with_name("name"))

	# Disconnect and reconnect
    node.remove_input_pipe(@pipe)
	fields = node.fields
    assert_equal(nil, fields.field_with_name("amount"))
    assert_not_equal(nil, fields.field_with_name("salary"))
    assert_equal(nil, fields.field_with_name("name"))

    node.add_input_pipe(@pipe)

	# Reset fields
    filter.reset_field_name("amount")
    filter.set_field_action("name", :keep)

	fields = node.fields
    assert_not_equal(nil, fields.field_with_name("amount"))
    assert_equal(nil, fields.field_with_name("salary"))
    assert_not_equal(nil, fields.field_with_name("name"))
end

def test_merge_node
    node = MergeNode.new
    
    node.add_input_pipe(@pipe)
	node.set_tag_for_input(@pipe, "a")
	assert_not_equal(nil, node.tag_for_input(@pipe))
	assert_equal(nil, node.tag_for_input(@pipe2))
	node.set_tag_for_input(@pipe2, "b")
	
    node.add_input_pipe(@pipe2)
	
	possible_keys = node.possible_keys
    assert_equal(1, possible_keys.count)

	assert_equal("customer_id", possible_keys[0])
	
    node.key_field_names = ["customer_id"]
    assert_equal(@input_count + @input2_count - 1, node.fields.count)
    assert_equal(@input_count + @input2_count - 1, node.created_fields.count, "created fields")
    
    node.remove_input_pipe(@pipe)
    assert_equal(@input2_count, node.fields.count)
    assert_equal(@input2_count, node.created_fields.count, "created fields")

    # puts node.sql_statement
end

def test_select_node
    node = SelectNode.new

    assert_equal(0, node.fields.count, "total output fields")
    assert_equal(0, node.created_fields.count, "created fields")

    node.add_input_pipe(@pipe)
    
    assert_equal(@input_count, node.fields.count, "total output fields")
    assert_equal(@input_count, node.created_fields.count, "created fields")

    node.remove_input_pipe(@pipe)

    assert_equal(0, node.fields.count, "total output fields")
    assert_equal(0, node.created_fields.count, "created fields")
end

end
