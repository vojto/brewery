require 'test/unit'
require 'brewery'


class TestNodes < Test::Unit::TestCase
def setup
    @test_file = 'test/test.csv'
end

def test_file_input
    input = FileSourceNode.new
    input.filename = @test_file
    input.reads_field_names = true
    input.prepare
    
    fields = input.fields
    
    assert_equal(fields.count, 3, "field count in the file does not match")

    rows = 0
    input.each do |record|
        rows = rows + 1
    end

    assert_equal(rows, 4, "record count in the file does not match")
end

def test_file_output
    input = FileSourceNode.new
    input.filename = @test_file
    input.reads_field_names = true
    input.prepare
    
    output = FileOutputNode.new
    output.filename = 'out.csv'
    output.mode = :replace
    output.include_field_names = true
    output.input_node = input

    output.evaluate
end

# FIXME: test unprepared input

def test_aggregation
    input = FileSourceNode.new
    input.filename = @test_file
    input.reads_field_names = true
    input.prepare
    
    agg = AggregationNode.new
    agg.add_aggregation(:amount_sum, :amount, :sum)
    agg.add_aggregation(:amount_avg, :amount, :average)
    agg.input_node = input

    assert_equal(agg.fields.count, 2, "number of aggregated fields does not match")

    agg.evaluate
    
    record = nil
    agg.each { |r|
        record = r
    }
    assert_equal(record[:amount_sum], 160, "sum is not equal")
    assert_equal(record[:amount_avg].to_i, 40, "average is not equal")
end


end
