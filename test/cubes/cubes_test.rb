require 'test/unit'
require 'rubygems'
require 'brewery'

class BreweryCubesTest < Test::Unit::TestCase
include Brewery

def setup
	manager = Brewery::data_store_manager
	manager.add_data_store(:default, "sqlite::memory:")

	@connection = manager.create_connection(:default)

    Brewery::set_brewery_datastore(:default)
    Brewery::initialize_brewery_datastore
    Brewery::create_default_workspace(@connection)

	create_example_data

    model = Brewery::LogicalModel.model_from_path('model')
    @cube = model.cube_with_name('test')
    @date_dimension = @cube.dimension_with_name('date')
end

def create_example_data

    ################################################################
    # Create date dimension
    
	@date_dim_table = :dm_date
	if @connection.table_exists?(@date_dim_table)
		@connection.drop_table(@date_dim_table)
	end

	@month_names = ["January", "February", "March", "April", "May", "June", 
				   "July", "August", "September", "October", "November", "December"]

	@month_snames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
				   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

	@connection.create_table(@date_dim_table) do
		primary_key :id
		column :year, :integer
		column :month, :integer
		column :month_name, :varchar
		column :month_sname, :varchar
		column :day, :integer
		column :week_day, :integer
	end
	
	date_dim = @connection[@date_dim_table]

	date = Date.strptime('2009-06-01')
	end_date = Date.strptime('2012-06-01')
	
	while date <= end_date do
		record = {
			:id => date.strftime('%Y%m%d'),
			:year => date.year,
			:month => date.month,
			:month_name => @month_names[date.month-1],
			:month_sname => @month_snames[date.month-1],
			:week_day => date.wday,
			:day => date.day
		}
		date_dim.insert(record)
		date = date + 1
	end

    ################################################################
    # Create category dimension table

	@category_dim_table = :dm_category
	if @connection.table_exists?(@category_dim_table)
		@connection.drop_table(@category_dim_table)
	end

	@connection.create_table(@category_dim_table) do
		primary_key :id
		column :category_code, :varchar
		column :category, :varchar
	end

	values = [
		{ :category_code => "unknown", :category => "unknown category" },
		{ :category_code => "new", :category => "New stuff" },
		{ :category_code => "old", :category => "Old stuff" },
	]

	table = @connection[@category_dim_table]
	table.multi_insert(values)

    ################################################################
    # Create fact table

	if @connection.table_exists?(:ft_sales)
		@connection.drop_table(:ft_sales)
	end

	@connection.create_table(:ft_sales) do
		primary_key :id
		column :date_id, :integer
		column :category, :varchar
		column :product, :varchar
		column :revenue, :numeric
		column :amount,  :integer
	end
	
	table = @connection[:ft_sales]
	
	values = [
		{:date_id => 20100101, :category => 'new', :product => 'foo', :revenue => 100, :amount => 10},
		{:date_id => 20100102, :category => 'new', :product => 'bar', :revenue => 200, :amount => 10},
		{:date_id => 20100203, :category => 'old', :product => 'fooz', :revenue => 300, :amount => 20},
		{:date_id => 20100204, :category => 'old', :product => 'barz', :revenue => 400, :amount => 20},
		{:date_id => 20100301, :category => 'old', :product => 'fooz', :revenue => 500, :amount => 30},
		{:date_id => 20100301, :category => 'new', :product => 'bar', :revenue => 600, :amount => 30},
		{:date_id => 20100401, :category => 'new', :product => 'foo', :revenue => 700, :amount => 40}
	]
	table.multi_insert(values)
	
    @cube_dataset = {
        name: 'ft_sales',
        label: 'Sales',
        data_store_name: 'default',
        fields: [
            { name: 'category' },
            { name: 'product' },
            { name: 'revenue' },
            { name: 'amount' }
        ]
    }
end

def test_dimension
	dim = @date_dimension

	level = dim.next_level(nil)
	assert_equal("year", level.name)

	level = dim.next_level([2009, 10])
	assert_equal("day", level.name)

	level = dim.next_level([2009, 10, 1])
	assert_equal(nil, level)
	
	path = dim.roll_up_path([2009, 10])
	assert_equal([2009], path)

	path = dim.roll_up_path(path)
	assert_equal([], path)
	path = dim.roll_up_path(path)
	assert_equal([], path)

	assert_equal([], dim.path_levels([]))
	assert_equal(["year"], dim.path_levels([2009]).collect { |l| l.name })
	assert_equal(["year", "month", "day"], dim.path_levels([2009,10,1]).collect { |l| l.name })

end

def test_dimension_value_list
	dim = @date_dimension

    slice = @cube.whole

	years = slice.dimension_values_at_path(:date, [])
	assert_equal([2010], years.collect {|r| r[:"date.year"] })

	months2009 = slice.dimension_values_at_path(:date, [2009]).collect {|r| r[:"date.month"] }
	months2010 = slice.dimension_values_at_path(:date, [2010]).collect {|r| r[:"date.month"] }
	months2011 = slice.dimension_values_at_path(:date, [2011]).collect {|r| r[:"date.month"] }
	months2012 = slice.dimension_values_at_path(:date, [2012]).collect {|r| r[:"date.month"] }
	months_any = slice.dimension_values_at_path(:date, [:all]).collect {|r| r[:"date.month"] }
	
	assert_equal([], months2009)
	assert_equal([1, 2, 3, 4], months2010)
	assert_equal([], months2011)
	assert_equal([], months2012)

	months2010 = slice.dimension_values_at_path(:date, [2010]).collect {|r| r }
	assert_equal("January", months2010[0][:"date.month_name"])

 	days_jan10 = slice.dimension_values_at_path(:date, [2010, 1]).collect {|r| r[:"date.day"] }
	assert_equal(2, days_jan10.count)

    # Test ordering
    
    # Yeah, ordering months by name useless, but can be used for testing
	values = slice.dimension_values_at_path(:date, [2010], { :order_by => "date.month_name" })
	values = values.collect { |r| r[:"date.month_name"] }
	assert_equal(["April", "February", "January", "March"], values)

    options = { :order_by => "date.month_name", :page => 1, :page_size => 2 }
	values = slice.dimension_values_at_path(:date, [2010], options)
	values = values.collect { |r| r[:"date.month_name"] }
	assert_equal(2, values.count)
	assert_equal(["January", "March"], values)

end

def test_cube

	result = @cube.whole.aggregate(:amount)
	assert_equal([], result.rows)
	assert_not_nil([], result.summary)
	assert_equal(160, result.summary[:sum])
	assert_equal(7, result.summary[:record_count])

	result = @cube.whole.aggregate(:revenue)
	assert_equal(2800, result.summary[:sum])
	assert_equal(7, result.summary[:record_count])

	slice = @cube.whole.cut_by_point(:date, [2010, 3])
	result = slice.aggregate(:revenue)
	assert_equal(1100, result.summary[:sum])
	assert_equal(2, result.summary[:record_count])

	cat_slice = slice.cut_by_point(:category, ['new'])
	result = cat_slice.aggregate(:revenue)
	assert_equal(600, result.summary[:sum])
	assert_equal(1, result.summary[:record_count])

	slice = @cube.whole.cut_by_point(:date, [2010, 3])
    result = slice.aggregate(:revenue, { :row_dimension => :category,
    				                      :row_levels => [:category]} )
	assert_equal(1100, result.summary[:sum])
	assert_equal(2, result.summary[:record_count])

    assert_equal(600, result.rows[0][:revenue_sum])
    assert_equal(500, result.rows[1][:revenue_sum])
end

def test_cuts

	slice = @cube.whole.cut_by_point(:date, [2010])
    result = slice.aggregate(:revenue)
    assert_equal(2800, result.summary[:sum])

    result = slice.aggregate(:revenue, {:row_dimension => :date, 
    								   :row_levels => [:year, :month]})
    assert_equal(300, result.rows[0][:revenue_sum])
    								   
	slice = @cube.whole.cut_by_range(:date, 20100100, 20100203)
    result = slice.aggregate(:revenue, {:row_dimension => :date, 
    			                      :row_levels => [:year, :month]})
    assert_equal(300, result.rows[0][:revenue_sum])
    assert_equal(2, result.rows.count)

    result = slice.aggregate(:revenue, {:row_dimension => :date, 
    			                      :row_levels => [:year, :month],
    			                      :limit => :rank,
    			                      :limit_value => 1,
    			                      :limit_sort => :top})
    assert_equal(1, result.rows.count)
    assert_equal(300, result.rows[0][:revenue_sum])
    assert_equal(300, result.remainder[:sum])
    assert_equal(2, result.remainder[:record_count])

    # Agregate with no data found (non-existant dimension point)
	slice = @cube.whole.cut_by_point(:date, [1980])
    result = slice.aggregate(:revenue)
# FIXME:    assert_equal(0, result.summary[:sum])
end

def test_detail
	slice = @cube.whole.cut_by_point(:date, [2010])
    assert_equal(7, slice.facts.count)
    								   
	from_key = 20100101
	to_key = 20100203

	slice = @cube.whole.cut_by_range(:date, from_key, to_key)
	facts = slice.facts
    assert_equal(3, facts.count, "Slice fact count does not match")
    
    first = facts.first
    record = {
        :id=>1,
        :category=>"new",
        :product=>"foo",
        :revenue=>100,
        :amount=>10,
        :"category.category_code"=>"new",
        :"category.category"=>"New stuff",
        :"date.year"=>2010,
        :"date.month"=>1,
        :"date.month_name"=>"January",
        :"date.month_sname"=>"Jan",
        :"date.day"=>1,
        :"date.week_day"=>5,
        :"date.id"=>20100101
    }
    
    flag = first.keys.detect{ |key| key.class != Symbol }
    assert_equal(nil, flag, "All kets should be symbols")
    assert_equal(first.keys.sort, record.keys.sort, "Returned keys do not match expected keys")
    assert_equal(first, record, "returned record does not match expected record")
end

end
