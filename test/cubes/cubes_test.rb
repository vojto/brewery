require 'test/unit'
require 'rubygems'
require 'brewery'

class BreweryCubesTest < Test::Unit::TestCase
include Brewery

def setup
	manager = Brewery::data_store_manager
	manager.add_data_store(:default, "sqlite::memory:")
	# manager.add_data_store(:default, "postgres://localhost/sandbox")
	@connection = manager.create_connection(:default)

	create_date_dimension
	create_category_dimension
	create_example_data
	define_dimensions
	
	
	@product_dataset = Dataset.dataset_from_database_table(@connection[:ft_product])
end

def define_dimensions
	dataset = Dataset.dataset_from_database_table(@connection[:dm_date])
	dim = Dimension.new
	dim.dataset = dataset
	dim.hierarchy = [:year, :month, :day]

	dim.levels = { :year => [:year],
		           :month => [:month, :month_name, :month_sname],
			       :day => [:day] }
	@date_dimension = dim
	
	dataset = Dataset.dataset_from_database_table(@connection[:dm_category])
	dim = Dimension.new
	dim.dataset = dataset
	dim.hierarchy = [:category]

	dim.levels = { :category => [:category_code, :category] }
	@category_dimension = dim
end

def create_date_dimension

	@date_dim_table = :dm_date
	if @connection.table_exists?(@date_dim_table)
		@connection.drop_table(@date_dim_table)
	end

	month_names = ["January", "February", "March", "April", "May", "June", 
				   "July", "August", "September", "October", "November", "December"]

	month_snames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
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
			:month_name => month_names[date.month-1],
			:month_sname => month_snames[date.month-1],
			:week_day => date.wday,
			:day => date.day
		}
		date_dim.insert(record)
		date = date + 1
	end
end

def create_category_dimension
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

end

def create_example_data
	if @connection.table_exists?(:ft_product)
		@connection.drop_table(:ft_product)
	end

	@connection.create_table(:ft_product) do
		primary_key :id
		column :date_id, :integer
		column :category, :varchar
		column :product, :varchar
		column :revenue, :numeric
		column :amount,  :integer
	end
	
	table = @connection[:ft_product]
	
	values = [
		{:date_id => '20100101', :category => 'new', :product => 'foo', :revenue => 100, :amount => 10},
		{:date_id => '20100102', :category => 'new', :product => 'bar', :revenue => 200, :amount => 10},
		{:date_id => '20100203', :category => 'old', :product => 'fooz', :revenue => 300, :amount => 20},
		{:date_id => '20100204', :category => 'old', :product => 'barz', :revenue => 400, :amount => 20},
		{:date_id => '20100301', :category => 'old', :product => 'fooz', :revenue => 500, :amount => 30},
		{:date_id => '20100301', :category => 'new', :product => 'bar', :revenue => 600, :amount => 30},
		{:date_id => '20100401', :category => 'new', :product => 'foo', :revenue => 700, :amount => 40}
	]
	table.multi_insert(values)
	
end
def test_dimension
	dim = @date_dimension

	level = dim.drill_down_level(nil)
	assert_equal(:year, level)

	level = dim.drill_down_level([2009, 10])
	assert_equal(:day, level)

	level = dim.drill_down_level([2009, 10, 1])
	assert_equal(:day, level)
	
	path = dim.roll_up_path([2009, 10])
	assert_equal([2009], path)

	path = dim.roll_up_path(path)
	assert_equal([], path)
	path = dim.roll_up_path(path)
	assert_equal([], path)

	assert_equal([], dim.path_levels([]))
	assert_equal([:year], dim.path_levels([2009]))
	assert_equal([:year, :month, :day], dim.path_levels([2009,10,1]))

end

def test_date_dimension
	dim = @date_dimension

	years = dim.drill_down_values([])
	assert_equal([2009, 2010, 2011, 2012], years.collect {|r| r[:year] })

	months2009 = dim.drill_down_values([2009]).collect {|r| r[:month] }
	months2010 = dim.drill_down_values([2010]).collect {|r| r[:month] }
	months2011 = dim.drill_down_values([2010]).collect {|r| r[:month] }
	months2012 = dim.drill_down_values([2012]).collect {|r| r[:month] }
	months_any = dim.drill_down_values([:all]).collect {|r| r[:month] }
	
	assert_equal([6,7,8,9,10,11,12], months2009)
	assert_equal(12, months2010.count)
	assert_equal(months2010, months2011)
	assert_equal(months2010, months_any)
	assert_equal([1,2,3,4,5,6], months2012)

	months2010 = dim.drill_down_values([2010])
	assert_equal("January", months2010[0][:month_name])

 	days_jan10 = dim.drill_down_values([2010, 1]).collect {|r| r[:day] }
	assert_equal(31, days_jan10.count)

 	days_jan = dim.drill_down_values([:all, 1]).collect {|r| r[:day] }
	assert_equal(31, days_jan.count)

 	all_days = dim.drill_down_values([:all, :all]).collect {|r| r[:day] }
	assert_equal(31, all_days.count)
end

def _test_cube
	cube = Cube.new
	cube.dataset = @product_dataset
	cube.join_dimension(:date, @date_dimension, :date_id, :id)
	cube.join_dimension(:category, @category_dimension, :category, :category_code)

	
	values = cube.aggregate(:amount)
	assert_equal(160, values[:sum])
	assert_equal(7, values[:record_count])

	values = cube.aggregate(:revenue)
	assert_equal(2800, values[:sum])
	assert_equal(7, values[:record_count])

	slice = cube.slice(:date, [2010, 3])
	values = slice.aggregate(:revenue)
	assert_equal(1100, values[:sum])
	assert_equal(2, values[:record_count])

	cat_slice = slice.slice(:category, ['new'])
	values = cat_slice.aggregate(:revenue)
	assert_equal(600, values[:sum])
	assert_equal(1, values[:record_count])

    results = slice.drill_down_aggregate(:category, :category, :revenue, [:sum])
    assert_equal(600, results[0][:sum])
    assert_equal(500, results[1][:sum])
    
end

def test_cuts
	ws = Workspace.default_workspace
	ws.add_dimension(:date, @date_dimension)
	ws.add_dimension(:category, @category_dimension)

	cube = Cube.new
	cube.dataset = @product_dataset
	cube.join_dimension(:date, :date_id, :id)
	cube.join_dimension(:category, :category, :category_code)

	slice = cube.whole.cut_by_point(:date, [2010])
    slice.aggregate_new_sql(:revenue)
    slice.aggregate_new_sql(:revenue, {:row_dimension => :date, 
    								   :row_levels => [:year, :month]})
    								   
	from_key = @date_dimension.key_for_path([2010,1,1])
	assert_equal(20100101, from_key)
	from_key = @date_dimension.key_for_path([2010,3])
	assert_equal(20100301, from_key)
	from_key = @date_dimension.key_for_path([2010])
	assert_equal(20100101, from_key)

	to_key = @date_dimension.key_for_path([2010,1,20])

	slice = cube.whole.cut_by_range(:date, from_key, to_key)
    slice.aggregate_new_sql(:revenue, {:row_dimension => :date, 
    								   :row_levels => [:year, :month]})
end

end
