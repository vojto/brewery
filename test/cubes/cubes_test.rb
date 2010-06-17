require 'test/unit'
require 'rubygems'
require 'brewery'

class BreweryCubesTest < Test::Unit::TestCase
include Brewery

def setup
    DataMapper.setup(:default, 'sqlite3::memory:')
    DataMapper.auto_migrate!


	manager = Brewery::data_store_manager
	manager.add_data_store(:default, "sqlite::memory:")
	# manager.add_data_store(:default, "postgres://localhost/sandbox")
	@connection = manager.create_connection(:default)

	create_date_dimension
	create_category_dimension
	create_example_data
	define_dimensions
	
	@product_dataset = Dataset.dataset_from_database_table(@connection[:ft_product])
	create_cube
end

def define_dimensions
	dataset = Dataset.dataset_from_database_table(@connection[:dm_date])
    dim = Dimension.new( { :name => :date ,
                           :levels =>  [
                                {:name => :year, :level_fields => [:year] },
                                {:name => :month, :level_fields => [:month, :month_name, :month_sname]},
                                {:name => :day, :level_fields => [:day, :week_day]}
                            ]
                            } 
                        )

    dim.save
    hier = dim.create_hierarchy(:default)
    hier.levels = [:year, :month, :day]
    hier.save
    dim.table = :dm_date
    dim.dataset = dataset
	@date_dimension = dim
	
	dataset = Dataset.dataset_from_database_table(@connection[:dm_category])
    dim = Dimension.new( { :name => :category ,
                           :levels => [ { :name => :category, :level_fields => [:category_code, :category] } ]
                           } )
    dim.key_field = "category_code"
    dim.save
    hier = dim.create_hierarchy(:default)
    hier.levels = [:category]
    hier.save
	dim.dataset = dataset
	dim.table = :dm_category

	@category_dimension = dim

	# Add dimensions to workspace
	ws = Workspace.default_workspace
	ws.add_dimension(:date, @date_dimension)
	ws.add_dimension(:category, @category_dimension)
end

def test_from_hash_and_file
    hash = 
        {
            :name => "date",
            :levels => [
                { :name => :year,  :level_fields => [:year] },
                { :name => :month, :level_fields => [:month, :month_name, :month_sname]},
                { :name => :day, :level_fields => [:day, :week_day, :week_day_name, :week_day_sname]}
            ]
        }
    dim = Dimension.new(hash)
    fields = dim.fields_for_level(:month)
    assert_equal([:month, :month_name, :month_sname], fields)
    # FIXME: TEST this
    # assert_equal([:year, :month, :day], dim.default_hierarchy)
    
    path = Pathname.new("model/date_dim.yml")
    dim = Dimension.new_from_file(path)
    
    l = dim.levels.first( :name => "month" ).level_fields
    
    fields = dim.fields_for_level("month")
    assert_not_nil(fields)
    assert_equal(["month", "month_name", "month_sname"], fields)
    assert_equal(["year", "month", "day"], dim.default_hierarchy.level_names)
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
		{:date_id => 20100101, :category => 'new', :product => 'foo', :revenue => 100, :amount => 10},
		{:date_id => 20100102, :category => 'new', :product => 'bar', :revenue => 200, :amount => 10},
		{:date_id => 20100203, :category => 'old', :product => 'fooz', :revenue => 300, :amount => 20},
		{:date_id => 20100204, :category => 'old', :product => 'barz', :revenue => 400, :amount => 20},
		{:date_id => 20100301, :category => 'old', :product => 'fooz', :revenue => 500, :amount => 30},
		{:date_id => 20100301, :category => 'new', :product => 'bar', :revenue => 600, :amount => 30},
		{:date_id => 20100401, :category => 'new', :product => 'foo', :revenue => 700, :amount => 40}
	]
	table.multi_insert(values)
	
end

def create_cube
	@cube = Cube.new
	@cube.dataset = @product_dataset
	@cube.join_dimension(@date_dimension, :date_id)
	@cube.join_dimension(@category_dimension, :category)
	@cube.fact_table = :ft_product
end

def test_dimension
	dim = @date_dimension

	level = dim.drill_down_level(nil)
	assert_equal("year", level.name)

	level = dim.drill_down_level([2009, 10])
	assert_equal("day", level.name)

	level = dim.drill_down_level([2009, 10, 1])
	assert_equal("day", level.name)
	
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

def test_date_dimension
	dim = @date_dimension

	years = dim.list_of_values([])
	assert_equal([2009, 2010, 2011, 2012], years.collect {|r| r[:year] })

	months2009 = dim.list_of_values([2009]).collect {|r| r[:month] }
	months2010 = dim.list_of_values([2010]).collect {|r| r[:month] }
	months2011 = dim.list_of_values([2010]).collect {|r| r[:month] }
	months2012 = dim.list_of_values([2012]).collect {|r| r[:month] }
	months_any = dim.list_of_values([:all]).collect {|r| r[:month] }
	
	assert_equal([6,7,8,9,10,11,12], months2009)
	assert_equal(12, months2010.count)
	assert_equal(months2010, months2011)
	assert_equal(months2010, months_any)
	assert_equal([1,2,3,4,5,6], months2012)

	months2010 = dim.list_of_values([2010])
	assert_equal("January", months2010[0][:month_name])

 	days_jan10 = dim.list_of_values([2010, 1]).collect {|r| r[:day] }
	assert_equal(31, days_jan10.count)

 	days_jan = dim.list_of_values([:all, 1]).collect {|r| r[:day] }
	assert_equal(31, days_jan.count)

 	all_days = dim.list_of_values([:all, :all]).collect {|r| r[:day] }
	assert_equal(31, all_days.count)
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
    assert_equal(600, result.rows[0][:sum])
    assert_equal(500, result.rows[1][:sum])
end

def test_cuts

	slice = @cube.whole.cut_by_point(:date, [2010])
    result = slice.aggregate(:revenue)
    assert_equal(2800, result.summary[:sum])

    result = slice.aggregate(:revenue, {:row_dimension => :date, 
    								   :row_levels => [:year, :month]})
    assert_equal(300, result.rows[0][:sum])
    								   
	from_key = @date_dimension.key_for_path([2010,1,1])
	assert_equal(20100101, from_key)
	from_key = @date_dimension.key_for_path([2010,3])
	assert_equal(20100301, from_key)
	from_key = @date_dimension.key_for_path([2010])
	assert_equal(20100101, from_key)

	to_key = @date_dimension.key_for_path([2010,2,3])

	slice = @cube.whole.cut_by_range(:date, from_key, to_key)
    result = slice.aggregate(:revenue, {:row_dimension => :date, 
    			                      :row_levels => [:year, :month]})
    assert_equal(300, result.rows[0][:sum])
    assert_equal(2, result.rows.count)

    result = slice.aggregate(:revenue, {:row_dimension => :date, 
    			                      :row_levels => [:year, :month],
    			                      :limit => :rank,
    			                      :limit_value => 1,
    			                      :limit_sort => :top})
    assert_equal(1, result.rows.count)
    assert_equal(300, result.rows[0][:sum])
    assert_equal(300, result.remainder[:sum])
    assert_equal(2, result.remainder[:record_count])
end

def test_detail
	slice = @cube.whole.cut_by_point(:date, [2010])
    assert_equal(7, slice.details.count)
    								   
	from_key = @date_dimension.key_for_path([2010,1,1])
	to_key = @date_dimension.key_for_path([2010,2,3])

	slice = @cube.whole.cut_by_range(:date, from_key, to_key)
    assert_equal(3, slice.details.count)
end

end
