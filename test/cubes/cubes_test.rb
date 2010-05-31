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
	create_example_data
	
	@product_dataset = Dataset.dataset_from_database_table(@connection[:ft_product])
end

def test_cube
	cube = Cube.new
	cube.dataset = @product_dataset
	
	values = cube.aggregate(:amount, [:sum])
	assert_equal(160, values[:sum])
	assert_equal(7, values[:record_count])

	values = cube.aggregate(:revenue, [:sum])
	assert_equal(2800, values[:sum])
	assert_equal(7, values[:record_count])

end

def xxtest_date_dimension
	dataset = Dataset.dataset_from_database_table(@connection[:dm_date])
	assert_equal(1096,dataset.count)

	dim = Dimension.new
	dim.hierarchy = [:year, :month, :day]
	dim.dataset = dataset

	years = dim.values_at_path([:year])
	
	assert_equal([2009, 2010, 2011], years)

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

	date = Date.strptime('2009-01-01')
	end_date = Date.strptime('2012-01-01')
	
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

def create_example_data
	if @connection.table_exists?(:ft_product)
		@connection.drop_table(:ft_product)
	end

	@connection.create_table(:ft_product) do
		primary_key :id
		column :date_id, :integer
		column :product, :varchar
		column :revenue, :numeric
		column :amount,  :integer
	end
	
	table = @connection[:ft_product]
	
	values = [
		{:date_id => '20090101', :product => 'foo', :revenue => 100, :amount => 10},
		{:date_id => '20090102', :product => 'bar', :revenue => 200, :amount => 10},
		{:date_id => '20090203', :product => 'foo', :revenue => 300, :amount => 20},
		{:date_id => '20090204', :product => 'bar', :revenue => 400, :amount => 20},
		{:date_id => '20090301', :product => 'foo', :revenue => 500, :amount => 30},
		{:date_id => '20090301', :product => 'bar', :revenue => 600, :amount => 30},
		{:date_id => '20090401', :product => 'foo', :revenue => 700, :amount => 40}
	]
	table.multi_insert(values)
	
end

end
