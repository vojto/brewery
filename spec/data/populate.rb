require 'rubygems'
require 'sequel'

connection = Sequel.connect('sqlite:///Users/stefan/Developer/Projects/brewery/spec/data/test.sqlite')

month_names = ["January", "February", "March", "April", "May", "June", 
			   "July", "August", "September", "October", "November", "December"]

month_snames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
			   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

date_dim_table = :dm_date

if connection.table_exists?(date_dim_table)
	connection.drop_table(date_dim_table)
end

connection.create_table(date_dim_table) do
	primary_key :id
	column :year, :integer
	column :month, :integer
	column :month_name, :varchar
	column :month_sname, :varchar
	column :day, :integer
	column :week_day, :integer
end

date_dim = connection[date_dim_table]

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

################################################################
# Create category dimension table

if connection.table_exists?(:dm_category)
	connection.drop_table(:dm_category)
end

connection.create_table(:dm_category) do
	primary_key :id
	column :category_code, :varchar
	column :category, :varchar
end

values = [
	{ :category_code => "unknown", :category => "unknown category" },
	{ :category_code => "new", :category => "New stuff" },
	{ :category_code => "old", :category => "Old stuff" },
]

table = connection[:dm_category]
table.multi_insert(values)

################################################################
# Create fact table

if connection.table_exists?(:ft_sales)
	connection.drop_table(:ft_sales)
end

connection.create_table(:ft_sales) do
	primary_key :id
	column :date_id, :integer
	column :category, :varchar
	column :product, :varchar
	column :revenue, :numeric
	column :amount,  :integer
end

table = connection[:ft_sales]

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

