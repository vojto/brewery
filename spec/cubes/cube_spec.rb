require 'spec_helper'

describe "Aggregations" do
    before :all do
        Brewery::Test.initialize_for_cube
        @cube = Brewery::Test.test_cube
        builder = Brewery::CubeViewBuilder.new(@cube)
        @view_name = "ft_sales_view"
        builder.create_materialized_view(@view_name)

        @cube.view = @view_name
    end

    describe "aggregation basics" do
        it "should aggregate amount" do
        	result = @cube.whole.aggregate(:amount)
        	result.summary[:sum].should == 160
        	result.summary[:record_count].should == 7
        end

        it "should aggregate revenue" do
        	result = @cube.whole.aggregate(:revenue)
        	result.summary[:sum].should == 2800
        	result.summary[:record_count].should == 7
        end
    
        it "should aggregate by date point" do
        	slice = @cube.whole.cut_by_point(:date, [2010, 3])
        	result = slice.aggregate(:revenue)
        	result.summary[:sum].should == 1100
        	result.summary[:record_count].should == 2
        end
    
        it "should aggregate by additional category point" do
        	slice = @cube.whole.cut_by_point(:date, [2010, 3])
        	slice = slice.cut_by_point(:category, ['new'])
        	result = slice.aggregate(:revenue)
        	result.summary[:sum].should == 600
        	result.summary[:record_count].should == 1
        end
    
        it "should aggregate by date drill-down" do
        	slice = @cube.whole.cut_by_point(:date, [2010, 3])
            result = slice.aggregate(:revenue, { :row_dimension => :category,
            				                      :row_levels => [:category]} )
          	result.summary[:sum].should == 1100
          	result.summary[:record_count].should == 2

            result.rows[0][:revenue_sum].should == 600
            result.rows[1][:revenue_sum].should == 500
        end
    end
    
    describe "aggregating cuts" do
        it "should cut by date" do
        	slice = @cube.whole.cut_by_point(:date, [2010])
            result = slice.aggregate(:revenue)
            result.summary[:sum].should == 2800

            result = slice.aggregate(:revenue, {:row_dimension => :date, 
            								   :row_levels => [:year, :month]})
            result.rows[0][:revenue_sum].should == 300
        end
        it "should cut by date range" do
        	slice = @cube.whole.cut_by_range(:date, 20100100, 20100203)
            result = slice.aggregate(:revenue, {:row_dimension => :date, 
            			                      :row_levels => [:year, :month]})

            result.rows[0][:revenue_sum].should == 300
            result.rows.count.should == 2

            result = slice.aggregate(:revenue, {:row_dimension => :date, 
            			                      :row_levels => [:year, :month],
            			                      :limit => :rank,
            			                      :limit_value => 1,
            			                      :limit_sort => :top})
            result.rows.count.should == 1
            result.rows[0][:revenue_sum].should == 300 
            result.remainder[:sum].should == 300
            result.remainder[:record_count].should == 2
        end
    end
    
    describe "facts" do
        it "should return facts" do
        	slice = @cube.whole.cut_by_point(:date, [2010])
            slice.facts.count.should == 7
        end
        
        it "should return facts within date range" do
        	from_key = 20100101
        	to_key = 20100203

        	slice = @cube.whole.cut_by_range(:date, from_key, to_key)
        	facts = slice.facts
            facts.count.should == 3
        end

        describe "fact properties" do
            before :each do
            	from_key = 20100101
            	to_key = 20100203

            	slice = @cube.whole.cut_by_range(:date, from_key, to_key)
            	facts = slice.facts

                @first = facts.first
                @record = {
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
            end
            it "should have all keys as symbols" do
                flag = first.keys.detect{ |key| key.class != Symbol }
                flag.should == nil
            end
            it "should return known keys" do
                first.keys.sort.should == record.keys.sort
            end
            it "should return expected record" do
                first == record
            end
            
        end
    end
end