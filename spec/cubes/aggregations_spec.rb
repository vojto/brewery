require 'spec_helper'

describe "Aggregations" do
    before :all do
        Brewery::Test.initialize_test_data_store
        Brewery::create_default_workspace(:brewery_sqlite_test)

        @models_path = Brewery::Test.models_path
        @model = Brewery::LogicalModel.create_model_from_path(@models_path + 'test')
        @cube = @model.cube_with_name('test')
        
        @day_cut = Brewery::Cut.point_cut('date', [2010, 2, 1])
        @month_cut = Brewery::Cut.point_cut('date', [2010, 2])
        @year_cut = Brewery::Cut.point_cut('date', [2010])
        @year_path = [2010]
        @month_path = [2010, 1]

        builder = Brewery::CubeViewBuilder.new(@cube)
        @view_name = "ft_sales_view"
        builder.create_materialized_view(@view_name)
    end
    
    before :each do
        @query = Brewery::CubeQuery.new(@cube, @view_name)
        @query.view_alias = 'v'
    end

    it "should get detail" do
        record = @query.record(1)
        record[:category].should == 'new'

        record = @query.record(5)
        record[:category].should == 'old'
        record[:product].should == 'fooz'
    end
    
    describe "simple amount aggregation" do
        before :all do
            @query = Brewery::CubeQuery.new(@cube, @view_name)
            @query.view_alias = 'v'
            @query.measure = :amount
            @query.create_aggregation_statements
            @summary = @query.aggregation_summary
        end
        
    	it "should return valid summary with proper fields" do
        	@summary.should_not == nil
        	@summary.keys.sort.should == [:amount_sum, :record_count]
        end

    	it "should return valid summary with valid values" do
        	@summary[:amount_sum].should == 160
        	@summary[:record_count].should == 7
        end
    end
end
