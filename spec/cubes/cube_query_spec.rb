require 'spec_helper'

describe "CubeQuery" do
    before :all do
        Brewery::Test.initialize_test_data_store

        @models_path = Brewery::Test.models_path
        @model = Brewery::LogicalModel.create_model_from_path(@models_path + 'test')
        @cube = @model.cube_with_name('test')
        
        @day_cut = Brewery::Cut.point_cut('date', [2010, 2, 1])
        @month_cut = Brewery::Cut.point_cut('date', [2010, 2])
        @year_cut = Brewery::Cut.point_cut('date', [2010])
    end
    
    describe "detail expressions" do
        before :each do
            @query = Brewery::CubeQuery.new(@cube, "view")
            @query.view_alias = 'v'
        end
        it "should create single record query" do
            sql = @query.record_sql(1)
            sql.should == 'SELECT * FROM view AS v WHERE v.id = 1'
        end

        it "should create multiple record query without conditions" do
            sql = @query.records_sql
            sql.should == 'SELECT * FROM view AS v'
        end
    
        it "should paginate details" do
            @query.page = 2
            @query.page_size = 100
            sql = @query.records_sql
            sql.should == 'SELECT * FROM view AS v LIMIT 100 OFFSET 200'
        end
    
        it "should filter details by day cut" do
            @query.add_cut(@day_cut)
            sql = @query.records_sql
            sql.should == 'SELECT * FROM view AS v WHERE "date.year" = 2010 AND "date.month" = 2 AND "date.day" = 1'
        end

        it "should filter details by month cut" do
            @query.add_cut(@month_cut)
            sql = @query.records_sql
            sql.should == 'SELECT * FROM view AS v WHERE "date.year" = 2010 AND "date.month" = 2'
        end
    
        it "should order details" do
            @query.order_by = "name"
            @query.order_direction = :asc
            sql = @query.records_sql
            sql.should == 'SELECT * FROM view AS v ORDER BY "sales.name" ASC'

            @query.order_by = "date.month"
            @query.order_direction = :asc
            sql = @query.records_sql
            sql.should == 'SELECT * FROM view AS v ORDER BY "date.month" ASC'
        end
    
        it "should order and paginate details" do
            @query.page = 2
            @query.page_size = 100
            @query.order_by = "date.month"
            @query.order_direction = :asc
            sql = @query.records_sql
            sql.should == 'SELECT * FROM view AS v ORDER BY "date.month" ASC LIMIT 100 OFFSET 200'
        end
    end
    
    describe "aggregation expressions" do
    end
end
