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
    
    before :each do
        @query = Brewery::CubeQuery.new(@cube, "view")
        @query.view_alias = 'v'
    end

    describe "detail expressions" do
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
        it "should return only count of all records" do
            sql = @query.aggregation_summary_sql
            sql.should == 'SELECT COUNT(1) AS record_count FROM view AS v'
        end

        it "should return count and sum of amount" do
            @query.measure = 'amount'
            sql = @query.aggregation_summary_sql
            sql.should == 'SELECT SUM(amount) AS amount_sum, COUNT(1) AS record_count FROM view AS v'
        end
        
        it "should drill down to year" do
            sql = @query.aggregation_drill_down_sql(:row_dimension => :date, :row_levels => [:year])
            sql.should == 'SELECT COUNT(1) AS record_count, "date.year" FROM view AS v GROUP BY "date.year" ORDER BY "date.year"'
        end

        it "should drill even deeper to month" do
            sql = @query.aggregation_drill_down_sql(:row_dimension => :date, :row_levels => [:year, :month])
            year = '"date.year"'
            month = '"date.month", "date.month_name", "date.month_sname"' 
            fields = "#{year}, #{month}"
            sql.should == "SELECT COUNT(1) AS record_count, #{fields} FROM view AS v GROUP BY #{fields} ORDER BY #{fields}"
        end

        it "should drill down and paginate" do
            @query.page = 2
            @query.page_size = 50
            sql = @query.aggregation_drill_down_sql(:row_dimension => :date, :row_levels => [:year])
            sql.should == 'SELECT COUNT(1) AS record_count, "date.year" FROM view AS v GROUP BY "date.year" ORDER BY "date.year" LIMIT 50 OFFSET 100'
        end
        
        it "should cut and aggregate" do
            @query.add_cut(@month_cut)
            sql = @query.aggregation_summary_sql
            sql.should == 'SELECT COUNT(1) AS record_count FROM view AS v WHERE "date.year" = 2010 AND "date.month" = 2'
        end

        it "should cut and drill down" do
            @query.add_cut(@month_cut)
            sql = @query.aggregation_drill_down_sql(:row_dimension => :date, :row_levels => [:year])
            sql.should == 'SELECT COUNT(1) AS record_count, "date.year" FROM view AS v WHERE "date.year" = 2010 AND "date.month" = 2 GROUP BY "date.year" ORDER BY "date.year"'
        end
    end
end
