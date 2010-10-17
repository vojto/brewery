require 'spec_helper'

describe "LogicalModel" do
    before(:all) do
        Brewery::Test.initialize_test_data_store
        @models_path = Brewery::Test.models_path
    end

    it "should raise exception on non-existant model" do
        @models_path.should_not == nil
        lambda {
            Brewery::LogicalModel.create_model_from_path(@models_path + "unknown")
        }.should raise_error (ArgumentError)
    end
    describe "model" do
        before(:all) do
            @model = Brewery::LogicalModel.create_model_from_path(@models_path + 'test')
        end
        
        it "should load model" do
            @model.should_not == nil
        end

        it "should have datasets" do
            @model.dataset_descriptions.count.should == 3
        end
        
        it "should have cube" do
            cube = @model.cube_with_name('test')
            cube.should_not == nil
            cube.name.should == 'test'
        end
        
        it "should have dimensions" do
            @model.dimensions.count.should == 2

            dims = @model.dimensions.collect { |dim| dim.name }
            dims = dims.sort
            dims.should == ["category", "date"]
        end
    end
end
