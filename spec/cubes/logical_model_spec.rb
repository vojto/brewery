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

    it "should load model" do
        model = Brewery::LogicalModel.create_model_from_path(@models_path + 'test')
        model.should_not == nil
    end
end
