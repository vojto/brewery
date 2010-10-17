require 'spec_helper'

describe "Dataset Description" do
    before(:all) do
        @test_dataset = {
            name: 'a_dataset',
            label: 'This is a dataset',
            description: 'This is long description of a dataset',
            data_store_name: 'somestore',
            fields: [
                { name: 'moo' },
                { name: 'boo' }
            ]
        }
    end


    it "should be created from hash" do
        desc = Brewery::DatasetDescription.new_from_hash(@test_dataset)
        desc.should_not == nil
        desc.name.should == "a_dataset"
        desc.field_descriptions.count.should == 2
    end
end
