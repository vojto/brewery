require 'spec_helper'

describe "Brewery" do
    it "should create default data store manager" do
        manager = Brewery::DataStoreManager.new
        manager.add_data_store(:brewery_test, "sqlite::memory:")
        Brewery::DataStoreManager.default_manager = manager
        Brewery.data_store_manager.should == manager

        store = manager.data_store(:brewery_test)
        store.should_not == nil
        store.should == "sqlite::memory:"
    end

    it "should initialize" do
        manager = Brewery::DataStoreManager.new
        manager.add_data_store(:brewery_test, "sqlite::memory:")
        Brewery::DataStoreManager.default_manager = manager

        Brewery::set_brewery_datastore(:brewery_test)
        Brewery::initialize_brewery_datastore

        Brewery::brewery_datastore_initialized?.should == true
    end
end
