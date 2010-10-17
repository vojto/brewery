module Brewery
module Test
    
def self.initialize_for_test
    @@test_models_path = SPEC_ROOT + 'models'
end

def self.initialize_test_data_store
    sqlite_db_path = SPEC_ROOT + 'data/test.sqlite'
    manager = Brewery::DataStoreManager.new
    manager.add_data_store(:brewery_test, "sqlite::memory:")
    manager.add_data_store(:brewery_sqlite_test, "sqlite://#{sqlite_db_path}")
    Brewery::DataStoreManager.default_manager = manager

    Brewery::set_brewery_datastore(:brewery_test)
    Brewery::initialize_brewery_datastore
    Brewery::create_default_workspace(:brewery_test)
end

def self.initialize_for_cube
    Brewery::Test.initialize_test_data_store
    Brewery::create_default_workspace(:brewery_sqlite_test)
end

def self.test_cube
    models_path = Brewery::Test.models_path
    model = Brewery::LogicalModel.create_model_from_path(models_path + 'test')
    return model.cube_with_name('test')
end

def self.models_path
    return @@test_models_path
end

end # module Test
end # module Brewery
    