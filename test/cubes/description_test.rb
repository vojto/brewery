require 'test/unit'
require 'rubygems'
require 'brewery'

class BreweryCubeDescriptionTest < Test::Unit::TestCase
include Brewery

def setup
#	manager = Brewery::data_store_manager
#	manager.add_data_store(:default, "sqlite::memory:")

#    Brewery::set_brewery_datastore(:default)
#    Brewery::initialize_brewery_datastore
#    Brewery::create_default_workspace(@connection)

    Brewery::load_default_configuration
    Brewery::set_brewery_datastore('stefan')

    Brewery::initialize_brewery_datastore

    @cube_dataset = {
        name: 'a_cube',
        label: 'This is a cube',
        description: 'This is long description of a cube dataset',
        data_store_name: 'somestore',
        fields: [
            { name: 'moo' },
            { name: 'boo' }
        ]
    }

end

def test_description
    desc = Brewery::DatasetDescription.new_from_hash(@cube_dataset)
    assert_not_nil(desc)
    assert_equal("a_cube", desc.name)
    assert_equal(2, desc.field_descriptions.count)
    # ds.save

    model = Model.new
    model.dataset_descriptions << desc
    model.save
    
    assert_equal(1, model.dataset_descriptions.count)
    stored_desc = model.dataset_description_with_name('a_cube')
    assert_equal(desc.description, stored_desc.description)
    
	cube = Cube.new
    assert_not_nil(cube)
    cube.dataset_description = desc
end



end
