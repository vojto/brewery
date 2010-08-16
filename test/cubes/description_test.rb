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

    model = LogicalModel.new
    model.dataset_descriptions << desc
    model.save
    
    assert_equal(1, model.dataset_descriptions.count)
    stored_desc = model.dataset_description_with_name('a_cube')
    assert_equal(desc.description, stored_desc.description)
end

def test_model_loading
    model = Brewery::LogicalModel.model_from_path('model')
    assert_not_nil(model)

    validation_results = model.validate
    assert_equal(0, validation_results.count)
    
    assert_equal(1, model.cubes.count)    
    assert_equal(2, model.dimensions.count)
    assert_equal(3, model.dataset_descriptions.count)
    
    cube = model.cube_with_name('test')
    assert_not_nil(cube)

    assert_equal("Test cube", cube.label)
    assert_equal(2, cube.dimensions.count)
    assert_equal(2, cube.joins.count)
end


# def test_from_hash_and_file
#     hash = 
#         {
#             :name => "date",
#             :levels => [
#                 { :name => :year,  :level_fields => [:year] },
#                 { :name => :month, :level_fields => [:month, :month_name, :month_sname]},
#                 { :name => :day, :level_fields => [:day, :week_day, :week_day_name, :week_day_sname]}
#             ]
#         }
#     dim = Dimension.new(hash)
#     fields = dim.fields_for_level(:month)
#     assert_equal([:month, :month_name, :month_sname], fields)
#     # FIXME: TEST this
#     # assert_equal([:year, :month, :day], dim.default_hierarchy)
#     
#     path = Pathname.new("model/date_dim.yml")
#     dim = Dimension.new_from_file(path)
#     
#     l = dim.levels.first( :name => "month" ).level_fields
#     
#     fields = dim.fields_for_level("month")
#     assert_not_nil(fields)
#     assert_equal(["month", "month_name", "month_sname"], fields)
#     assert_equal(["year", "month", "day"], dim.default_hierarchy.level_names)
# end
end
