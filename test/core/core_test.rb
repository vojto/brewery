require 'test/unit'
require 'rubygems'
require 'brewery'

class BreweryCoreTest < Test::Unit::TestCase
include Brewery

def setup
end
def test_tree
	tree = HierarchyTree.new
	
	# --+- A +-- B +--C
    #   |    |     |
	#   |    |     +--D
    #   |    |     
	#   |    +---E----F
    #   |    |
	#   |    +---G----H----I
    #   |
	#   +- J --- K
    #   |
	#   +- L

	assert_equal(0, tree.depth)
	
	tree.add_path(['a', 'b', 'c'])
	tree.add_path(['a', 'b', 'd'])
	tree.add_path(['a', 'e', 'f'])
	tree.add_path(['a', 'g', 'h', 'i'])
	tree.add_path(['j', 'k'])
	tree.add_path(['l'])

	assert_equal(4, tree.depth)
	
 	assert_equal(3, tree.paths_at_level(0).count)
	assert_equal(4, tree.paths_at_level(1).count)
	assert_equal(4, tree.paths_at_level(2).count)
	assert_equal(1, tree.paths_at_level(3).count)
	assert_equal(0, tree.paths_at_level(4).count)

	tree.add_path(['a', 'e', 'f'])
 	assert_equal(3, tree.paths_at_level(0).count)
	assert_equal(4, tree.paths_at_level(1).count)

	assert_equal(nil, tree.object_at_path(['a', 'e', 'f']))
	tree.set_object_at_path(['a', 'e', 'f'], "foo")
	assert_equal("foo", tree.object_at_path(['a', 'e', 'f']))
	assert_equal(nil, tree.object_at_path(['a', 'e', 'f', 'x']))
	
	count = 0
	tree.each(true) { |path, represented_object|
		count += 1
	}
	assert_equal(12, count)
end

def test_data_sources
	manager = Brewery::data_source_manager
	manager2 = Brewery::DataSourceManager::default_manager
	
	assert_equal(manager, manager2)
	
	src = manager.source(:default)
	assert_equal(nil, src)
	manager.add_source( :default, { :adapter => :sqlite3 } )
	
	src = manager.source(:default)
	assert_not_nil(src)
	
	manager.add_sources_in_file('data_sources1.yml')
	src = manager.source(:shop)
	assert_not_nil(src)

	src = manager.source("shop")
	assert_not_nil(src)

	manager.add_source( :shop, { :adapter => :sqlite3 } )
	src = manager.source("shop")
	assert_equal(:sqlite3, src[:adapter])
	
	src = manager.source(:my_project)
	assert_not_nil(src)
	
end

def test_config
	Brewery::load_default_configuration
	assert_equal(1,1)
end

end
