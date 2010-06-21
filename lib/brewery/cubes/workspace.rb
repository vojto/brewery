module Brewery

class Workspace
@@default_workspace = nil

attr_reader :connection

def fixme_ugly_initialize_cube_example
    Brewery::load_rails_configuration
    @connection = Brewery::data_store_manager.create_connection(:default)
    
    
    @model = Brewery::Model.first(:name => "vvo")
    @cube = @model.cubes.first( :name => "vvo" )
    table = @connection[@cube.fact_table.to_sym]
    @cube.dataset = Brewery::Dataset.dataset_from_database_table(table)
end

def fixme_nice_initialize_cube_example
    @workspace = Brewery::workspace_with_data_store(:default)

    @connection = Brewery::data_store_manager.create_connection(:default)
    
    @model = Brewery::Model.mode_with_name("vvo")
    @cube = @model.cube_with_name("vvo")
end


def self.default_workspace
    if !@@default_workspace
        @@default_workspace = self.new
    end
    return @@default_workspace
end

def set_default
    @@default_workspace = self
end

# Initializes new workspace with a connection which can be either a name or
# already established connection. If name is provided, connection is created
# using default DataStoreManager
# @see DataStoreManager#create_connection
def initialize(connection)
    if connection.class == String || connection.class == Symbol
        @connection = DataStoreManager.default_manager.create_connection(connection)
    else
        @connection = connection
    end
end

def execute_sql(sql_statement)
    # FIXME: add logging and time measurement
    return @connection[sql_statement]
end

end # class Workspace

end # module
