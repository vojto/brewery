module Brewery

class Workspace
@@default_workspace = nil

attr_reader :connection

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
        if !@connection
            raise ArgumentError, "Unable to create connection with name '#{connection}'"
        end
    else
        @connection = connection
    end
end
# FIXME: make this execute_sql and the other one to be execute_select_sql
def execute_sql_no_data(sql_statement)
    @connection << sql_statement
end
def execute_sql(sql_statement)
    # FIXME: add logging and time measurement
    return @connection[sql_statement]
end

end # class Workspace

end # module
