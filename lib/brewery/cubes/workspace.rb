module Brewery

class Workspace
@@default_workspace = nil

attr_accessor :connection

def self.default_workspace
    ws = Thread.current[:brewery_default_workspace]
    if !ws
        raise RuntimeError, "Default workspace not initialized"
    end
    return ws
end

def self.destroy_default_workspace
    ws = Thread.current[:brewery_default_workspace]
    if ws
        ws.close_connection
        Thread.current[:brewery_default_workspace] = nil
    end
end

def set_default
    Thread.current[:brewery_default_workspace] = self
end

# Initializes new workspace with a connection which can be either a name or
# already established connection. If name is provided, connection is created
# using default DataStoreManager
# @see DataStoreManager#create_connection
def initialize(conn_object)
    if conn_object.class == String || conn_object.class == Symbol
        store = DataStoreManager.default_manager.data_store(conn_object)
        if store.class == Hash
            store = store.hash_by_symbolising_keys
        end

        @connection = Sequel.connect(store)
        if !@connection
            raise ArgumentError, "Unable to create connection with name '#{connection}'"
        end

		if store.class == Hash && store[:search_path] && store[:adapter].to_s == 'postgres'
      		@connection.execute("SET search_path TO #{store[:search_path]}") 
		end
    else
        @connection = conn_object
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

def close_connection
    if @connection
        @connection.disconnect
    end
    @connection = nil
end

end # class Workspace

end # module
