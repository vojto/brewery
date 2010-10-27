require 'data_objects'

module Brewery
class DODatabase < Database
include DataObjects::Quoting

def connect
    @connection = DataObjects::Connection.new(@uri)
end

def disconnect
    @connection.disconnect
end
# FIXME: make this execute_sql and the other one to be execute_select_sql
def execute_sql_no_data(sql_statement)
    command = @connection.create_command(sql_statement)
    return command.execute_non_query
end

def execute_sql(sql_statement)
    # FIXME: add logging and time measurement
    command = @connection.create_command(sql_statement)
    return command.execute_reader
end

end
end