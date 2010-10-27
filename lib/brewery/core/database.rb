module Brewery
    
# Represents relational database
# Will be abstracted later
# Main funcionality
# * execute SQL without returning a dataset
# * execute SQL with returning an enumerable dataset
# * create table from a field set
# * database table reflection
# Note that creating tables programatically like in Rails will never be implemented here, as it is 
# for different purpose.
class Database

attr_reader :adapter_name
attr_reader :uri

def self.database_with_uri(uri, options = {})
    uri = DataObjects::URI.parse(uri)
    adapter_name = uri.scheme
    
    require "brewery/database/#{adapter_name}_database"

    obj = ::Brewery
    class_name = "#{adapter_name.capitalize}Database"

    if obj.const_defined?(class_name)
        db_class = obj.const_get(class_name)
    else
        raise ArgumentError, "Database adapter '#{adapter_name}' was not found"
    end
    
    instance = db_class.new(uri)

    if options[:connect] == true
        instance.connect
    end
    
    return instance
end

def initialize(uri)
    @uri = uri
    @adapter_name = uri.scheme
end

def connect
    raise RuntimeError, "Subclasses should override method 'connect'"
end

def disconnect
    raise RuntimeError, "Subclasses should override method 'disconnect'"
end

def execute_sql_no_data(sql_statement)
    raise RuntimeError, "Subclasses should override method 'execute_sql_no_data'"
end
def execute_sql(sql_statement)
    # FIXME: add logging and time measurement
    raise RuntimeError, "Subclasses should override method 'execute_sql'"
end

end # class Database
end # module Brewery