require 'brewery/database/do_database'
require 'do_sqlite3'

module Brewery
class SqliteDatabase < DODatabase

@@concrete_storage_types = { 
        :string => "string",
        :text => "text",
        :numeric => "numeric",
        :date => "date",
        :integer => "integer",
        :boolean => "boolean"
    }
    
def initialize(uri)
    uri = uri.dup
    uri.scheme = 'sqlite3'
    super(uri)
    @adapter_name = 'sqlite'
end

def create_table(name, field_list)
    statements = []
    fields = [] 

    statements << "CREATE TABLE #{quote_string(name.to_s)} ("

    field_list.each { |field| 
        field_name = field[0]
        field_type = concrete_storage_type(field[1])
        fields << "#{field_name} #{field_type}"
    }

    statements << fields.join(', ')
    statements << ")"
    sql = statements.join(' ')

    execute_sql_no_data(sql)
end

def concrete_storage_type(type)
    concrete_type = @@concrete_storage_types[type]
    if !concrete_type
        raise "No concrete storage type for type '#{type}' defined in '#{self.class.name}'"
    end
    return concrete_type
end

def storage_type(concrete_type)
    return :unknown
end

def tables
    statement = "SELECT name FROM sqlite_master WHERE type='table'"
    boo = execute_sql(statement).collect { |row|
        row["name"]
    }
    return boo
end

def table_field_names(table_name)
    statement = "PRAGMA table_info(#{quote_string(table_name.to_s)})"
    array = execute_sql(statement).collect { |row|
        row["name"]
    }
    return array
end

end # class Database
end # module Brewery
