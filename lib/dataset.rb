class Dataset
attr_accessor :fields
attr_reader :table_name
attr_reader :is_cache
attr_reader :datastore

def initialize(datastore, attributes)
    @datastore = datastore
    @table_name = attributes[:table_name]
    @is_cache = attributes[:is_cache] == 1
end

def table
    return @datastore.connection[@table_name.to_sym]
end

end
