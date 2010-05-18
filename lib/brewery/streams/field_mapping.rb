class FieldMapping
attr_reader :source_object
attr_accessor :source_field, :target_field

def initialize(source_object, source_field, target_field)
    @source_object = source_object
    @source_field = source_field
    @target_field = target_field
end

def self.new_identity(source_object, source_field)
    target_field = source_field.clone
    return self.new(source_object, source_field, target_field)
end

def self.new_created(source_object, target_field)
    return self.new(source_object, nil, target_field)
end

def is_created
    return @source_field == nil
end

end
