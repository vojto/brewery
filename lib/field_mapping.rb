class FieldMapping
attr_reader :source_field, :target_field, :source_object
def initialize(source_object, source_field, target_field)
    @source_object = source_object
    @source_field = source_field
    @target_field = target_field
end

def self.new_identity(source_object, source_field)
    target_field = source_field.clone
    return self.new(source_object, source_field, target_field)
end

end
