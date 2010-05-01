require 'node'
require 'field_set'

class AggregateNode < Node

attr_accessor :extensions_as_prefix
attr_accessor :name_extension
attr_accessor :count_field_name
attr_reader   :include_count

def initialize(hash = {})
    super(hash)
    @field_aggs = Hash.new
    @group_fields = Array.new
end

################################################################
# Node properties

def set_field_aggregations(field_name, aggregations)
    # FIXME: check input field type to be numeric
    
    if not field_name
        raise ArgumentError, "Nil field name given for aggregation"
    end
    
    @field_aggs[field_name] = aggregations

	fields_changed
end

def remove_aggregation(field_name)
    @field_aggs.delete[field_name]
	fields_changed
end

def include_count= (flag)
    if flag != @include_count
        @include_count = flag
    end
	fields_changed
end

def group_fields= (fields)
    @group_fields = fields
	fields_changed
end

################################################################
# Node specification

def creates_dataset
    return true
end

def created_fields
    return fields
end

def update_fields
	@fields = FieldSet.new
	
	# Group fields 
	input_fields = all_input_fields
    @group_fields.each { |field_name|
	
		input_field = input_fields.field_with_name(field_name)
		if input_field
			field = input_field.clone
			field.name = field_name
		else
			field = Field.new(field_name, :storage_type => :unknown,
									:field_type => :unknown)
		end
		@fields << field
    }

	# Aggregations 
	
    ext = @name_extension

    @field_aggs.each_key {|field_name|
        aggs = @field_aggs[field_name]
		
        aggs.each { |agg|
            if @extension_as_prefix
                ext = "#{ext}_" if ext
                name = "#{agg}_#{ext}#{field_name}"
            else
                ext = "_#{ext}" if ext
                name = "#{field_name}#{ext}_#{agg}"
            end

            field = Field.new(name, :storage_type => :float,
                                    :field_type => :range)
			@fields << field
        }
    }

	# Count field
	
    if include_count
        if count_field_name
            name = count_field_name
        else
            name = "record_count"
        end
        
        field = Field.new(name, :storage_type => :integer,
                                :field_type => :range)
        @fields << field
    end
end

end
