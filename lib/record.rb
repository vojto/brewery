require 'node'

class Record
attr_reader :values
attr_reader :fields

def values=(array)
    @values = array
end

def fields=(array)
    @fields = array
    @field_names = Array.new

    array.each { |field|
        if field.kind_of?(Field)
            name = field.name
        else
            name = field
        end
        @field_names << name.to_sym
    }

    # return if not array
end

def [](ref)
    # FIXME: add some checks
    index = @field_names.index(ref.to_sym)

    if index
        return @values[index]
    elsif ref.kind_of?(Numeric)
        return @values[i]
    end

    return nil
end

end
