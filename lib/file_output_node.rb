require 'node'
require 'csv'

class FileOutputNode < Node
attr_accessor :filename
attr_accessor :mode # :append, :overwrite
attr_accessor :include_field_names

def evaluate
    # requre: filename
    # optional: mode
    
    if @mode == :append
        write_mode = "a"
    else
        write_mode = "w"
    end
    
    file = File.new(filename,  write_mode)

    if @include_field_names
        fields = input_node.fields
        
        names = fields.collect { |field|
                        field.name
                    }
        line = CSV.generate_line(names)
        file.puts("#{line}\n")
    end

    input_node.each do |record|
        values = record.values
        line = CSV.generate_line(values)
        file.puts("#{line}\n")
    end
end

end
