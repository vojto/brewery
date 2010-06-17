module Brewery

# Column information for DataTable
class DataTableColumn
attr_accessor :label
attr_accessor :identifier
attr_accessor :format
attr_accessor :parameters
end

# DataTable Cell contents
class DataTableCell
# Object that represents cell content, mostly used for computation
attr_accessor :value

# Displayable representation of cell content
attr_accessor :formatted_value

# Additional parameters. You might use this for store formatting, references or any other kind
# of metadata.
attr_accessor :parameters

def initialize(value = nil, formatted_value = nil, parameters = {})
    @value = value
    @formatted_value = formatted_value
    @parameters = parameters
end
end

# Container for metadata enriched data in tabular form.
# Note: very similar to Google DataTable
# @example
#    # Create table and add some values
#    table = DataTable.new
#    table.add_column(:text, "Company", :company)
#    table.add_column(:currency, "Amount", :amount, {:precision => 0, :currency => '€', :alignment => :right})
#    table.add_column(:percent, "Ratio", :ratio, { :precision => 2 , :alignment => :right} )
#    # ...
#    records.each { | rec |
#        table.add_row([[rec[:company_id], rec[:name]], rec[:sum], rec[:ratio]])
#    }
#    # ...
#    # Retrieve formatted value:
#    amount = table.formatted_value_at(1, 2)
#    ratio = table.formatted_value_at(1, 3)
#    # amount will be: '100 500€’
#    # ratio will be : '5,12%’
class DataTable

# Table rows - arrays of TableCells. Treat contents of this attribute as read-only and limit
# its use for enumeration or counting to maintain internal table consistency.
attr_reader :rows

# Information about table columns - array of TableColumn
attr_reader :columns

def initialize
    @rows = Array.new
    @columns = Array.new
end

# Add a row to the table
# @param [Array] values Array of values for corresponding dimension hierarchy level. Each element of
#   the array might be either a simple object, Hash or an Array. Cell value is either the object value,
#   value for key :value in the Hash or first object in the array, respectively. Additional cell
#   attributes are: hash[:formatted_value] and array[1] for formatted_value, hash[:parameters] and
#   array[2] for cell parameters.
# @example
#   # Assume we have columns: item, count
#   table.add_row(['apples', 10])
#   table.add_row(['banannas', [0, 'none']])
#   table.add_row(['oranges', { :value => 1000, :formatted_value => 'too many'} ] )
def add_row(values = nil)
    row = Array.new(number_of_columns)

    # FIXME: check whether values is type of Array
    if values
        for i in 0..(number_of_columns-1)
            value = values[i]
            if value.class == Hash
                row[i] = DataTableCell.new(value[:value], value[:formatted_value], value[:parameters])
            elsif value.class == Array
                row[i] = DataTableCell.new(value[0], value[1], value[2])
            else
                row[i] = DataTableCell.new(value)
            end
        end
    end
    
    @rows << row
    
    return row
end

# Add table column
# @param [Symbol] format
# @param [String] label - Column title
# @param [Symbol] identifier - column identifier
# @param [Hash] parameters - additional column parameters, can be used for formatting, alignment or
#   any other necessary information
def add_column(format, label = nil, identifier = nil, parameters = {})
    col = DataTableColumn.new
    col.format = format
    col.label = label
    col.identifier = identifier
    col.parameters = parameters
    
    @columns << col
    
    @rows.each { |row|
        row << DataTableCell.new
    }
end

# @param [Integer] row
# @param [Integer] column
# @return cell value at row, column
def value_at(row, column)
    return rows[row][column].value
end

# @param [Integer] row
# @param [Integer] column
# @return Table cell at row, column
def cell_at(row, column)
    return rows[row][column]
end

# @param [Integer] column
# @return format of column
def column_format(column)
    return @columns[column].format
end

# @param [Integer] column
# @return label of column
def column_label(column)
    return @columns[column].label
end

# @param [Integer] column
# @return identifier of column
def column_identifier(column)
    return @columns[column].identifier
end

# @return number of rows in table
def number_of_rows
    return @rows.count
end
# @return number of columns in table
def number_of_columns
    return @columns.count
end

# @param [Symbol] identifier
# @return index of column with given identifier or nil if there is no such column.
def index_of_column(identifier)
    columns.each.index { |i|
        if column[i].identifier.to_sym == identifier
            return i
        end
    }
    return nil
end

# @param [Integer] column
# @return cell values in a column
def column_values(column)
    case column
    when String, Symbol
        index = index_of_column(column)
    else
        index = column
    end
    
    return rows.collect{ |row| row[column].value }
end

# Return formatted value of a cell. If formatted_value is not set for a cell, then the value
# is being formatted according to column format.
# Currently recognized formats:
# * :text - no formatting
# * :number
# * :currency
# * :units
# Parameters used in formatting:
# * :precision - number of decimal places
# * :currency - currency to be displayed, default is '$'
# * :units - units to be displayed, default is the same as currency
# * :format_string - string to format currency/unit values. Use %n vor value (number) and %u for unit. Default: '%n%u'
# @param [Integer] row
# @param [Integer] column
# @return Formatted cell value at row, column. 
def formatted_value_at(row, column)
    # FIXME: configure formatting; this is just for testing purposes
    col = columns[column]
    cell = @rows[row][column]
    fvalue = cell.formatted_value
    if fvalue
        return fvalue
    end
    precision = col.parameters[:precision]
    precision ||= 2

    value = cell.value
    return nil unless value

    # FIXME: localize
    delimiter = ' '
    separator = '.'
    
    case col.format
    when :text
        return value
    when :number
        # FIXME: make this localizable
        fvalue = value.to_string_with_precision(precision,delimiter, separator)
        return fvalue
    when :percent
        fvalue = (value * 100.0).to_string_with_precision(precision,delimiter, separator)
        return "#{fvalue} %"
    when :currency, :units
        fvalue = value.to_string_with_precision(precision,delimiter, separator)
        unit = col.parameters[:currency]
        unit ||= col.parameters[:unit]
        unit ||= '$'
        format = col.parameters[:format_string]
        format ||= '%n%u'
        
        begin
            return format.gsub(/%n/, fvalue).gsub(/%u/, unit)
        rescue
            return fvalue
        end
    else
        return value
    end
end

end # class DataTable
end # module Brewery
