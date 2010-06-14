module Brewery
class DataTablePresenter

def initialize(options = {})
    @options = options
    @formatters = Hash.new
end

def present_as_html(table)
    # FIXME: this does not belong here
    # insert style here
    table_id = @options[:table_id]
    table_style = @options[:table_style]
    attributes = Array.new
    if table_id
        attributes << " id='#{table_id}'"
    end
    if table_style
        attributes << " class='#{table_style}'"
    end
       
    attr_string = ' ' + attributes.join(' ')
    html = "<table#{attr_string}>\n"

    # Header row
    html << "<tr>\n"
    table.columns.each { |col|
        html << "<th>#{col.label}</th>"
    }
    html << "</tr>\n"
    
    # Rows
    table.rows.each_index { |r|
        html << "<tr>"
        table.rows[r].each_index { |c|
            column = table.columns[c]

            if column.parameters
                attrs = []
                alignment = column.parameters[:alignment]
                style = column.parameters[:style]
                if alignment
                    attrs << "align=#{alignment.to_s}"
                end
                if style
                    attrs << "class=#{style.to_s}"
                end
            else
            end
            
            if @formatters[c]
                cell = table.cell_at(r,c)
                if cell.parameters && cell.parameters[:style]
                    attrs << "class=#{cell.parameters[:style].to_s}"
                end
                value = @formatters[c].call(cell)
            else
                value = table.formatted_value_at(r, c)
            end
            
            if attrs && attrs.count > 0
                attr_string = ' ' + attrs.join(' ')
            else
                attr_string = ''
            end

            html << "<td#{attr_string}>#{value}</td>"
        }
        html << "</tr>\n"
    }
    
    html << "</table>\n"

    return html
end

def format_column(column, &block)
    @formatters[column] = block
end

end # class DataTablePresenter
end # module Brewery
