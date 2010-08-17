# This comment should be ignored
# Boo baa bah
#

require 'data_objects'

module Brewery

# Portion of a cube. Slices are creating by cutting cubes or other slices. Used mainly
# for aggregating measures and drilling down through dimensions.
#
# @example Slicing a cube (see {Cube#whole})
#   # Assume we have a cube with dimensions date and category
#   slice = cube.whole # Get unsliced cube
#   slice = slice.cut_by_point(:date, [2010])
# @example Aggregate whole slice (see {#aggregate})
#   rows = slice.aggregate(:amount)
#   result = rows[0] # Only one row - summary row
#   puts "Total amount     : result[:sum]"
#   puts "Transaction count: result[:record_count]"
# @example Drill-down and aggregate (see {#aggregate} with row options)
#   rows = slice.aggregate(:amount, {:row_dimension => :date, 
#                                    :row_levels => [:year, :month]})
#   puts "Totals by month"
#   result.each { |row|
#       puts "#{row[:year]} #{row[:month_name]}: #{row[:sum]}"
#   }
#
# @author Stefan Urbanek <stefan@agentfarms.net>

class Slice
include DataObjects::Quoting

# List of cuts which define the slice - portion of a cube.
attr_reader :cuts

# @deprecated
attr_reader :cut_values
attr_reader :summaries

# Initialize slice instance as part of a cube
def initialize(cube)
    @cube = cube
    @cut_values = Hash.new
    
    @cuts = Array.new
    @summaries = Hash.new
end

# Copying contructor, called for Slice#dup
def initialize_copy(*)
    @cut_values = @cut_values.dup
    @cuts = @cuts.dup
    @summaries = Hash.new
    # FIXME: is this the right behaviour?
    @computed_fields = nil
end

# Cut slice by provided cut
# @see Cut#initialize
# @param [Cut] cut to cut the slice by
# @return [Slice] new slice with added cut
def cut_by(cut)
	slice = self.dup
	slice.add_cut(cut)
	return slice
end

# Cut slice by dimension point specified by path
# @param [Array] path Dimension point specified by array of values. See {Dimension}
# @see Cut#initialize
# @return [Slice] new slice with added cut
def cut_by_point(dimension, path)
	return self.cut_by(Cut.point_cut(@cube.dimension_object(dimension), path))
end

# Cut slice by ordered dimension from point specified by dimension
# keys in from_key to to_key
# @return [Slice] new slice with added cut
# @param [Array] path Dimension point specified by array of values. See {Dimension}
def cut_by_range(dimension, from_key, to_key)
	return self.cut_by(Cut.range_cut(@cube.dimension_object(dimension), from_key, to_key))
end


# Add another cut to the receiver.
# @param [Cut] cut Cut to be added
def add_cut(cut)
	@cuts << cut
	@summaries.clear
end

# Remove all cuts by dimension from the receiver.
def remove_cuts_by_dimension(dimension)
	@cuts.delete_if { |cut|
		@cube.dimension_object(cut.dimension) == @cube.dimension_object(dimension)
	}
	@summaries.clear
end

# @deprecated
def dataset
    @cube.dataset
end

# Aggregate measure.
#
# @param [Symbol] measure Measure to be aggregated, for example: amount, price, ...
# @param [Hash] options Options for more refined aggregation
# @option options [Symbol, Dimension] :row_dimension Dimension used for row grouping
# @option options [Array] :row_levels Group by dimension levels
# @option options [Symbol] :limit Possible values: ':value', `:percent`, `:rank`
# @option options [Symbol] :limit_aggregation Which aggregation is used for determining limit
# @option options [Number] :limit_value Limit value based on limit_type 
# @option options [Symbol] :limit_sort Possible values: `:ascending`, `:descending`
# @option options [Symbol] :order_direction How to order aggregated fields
# @option options [Symbol] :page Used for pagination of results.
# @option options [Symbol] :page_size Size of page for paginated results
# == Examples:
# * aggregate(:amount, { :row_dimension => [:date], :row_levels => [:year, :month]} )
# @return [AggregationResult] object with aggregation summary and rows where each
#   row represents a point at row_dimension if specified.
# @todo Rewrite this to use StarSchema - reuse code
def aggregate(measure, options = {})

    query = create_star_query(options)
    query.prepare_for_aggregation(measure, options)
    query.computed_fields = @computed_fields
    
    ################################################
	# 7. Compute summary

    # Brewery::logger.debug "slice SQL: #{statement}"

    if !@summaries[measure]
        summary_data = query.aggregation_summary

        summary = Hash.new

        if options[:operators]
            aggregations = options[:operators]
        else
            aggregations = [:sum]
        end

        aggregations.each { |agg|
            field = query.aggregated_field_name(measure, agg).to_sym
            value = summary_data[field]
    
            # FIXME: use appropriate type (Sequel SQLite returns String)
            if value.class == String
                value = value.to_f
            end
            summary[agg] = value
    	}
    	
        value = summary_data[:record_count]
        if value.class == String
            value = value.to_f
        end
        summary[:record_count] = value

    	@summaries[measure] = summary
    else
        summary = @summaries[measure]
    end

    ################################################
	# 8. Execute main selection

    if query.is_drill_down
        query.aggregate_drill_down_rows
        rows = query.rows
        r_sum = query.row_sum
    else
        # Only summary
        rows = Array.new
        r_sum = 0
    end
    
    # Compute remainder
    
    if query.has_limit
        remainder = Hash.new
        sumsum = summary[:sum]?summary[:sum] : 0
        remainder[:sum] = sumsum - r_sum
        remainder[:record_count] = summary[:record_count] - rows.count
    else
        remainder = nil
    end
    
# puts "==> ELAPSED COLLECT: #{Time.now - t}"

    results = AggregationResult.new
    results.rows = rows
    results.aggregation_options = options
    results.measure = measure
    results.remainder = remainder
    results.summary = @summaries[measure]
    
    return results
end

def create_star_query(options = {})
	query = @cube.create_star_query

    ################################################
	# 1. Apply cuts
	
	@cuts.each { |cut|
		if !cut.dimension
		    raise RuntimeError, "No dimension in cut (#{cut.class}), slicing cube '#{@cube.name}'"
		end

		dimension = @cube.dimension_object(cut.dimension)
		if !dimension
		    raise RuntimeError, "No cut dimension '#{cut.dimension.name}' in cube '#{@cube.name}'"
		end

		# puts "==> WHERE COND CUT: #{cut.dimension} DIM: #{dimension} ALIAS: #{dim_alias}"
		query.add_cut(cut)
	}
    
    query.order_by = options[:order_by]
    query.order_direction = options[:order_direction]
    query.page = options[:page]
    query.page_size = options[:page_size]
    
    return query
end

def dimension_values_at_path(dimension_ref, path, options = {})
    dimension = @cube.dimension_object(dimension_ref)
    query = create_star_query(options)
    return query.dimension_values_at_path(dimension, path)
end

def dimension_detail_at_path(dimension_ref, path)
    dimension = @cube.dimension_object(dimension_ref)
    query = create_star_query
    return query.dimension_detail_at_path(dimension, path)
end

def facts(options = {})
	query = create_star_query(options)

    return query.records
end

def add_computed_field(field_name, &block) 
    if !@computed_fields
        @computed_fields = Hash.new
    end
    
    @computed_fields[field_name] = block
end

end # class Slice
end # module Brewery