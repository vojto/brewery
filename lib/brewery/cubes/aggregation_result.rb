module Brewery

# Result object from {Slice#aggregate}
class AggregationResult

# Aggregated measure
attr_accessor :measure

# Summary of aggregation. Hash with keys: :sum, :record_count
attr_accessor :summary

# If aggregation was by breaking down a dimension, rows contains aggregations
# per each dimension level value specified in aggregation options. For example,
# if you aggregate by dimension Date at the level Month, then each row will
# represent aggregation for given month. For more information see {Slice#aggregate}
attr_accessor :rows

# Remainder of aggregation if limit was used. Similar hash as summary hash.
attr_accessor :remainder

# Options used for aggregation
attr_accessor :aggregation_options
end # class AggregationResult
end # module Brewery
