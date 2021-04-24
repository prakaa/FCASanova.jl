
function groupby_and_sum(df::DataFrame, groupby_cols::Array{Symbol},
                         sum_col::String)
    grouped = groupby(df, groupby_cols)
    sum_df = combine(grouped, sum_col => sum)
    sort!(sum_df, groupby_cols)
    return sum_df
end

function groupby_and_count(df::DataFrame, groupby_cols::Array{Symbol})
    gd = groupby(df, groupby_cols)
    count = combine(gd, nrow => :COUNT)
    return count
end