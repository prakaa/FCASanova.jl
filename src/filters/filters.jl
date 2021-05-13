function filter_data_by_year(df::DataFrame, year::Int, datetime_col::Symbol)
    mask = (
        (df[:, datetime_col].>= DateTime("$year-01-01T00:05:00")) .&
         (df[:, datetime_col].<= DateTime("$(year+1)-01-01T00:00:00"))
    )
    year_df = df[mask, :]
    return year_df
end

"""
    filter_by_prefix(df::DataFrame, col::Symbol, prefix::String)

Finds rows in the column `col` of DataFrame `df` that begin with `prefix`
"""
function filter_by_prefix(df::DataFrame, col::Symbol,
                          prefix::String)
    filtered_df = df[startswith.(df[:, col], prefix), :]
    return filtered_df
end
