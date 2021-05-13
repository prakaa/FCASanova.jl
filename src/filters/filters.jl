function filter_data_by_year(df::DataFrame, year::Int, datetime_col::Symbol)
    mask = (
        (df[:, datetime_col].>= DateTime("$year-01-01T00:05:00")) .&
         (df[:, datetime_col].<= DateTime("$(year+1)-01-01T00:00:00"))
    )
    year_df = df[mask, :]
    return year_df
end