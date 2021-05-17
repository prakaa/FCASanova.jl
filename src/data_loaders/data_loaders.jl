
function list_data_by_years(dir::String; fformat::String="csv",
                            years::Array{Int})
    files = String[]
    for year in years
        year_files = glob("*$year*.$fformat", dir)
        append!(files, year_files)
    end
    return files
end

"""
    load_parquet_data(files::Array{String}; str::String="data")

Loads parquet files (path to files in 'files' variable)

# Arguments
- `str::String`: data description printed when loading data
"""
function load_parquet_data(files::Array{String}; str::String="data")
    n = length(files)
    dfs = Array{DataFrame}(undef, n)
    println("Loading $str")
    @showprogress for i in 1:n
        chunk = DataFrame(read_parquet(files[i], use_threads=true))
        dfs[i] = chunk
    end
    df = vcat(dfs...)
    return df
end