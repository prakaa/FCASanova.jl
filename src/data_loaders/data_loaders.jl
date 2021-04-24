
function list_data_by_years(dir::String; fformat::String="csv",
                            years::UnitRange)
    files = String[]
    for year in years
        year_files = glob("*$year*.$fformat", dir)
        append!(files, year_files)
    end
    return files
end

function load_parquet_data(files::Array{String}; str::String="data")
    dfs = DataFrame[]
    println("Loading $str")
    @showprogress for file in files
        chunk = DataFrame(read_parquet(file, use_threads=true))
        push!(dfs, chunk)
    end
    df = vcat(dfs...)
    return df
end