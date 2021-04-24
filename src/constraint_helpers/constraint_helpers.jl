
function find_binding_constraints(df::DataFrame)
    filtered_df = df |> @filter(abs(_.MARGINALVALUE) > 0) |> DataFrame
    return filtered_df
end

function filter_by_prefix(df::DataFrame, col::Symbol,
                          prefix::String)
    filtered_df = df[startswith.(df[:, col], prefix), :]
    return filtered_df
end

function load_binding_fcas_constraints(files::Array{String})
    dfs = DataFrame[]
    println("Loading binding constraints")
    @showprogress for file in files
        chunk = DataFrame(read_parquet(file, use_threads=true))
        chunk = filter_by_prefix(chunk, :CONSTRAINTID, "F_")
        chunk = find_binding_constraints(chunk)
        push!(dfs, chunk)
    end
    return dfs
end

function impute_constraint_service(constraint::String)
    fcas_services = ["RREG", "R6", "R60", "R5",
                     "LREG", "L6", "L60", "L5"]
    category = "undef"
    for service in fcas_services
        if occursin("$(service)_", constraint)
            category = service
            break
        elseif endswith(constraint, "$(service)")
            category = service
            break
        elseif occursin("_$(service)-", constraint)
            category = service
            break
        end
    end
    return category
end

function impute_constraint_region(constraint::String)
    reg_match = match(r"F_([A-Z]*)[_+{1,2}]", constraint)
    region = string(reg_match.captures[1])
    return region
end

function map_binding_constraints(binding::DataFrame,
                                 mapping::DataFrame)
    println("Mapping constraint to regions and bidtypes")
    merged = leftjoin(binding, mapping,
                      on=[:CONSTRAINTID => :GENCONID,
                          :GENCONID_EFFECTIVEDATE => :EFFECTIVEDATE,
                          :GENCONID_VERSIONNO => :VERSIONNO],
                          makeunique=true)
    merged = merged[.!ismissing.(merged.REGIONID), :]
    return merged
end