"""
    filter_by_prefix(df::DataFrame, col::Symbol, prefix::String)

Finds rows in the column `col` of DataFrame `df` that begin with `prefix`
"""
function filter_by_prefix(df::DataFrame, col::Symbol,
                          prefix::String)
    filtered_df = df[startswith.(df[:, col], prefix), :]
    return filtered_df
end

"""
    find_binding_constraints(df::DataFrame)

Find binding constraints, where binding is defined to be when: `|MARGINALVALUE| > 0`.

"""
function find_binding_constraints(df::DataFrame)
    filtered_df = df[df[!, :MARGINALVALUE] .> 0.0, :]
    return filtered_df
end

"""
    find_applicable_constraints(df::DataFrame)

Find applicable constraints, where applicable is defined to be when:
- ``MARGINALVALUE = 0``
- ``LHS ≥ RHS``
- ``RHS ≥ 0``
"""
function find_applicable_constraints(df::DataFrame)
    df = df[df[!, :MARGINALVALUE] .== 0.0, :]
    df = df[df[!, :RHS] .≥ 0.0, :]
    df[!, :LHS] = round.(df[!, :LHS], digits=2)
    df[!, :RHS] = round.(df[!, :RHS], digits=2)
    filtered_df = df[df[!, :LHS] .≥ df[!, :RHS], :]
    return filtered_df
end

"""
    load_binding_fcas_constraints(files::Array{String})

Load constraint data (file paths in 'files`) and find binding constraints. 
Binding is defined to be when: `|MARGINALVALUE| > 0`.
"""
function load_binding_fcas_constraints(files::Array{String})
    n = length(files)
    dfs = Array{DataFrame}(undef, n)
    println("Loading binding constraints")
    @showprogress for i in 1:n
        @inbounds file = files[i]
        chunk = DataFrame(read_parquet(file, use_threads=true))
        chunk = filter_by_prefix(chunk, :CONSTRAINTID, "F_")
        chunk = find_binding_constraints(chunk)
        @inbounds dfs[i] = chunk
    end
    println("Binding constraints loaded")
    return dfs
end

"""
    load_applicable_fcas_constraints(files::Array{String})

Load constraint data (file paths in 'files`) and find applicable constraints. 
Applicable is defined to be when: `LHS ≥ RHS`.
"""
function load_applicable_fcas_constraints(files::Array{String})
    n = length(files)
    dfs = Array{DataFrame}(undef, n)
    println("Loading applicable constraints")
    p = Progress(n)
    s_lock = Threads.SpinLock()
    prog = Threads.Atomic{Int}(0)
    Threads.@threads for i in 1:n
        @inbounds file = files[i]
        chunk = DataFrame(read_parquet(file, use_threads=false))
        chunk = filter_by_prefix(chunk, :CONSTRAINTID, "F_")
        chunk = find_applicable_constraints(chunk)
        @inbounds dfs[i] = chunk
        Threads.atomic_add!(prog, 1)
        lock(s_lock)
        update!(p, prog[])
        unlock(s_lock)
    end
    println("Applicable constraints loaded")
    return dfs
end

"""
    impute_constraint_service(Constraint::String)

Impute the FCAS service/market for which the constraint applies. Mostly based on AEMO'S
constraint naming guidelines, but includes imputation for exceptions that have been
encountered in the constraint bank.

Will return a short description of the FCAS service/market, e.g. L6 for Lower 6 Seconds
"""
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

"""
    impute_constraint_region(constraint::String)

Based on AEMO's constraint naming guidelines, impute the region to which the constraint
applies. 

# Regions
- Global (`I`)
- Mainland (`MAIN`)
- Individual states, or some combination (`T`, `S`, `N`, `Q`, `V`)
- Yet to be determined code related to Tasmania (`TASCAP`)
    - Applies when global Regulation constraints are binding
- Region groups (e.g. `ESTN` or `STHN`)

"""
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