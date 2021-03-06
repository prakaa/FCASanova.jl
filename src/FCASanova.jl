module FCASanova

using ColorSchemes
using DataFrames
using Dates
using Glob
using Parquet
using Plots
using ProgressMeter
using Query

include("aggregation/groupbys.jl")
include("constraint_helpers/constraint_helpers.jl")
include("data_loaders/data_loaders.jl")
include("filters/filters.jl")
include("mappers/mappers.jl")
include("plot_helpers/plot_settings.jl")

export fcas_markets,
       find_binding_constraints,
       filter_by_prefix,
       filter_data_by_year,
       groupby_and_sum,
       groupby_and_count,
       impute_constraint_categories,
       list_data_by_years,
       load_applicable_fcas_constraints,
       load_binding_fcas_constraints,
       load_parquet_data,
       map_binding_constraints,
       map_fcas_short_and_long,
       set_plot_style

const fcas_markets = ["RAISE6SEC", "RAISE60SEC", "LOWER6SEC", 
                      "LOWER60SEC", "RAISE5MIN", "LOWER5MIN",
                      "RAISEREG", "LOWERREG"]
end
