
function map_fcas_short_and_long(col::Array{String}; 
                                 short_to_long=true)
    fcas_map = Dict("R6" => "RAISE6SEC", "R60" => "RAISE60SEC",
                    "L6" => "LOWER6SEC", "L60" => "LOWER60SEC",
                    "R5" => "RAISE5MIN", "L5" => "LOWER5MIN",
                    "RREG" => "RAISEREG", "LREG" => "LOWERREG")
    if !short_to_long
        fcas_map = Dict(value => key for (key, value) in map)
    end
    col = [fcas_map[key] for key in col]
    return col
end