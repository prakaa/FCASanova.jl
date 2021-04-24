function set_gr_plot_style()
    colors = palette(parse.(Colorant, ["#E24A33", "#348ABD", "#988ED5", "#777777",
                                    "#FBC15E", "#8EBA42", "#FFB5B8"]))
    default(titlefont = (16, "times"), guidefont=(12, "times"),
            legendfont = (12, "times"), tickfont = (10, "times"),
            background_color="#f0f0f0", palette=colors)
    gr()
end