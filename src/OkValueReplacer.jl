module OkValueReplacer

using DataFrames

export replace_nan_with_missing,
    replace_missing_with_nan,
    replace_missing_with_nothing,
    replace_nan_with_nothing,
    replace_nothing_with_nan,
    replace_nothing_with_missing,
    replace_inf_with_nan

"""
    _replace_value_in_df(df::DataFrame, old_value, new_value; cols=All())

Internal core function to replace a specified old value with a new value
across specified columns of a new copy of a DataFrame.

# Arguments
- `df::DataFrame`: The DataFrame to process
- `old_value::Any`: The value to be replaced
- `new_value::Any`: The value to replace with

# Keywords
- `cols=All()`: Columns to process. Defaults to all columns.
  Can be column names, indices, or a vector of these.
"""
function _replace_value_in_df(df::DataFrame, old_value::Any, new_value::Any; cols=All())
    new_df = copy(df) # Create a new copy of the DataFrame
    col_names = DataFrames.names(df, cols)
    for col_name in col_names
        column_vector = new_df[!, col_name]
        # Base.replace creates a new vector with the replacements and handles type promotion
        new_df[!, col_name] = Base.replace(column_vector, old_value => new_value)
    end
    return new_df
end


replace_nan_with_missing(df; kwargs...) = _replace_value_in_df(df, NaN, missing; kwargs...)


replace_missing_with_nan(df; kwargs...) = _replace_value_in_df(df, missing, NaN; kwargs...)


replace_missing_with_nothing(df; kwargs...) = _replace_value_in_df(df, missing, nothing; kwargs...)


replace_nan_with_nothing(df; kwargs...) = _replace_value_in_df(df, NaN, nothing; kwargs...)


replace_nothing_with_nan(df; kwargs...) = _replace_value_in_df(df, nothing, NaN; kwargs...)


replace_nothing_with_missing(df; kwargs...) = _replace_value_in_df(df, nothing, missing; kwargs...)


replace_inf_with_nan(df; kwargs...) = _replace_value_in_df(df, Inf, NaN; kwargs...)

end
