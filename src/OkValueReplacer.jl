module OkValueReplacer

using DataFrames

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

OkValueReplacer.replace(df::AbstractDataFrame, p::Pair; kwargs...) = _replace_value_in_df(df, p[1], p[2]; kwargs...)

end
