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
    _replace_value_in_df(df::DataFrame, old_value, new_value)

Internal core function to replace a specified old value with a new value
across all columns of a new copy of a DataFrame.
"""
function _replace_value_in_df(df::DataFrame, old_value::Any, new_value::Any)
    new_df = copy(df) # Create a new copy of the DataFrame
    for col_name in names(new_df)
        column_vector = new_df[!, col_name]
        # Base.replace creates a new vector with the replacements and handles type promotion
        new_df[!, col_name] = Base.replace(column_vector, old_value => new_value)
    end
    return new_df
end

"""
    replace_nan_with_missing(df::DataFrame)

Creates a new DataFrame replacing all `NaN` values with `missing`.
"""
replace_nan_with_missing(df::DataFrame) = _replace_value_in_df(df, NaN, missing)

"""
    replace_missing_with_nan(df::DataFrame)

Creates a new DataFrame replacing all `missing` values with `NaN`.
Column types will be promoted to support `Float64` (for `NaN`) if necessary.
"""
replace_missing_with_nan(df::DataFrame) = _replace_value_in_df(df, missing, NaN)

"""
    replace_missing_with_nothing(df::DataFrame)

Creates a new DataFrame replacing all `missing` values with `nothing`.
Column types will be promoted to `Union{T, Nothing}` if necessary.
"""
replace_missing_with_nothing(df::DataFrame) = _replace_value_in_df(df, missing, nothing)

"""
    replace_nan_with_nothing(df::DataFrame)

Creates a new DataFrame replacing all `NaN` values with `nothing`.
Column types will be promoted to `Union{T, Nothing}` if necessary.
"""
replace_nan_with_nothing(df::DataFrame) = _replace_value_in_df(df, NaN, nothing)

"""
    replace_nothing_with_nan(df::DataFrame)

Creates a new DataFrame replacing all `nothing` values with `NaN`.
Column types will be promoted to support `Float64` (for `NaN`) if necessary.
"""
replace_nothing_with_nan(df::DataFrame) = _replace_value_in_df(df, nothing, NaN)

"""
    replace_nothing_with_missing(df::DataFrame)

Creates a new DataFrame replacing all `nothing` values with `missing`.
Column types will be promoted to `Union{T, Missing}` if necessary.
"""
replace_nothing_with_missing(df::DataFrame) = _replace_value_in_df(df, nothing, missing)

"""
    replace_inf_with_nan(df::DataFrame)

Creates a new DataFrame replacing all positive `Inf` (Infinity) values with `NaN`.
This function specifically targets `Inf`. For `-Inf`, a separate or modified
function would be needed.
"""
replace_inf_with_nan(df::DataFrame) = _replace_value_in_df(df, Inf, NaN)

end
