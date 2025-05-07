using DataFrames
# using Dates # Only if DateTime is part of your test data and predicates

# --- Predicates for "User's Approach" in testing ---
# These functions define how to identify the values to be replaced for the reference calculation.

# Predicate for actual NaN values (IEEE 754 NaN)
is_actual_nan(x) = isa(x, AbstractFloat) && isnan(x)

# Predicate for missing values
is_actual_missing(x) = ismissing(x)

# Predicate for nothing values
is_actual_nothing(x) = x === nothing # or isnothing(x) for Julia >= 1.2

# Predicate for positive Infinity values
is_actual_pos_inf(x) = isa(x, AbstractFloat) && isinf(x) && x > 0

replace_nan_with_missing(df; kwargs...) = OkValueReplacer.replace(df, NaN => missing; kwargs...)


replace_missing_with_nan(df; kwargs...) = OkValueReplacer.replace(df, missing => NaN; kwargs...)


replace_missing_with_nothing(df; kwargs...) = OkValueReplacer.replace(df, missing => nothing; kwargs...)


replace_nan_with_nothing(df; kwargs...) = OkValueReplacer.replace(df, NaN => nothing; kwargs...)


replace_nothing_with_nan(df; kwargs...) = OkValueReplacer.replace(df, nothing => NaN; kwargs...)



replace_nothing_with_missing(df; kwargs...) = OkValueReplacer.replace(df, nothing => missing; kwargs...)

replace_inf_with_nan(df; kwargs...) = OkValueReplacer.replace(df, Inf => NaN; kwargs...)

# --- Test Suite ---
@testset "OkValueReplacer.jl Tests" begin

    function test_replacement_integrity(
        original_df_builder,
        replacement_func::Function,
        predicate_func::Function,
        new_value_for_ref
    )
        df_orig = original_df_builder()
        df_orig_copy_for_mutation_check = copy(df_orig)

        # Apply the package function
        df_processed = replacement_func(df_orig)

        # --- Create reference DataFrame using "User's Approach" ---
        df_ref = copy(df_orig)
        for col_name in names(df_ref)
            col_data = df_ref[!, col_name]
            # The ifelse.(predicate, new_val, old_val) construct promotes types automatically
            df_ref[!, col_name] = ifelse.(predicate_func.(col_data), new_value_for_ref, col_data)
        end

        # Test 1: Processed DataFrame matches reference DataFrame
        # isequal is important for DataFrames with NaN, missing, nothing
        @test isequal(df_processed, df_ref)

        # Test 2: Original DataFrame should not be modified
        @test isequal(df_orig, df_orig_copy_for_mutation_check)

        # Test 3: Ensure a new DataFrame object was created (unless no changes were made at all)
        if !isequal(df_orig, df_processed) # If changes occurred
            @test df_orig !== df_processed
        end
    end

    # --- Test Cases ---

    @testset "replace_nan_with_missing" begin
        df_builder() = DataFrame(
            A=[1.0, NaN, 3.0, Inf, missing],
            B=[NaN, 2.0, NaN, 0.0, NaN],
            C=["text", "NaN", "data", "end", "another"], # String "NaN" should not be affected
            D=Any[NaN, 1, nothing, NaN, 5.5]
        )
        test_replacement_integrity(df_builder, replace_nan_with_missing, is_actual_nan, missing)
    end

    @testset "replace_missing_with_nan" begin
        df_builder() = DataFrame(
            A=[missing, 1.0, missing, NaN, 0.0],
            B=[1, missing, 3, 4, missing],
            C=Any[missing, "text", missing, nothing, 5]
        )
        test_replacement_integrity(df_builder, replace_missing_with_nan, is_actual_missing, NaN)
    end

    @testset "replace_missing_with_nothing" begin
        df_builder() = DataFrame(
            A=[missing, 1.0, missing, NaN, nothing],
            B=[1, missing, 3, 4, missing],
            C=Any[missing, "text", missing, 0.0, 5]
        )
        test_replacement_integrity(df_builder, replace_missing_with_nothing, is_actual_missing, nothing)
    end

    @testset "replace_nan_with_nothing" begin
        df_builder() = DataFrame(
            A=[1.0, NaN, 3.0, Inf, missing, NaN],
            B=[NaN, 2.0, NaN, 0.0, NaN, 1.0],
            C=Any[NaN, "text", nothing, NaN, 5, 5]
        )
        test_replacement_integrity(df_builder, replace_nan_with_nothing, is_actual_nan, nothing)
    end

    @testset "replace_nothing_with_nan" begin
        df_builder() = DataFrame(
            A=[nothing, 1.0, nothing, NaN, missing],
            B=[1, nothing, 3, 4, nothing],
            C=Any[nothing, "text", nothing, 0.0, 5]
        )
        test_replacement_integrity(df_builder, replace_nothing_with_nan, is_actual_nothing, NaN)
    end

    @testset "replace_nothing_with_missing" begin
        df_builder() = DataFrame(
            A=[nothing, 1.0, nothing, NaN, missing],
            B=[1, nothing, 3, 4, nothing],
            C=Any[nothing, "text", nothing, 0.0, 5]
        )
        test_replacement_integrity(df_builder, replace_nothing_with_missing, is_actual_nothing, missing)
    end

    @testset "replace_inf_with_nan" begin
        df_builder() = DataFrame(
            A=[Inf, 1.0, -Inf, NaN, Inf, 0.0], # Test with positive and negative Inf
            B=[1.0, Inf, 3.0, Inf, missing, nothing],
            C=Any[Inf, "text", -Inf, 0.0, 5, 5]
        )
        test_replacement_integrity(df_builder, replace_inf_with_nan, is_actual_pos_inf, NaN)

        # Additional check: -Inf should not be replaced by replace_inf_with_nan
        df_specific_inf = DataFrame(val=[Inf, -Inf, 0.0, NaN])
        df_processed_specific = replace_inf_with_nan(df_specific_inf)
        @test isequal(df_processed_specific.val[1], NaN)     # Inf -> NaN
        @test isequal(df_processed_specific.val[2], -Inf)    # -Inf remains -Inf
        @test isequal(df_processed_specific.val[3], 0.0)
        @test is_actual_nan(df_processed_specific.val[4]) # NaN remains NaN
    end

end
