// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Provides validation logic for expected errors.
use risingwave_expr::ExprError;

/// Ignore errors related to `0`.
fn is_zero_err(db_error: &str) -> bool {
    db_error.contains(&ExprError::DivisionByZero.to_string()) || db_error.contains("can't be zero")
}

/// `Casting to u32 out of range` occurs when we have functions
/// which expect non-negative arguments,
/// e.g. `select 222 << -1`
// NOTE: If this error occurs too often, perhaps it is better to
// wrap call sites with `abs(rhs)`, e.g. 222 << abs(-1);
fn is_numeric_out_of_range_err(db_error: &str) -> bool {
    db_error.contains(&ExprError::NumericOutOfRange.to_string())
        || db_error.contains("Casting to u32 out of range")
}

/// Skip queries with unimplemented features
fn is_unimplemented_error(db_error: &str) -> bool {
    db_error.contains("not yet implemented")
}

/// This error occurs because we test `implicit` casts as well,
/// generated expressions may be ambiguous as a result,
/// if there are multiple candidates signatures.
/// Additionally.
fn not_unique_error(db_error: &str) -> bool {
    db_error.contains("Bind error") && db_error.contains("is not unique")
}

fn is_window_error(db_error: &str) -> bool {
    db_error.contains("Bind error: The size arg of window table function should be an interval literal")
        || db_error.contains("Bind error: The 2st arg of window table function should be a column name but not complex expression. Consider using an intermediate CTE or view as workaround")
}

// Streaming nested-loop join is not supported, as it is expensive.
fn is_nested_loop_join_error(db_error: &str) -> bool {
    db_error.contains("Not supported: streaming nested-loop join")
}

fn is_subquery_unnesting_error(db_error: &str) -> bool {
    db_error.contains("Subquery can not be unnested")
}

/// Can't avoid numeric overflows, we do not eval const expr
fn is_numeric_overflow_error(db_error: &str) -> bool {
    db_error.contains("Number") && db_error.contains("overflows")
}

/// Negative substr error
fn is_neg_substr_error(db_error: &str) -> bool {
    db_error.contains("negative substring length not allowed")
}

/// Zero or negative overlay start error
fn is_overlay_start_error(db_error: &str) -> bool {
    db_error.contains("Invalid parameter start") && db_error.contains("is not positive")
}

/// Certain errors are permitted to occur. This is because:
/// 1. It is more complex to generate queries without these errors.
/// 2. These errors seldom occur, skipping them won't affect overall effectiveness of sqlsmith.
pub fn is_permissible_error(db_error: &str) -> bool {
    is_numeric_out_of_range_err(db_error)
        || is_zero_err(db_error)
        || is_unimplemented_error(db_error)
        || not_unique_error(db_error)
        || is_window_error(db_error)
        || is_nested_loop_join_error(db_error)
        || is_subquery_unnesting_error(db_error)
        || is_numeric_overflow_error(db_error)
        || is_neg_substr_error(db_error)
        || is_overlay_start_error(db_error)
}
