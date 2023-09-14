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

use std::fmt::Debug;

use risingwave_common::array::{Array, BoolArray};
use risingwave_common::buffer::Bitmap;
use risingwave_expr_macro::function;

#[function("equal(boolean, boolean) -> boolean", batch_fn = "boolarray_eq")]
#[function("equal(*int, *int) -> boolean")]
#[function("equal(decimal, decimal) -> boolean")]
#[function("equal(*float, *float) -> boolean")]
#[function("equal(int256, int256) -> boolean")]
#[function("equal(serial, serial) -> boolean")]
#[function("equal(date, date) -> boolean")]
#[function("equal(time, time) -> boolean")]
#[function("equal(interval, interval) -> boolean")]
#[function("equal(timestamp, timestamp) -> boolean")]
#[function("equal(timestamptz, timestamptz) -> boolean")]
#[function("equal(date, timestamp) -> boolean")]
#[function("equal(timestamp, date) -> boolean")]
#[function("equal(time, interval) -> boolean")]
#[function("equal(interval, time) -> boolean")]
#[function("equal(varchar, varchar) -> boolean")]
#[function("equal(bytea, bytea) -> boolean")]
#[function("equal(list, list) -> boolean")]
#[function("equal(struct, struct) -> boolean")]
pub fn general_eq<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() == r.into()
}

#[function("not_equal(boolean, boolean) -> boolean", batch_fn = "boolarray_ne")]
#[function("not_equal(*int, *int) -> boolean")]
#[function("not_equal(decimal, decimal) -> boolean")]
#[function("not_equal(*float, *float) -> boolean")]
#[function("not_equal(int256, int256) -> boolean")]
#[function("not_equal(serial, serial) -> boolean")]
#[function("not_equal(date, date) -> boolean")]
#[function("not_equal(time, time) -> boolean")]
#[function("not_equal(interval, interval) -> boolean")]
#[function("not_equal(timestamp, timestamp) -> boolean")]
#[function("not_equal(timestamptz, timestamptz) -> boolean")]
#[function("not_equal(date, timestamp) -> boolean")]
#[function("not_equal(timestamp, date) -> boolean")]
#[function("not_equal(time, interval) -> boolean")]
#[function("not_equal(interval, time) -> boolean")]
#[function("not_equal(varchar, varchar) -> boolean")]
#[function("not_equal(bytea, bytea) -> boolean")]
#[function("not_equal(list, list) -> boolean")]
#[function("not_equal(struct, struct) -> boolean")]
pub fn general_ne<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() != r.into()
}

#[function(
    "greater_than_or_equal(boolean, boolean) -> boolean",
    batch_fn = "boolarray_ge"
)]
#[function("greater_than_or_equal(*int, *int) -> boolean")]
#[function("greater_than_or_equal(decimal, decimal) -> boolean")]
#[function("greater_than_or_equal(*float, *float) -> boolean")]
#[function("greater_than_or_equal(serial, serial) -> boolean")]
#[function("greater_than_or_equal(int256, int256) -> boolean")]
#[function("greater_than_or_equal(date, date) -> boolean")]
#[function("greater_than_or_equal(time, time) -> boolean")]
#[function("greater_than_or_equal(interval, interval) -> boolean")]
#[function("greater_than_or_equal(timestamp, timestamp) -> boolean")]
#[function("greater_than_or_equal(timestamptz, timestamptz) -> boolean")]
#[function("greater_than_or_equal(date, timestamp) -> boolean")]
#[function("greater_than_or_equal(timestamp, date) -> boolean")]
#[function("greater_than_or_equal(time, interval) -> boolean")]
#[function("greater_than_or_equal(interval, time) -> boolean")]
#[function("greater_than_or_equal(varchar, varchar) -> boolean")]
#[function("greater_than_or_equal(bytea, bytea) -> boolean")]
#[function("greater_than_or_equal(list, list) -> boolean")]
#[function("greater_than_or_equal(struct, struct) -> boolean")]
pub fn general_ge<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() >= r.into()
}

#[function("greater_than(boolean, boolean) -> boolean", batch_fn = "boolarray_gt")]
#[function("greater_than(*int, *int) -> boolean")]
#[function("greater_than(decimal, decimal) -> boolean")]
#[function("greater_than(*float, *float) -> boolean")]
#[function("greater_than(serial, serial) -> boolean")]
#[function("greater_than(int256, int256) -> boolean")]
#[function("greater_than(date, date) -> boolean")]
#[function("greater_than(time, time) -> boolean")]
#[function("greater_than(interval, interval) -> boolean")]
#[function("greater_than(timestamp, timestamp) -> boolean")]
#[function("greater_than(timestamptz, timestamptz) -> boolean")]
#[function("greater_than(date, timestamp) -> boolean")]
#[function("greater_than(timestamp, date) -> boolean")]
#[function("greater_than(time, interval) -> boolean")]
#[function("greater_than(interval, time) -> boolean")]
#[function("greater_than(varchar, varchar) -> boolean")]
#[function("greater_than(bytea, bytea) -> boolean")]
#[function("greater_than(list, list) -> boolean")]
#[function("greater_than(struct, struct) -> boolean")]
pub fn general_gt<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() > r.into()
}

#[function(
    "less_than_or_equal(boolean, boolean) -> boolean",
    batch_fn = "boolarray_le"
)]
#[function("less_than_or_equal(*int, *int) -> boolean")]
#[function("less_than_or_equal(decimal, decimal) -> boolean")]
#[function("less_than_or_equal(*float, *float) -> boolean")]
#[function("less_than_or_equal(serial, serial) -> boolean")]
#[function("less_than_or_equal(int256, int256) -> boolean")]
#[function("less_than_or_equal(date, date) -> boolean")]
#[function("less_than_or_equal(time, time) -> boolean")]
#[function("less_than_or_equal(interval, interval) -> boolean")]
#[function("less_than_or_equal(timestamp, timestamp) -> boolean")]
#[function("less_than_or_equal(timestamptz, timestamptz) -> boolean")]
#[function("less_than_or_equal(date, timestamp) -> boolean")]
#[function("less_than_or_equal(timestamp, date) -> boolean")]
#[function("less_than_or_equal(time, interval) -> boolean")]
#[function("less_than_or_equal(interval, time) -> boolean")]
#[function("less_than_or_equal(varchar, varchar) -> boolean")]
#[function("less_than_or_equal(bytea, bytea) -> boolean")]
#[function("less_than_or_equal(list, list) -> boolean")]
#[function("less_than_or_equal(struct, struct) -> boolean")]
pub fn general_le<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() <= r.into()
}

#[function("less_than(boolean, boolean) -> boolean", batch_fn = "boolarray_lt")]
#[function("less_than(*int, *int) -> boolean")]
#[function("less_than(decimal, decimal) -> boolean")]
#[function("less_than(*float, *float) -> boolean")]
#[function("less_than(serial, serial) -> boolean")]
#[function("less_than(int256, int256) -> boolean")]
#[function("less_than(date, date) -> boolean")]
#[function("less_than(time, time) -> boolean")]
#[function("less_than(interval, interval) -> boolean")]
#[function("less_than(timestamp, timestamp) -> boolean")]
#[function("less_than(timestamptz, timestamptz) -> boolean")]
#[function("less_than(date, timestamp) -> boolean")]
#[function("less_than(timestamp, date) -> boolean")]
#[function("less_than(time, interval) -> boolean")]
#[function("less_than(interval, time) -> boolean")]
#[function("less_than(varchar, varchar) -> boolean")]
#[function("less_than(bytea, bytea) -> boolean")]
#[function("less_than(list, list) -> boolean")]
#[function("less_than(struct, struct) -> boolean")]
pub fn general_lt<T1, T2, T3>(l: T1, r: T2) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.into() < r.into()
}

#[function(
    "is_distinct_from(boolean, boolean) -> boolean",
    batch_fn = "boolarray_is_distinct_from"
)]
#[function("is_distinct_from(*int, *int) -> boolean")]
#[function("is_distinct_from(decimal, decimal) -> boolean")]
#[function("is_distinct_from(*float, *float) -> boolean")]
#[function("is_distinct_from(serial, serial) -> boolean")]
#[function("is_distinct_from(int256, int256) -> boolean")]
#[function("is_distinct_from(date, date) -> boolean")]
#[function("is_distinct_from(time, time) -> boolean")]
#[function("is_distinct_from(interval, interval) -> boolean")]
#[function("is_distinct_from(timestamp, timestamp) -> boolean")]
#[function("is_distinct_from(timestamptz, timestamptz) -> boolean")]
#[function("is_distinct_from(date, timestamp) -> boolean")]
#[function("is_distinct_from(timestamp, date) -> boolean")]
#[function("is_distinct_from(time, interval) -> boolean")]
#[function("is_distinct_from(interval, time) -> boolean")]
#[function("is_distinct_from(varchar, varchar) -> boolean")]
#[function("is_distinct_from(bytea, bytea) -> boolean")]
#[function("is_distinct_from(list, list) -> boolean")]
#[function("is_distinct_from(struct, struct) -> boolean")]
pub fn general_is_distinct_from<T1, T2, T3>(l: Option<T1>, r: Option<T2>) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.map(Into::into) != r.map(Into::into)
}

#[function(
    "is_not_distinct_from(boolean, boolean) -> boolean",
    batch_fn = "boolarray_is_not_distinct_from"
)]
#[function("is_not_distinct_from(*int, *int) -> boolean")]
#[function("is_not_distinct_from(decimal, decimal) -> boolean")]
#[function("is_not_distinct_from(*float, *float) -> boolean")]
#[function("is_not_distinct_from(serial, serial) -> boolean")]
#[function("is_not_distinct_from(int256, int256) -> boolean")]
#[function("is_not_distinct_from(date, date) -> boolean")]
#[function("is_not_distinct_from(time, time) -> boolean")]
#[function("is_not_distinct_from(interval, interval) -> boolean")]
#[function("is_not_distinct_from(timestamp, timestamp) -> boolean")]
#[function("is_not_distinct_from(timestamptz, timestamptz) -> boolean")]
#[function("is_not_distinct_from(date, timestamp) -> boolean")]
#[function("is_not_distinct_from(timestamp, date) -> boolean")]
#[function("is_not_distinct_from(time, interval) -> boolean")]
#[function("is_not_distinct_from(interval, time) -> boolean")]
#[function("is_not_distinct_from(varchar, varchar) -> boolean")]
#[function("is_not_distinct_from(bytea, bytea) -> boolean")]
#[function("is_not_distinct_from(list, list) -> boolean")]
#[function("is_not_distinct_from(struct, struct) -> boolean")]
pub fn general_is_not_distinct_from<T1, T2, T3>(l: Option<T1>, r: Option<T2>) -> bool
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: Ord,
{
    l.map(Into::into) == r.map(Into::into)
}

#[function("is_true(boolean) -> boolean", batch_fn = "boolarray_is_true")]
pub fn is_true(v: Option<bool>) -> bool {
    v == Some(true)
}

#[function("is_not_true(boolean) -> boolean", batch_fn = "boolarray_is_not_true")]
pub fn is_not_true(v: Option<bool>) -> bool {
    v != Some(true)
}

#[function("is_false(boolean) -> boolean", batch_fn = "boolarray_is_false")]
pub fn is_false(v: Option<bool>) -> bool {
    v == Some(false)
}

#[function(
    "is_not_false(boolean) -> boolean",
    batch_fn = "boolarray_is_not_false"
)]
pub fn is_not_false(v: Option<bool>) -> bool {
    v != Some(false)
}

#[function("is_null(*) -> boolean", batch_fn = "batch_is_null")]
fn is_null<T>(v: Option<T>) -> bool {
    v.is_none()
}

#[function("is_not_null(*) -> boolean", batch_fn = "batch_is_not_null")]
fn is_not_null<T>(v: Option<T>) -> bool {
    v.is_some()
}

// optimized functions for bool arrays

fn boolarray_eq(l: &BoolArray, r: &BoolArray) -> BoolArray {
    let data = !(l.data() ^ r.data());
    let bitmap = l.null_bitmap() & r.null_bitmap();
    BoolArray::new(data, bitmap)
}

fn boolarray_ne(l: &BoolArray, r: &BoolArray) -> BoolArray {
    let data = l.data() ^ r.data();
    let bitmap = l.null_bitmap() & r.null_bitmap();
    BoolArray::new(data, bitmap)
}

fn boolarray_gt(l: &BoolArray, r: &BoolArray) -> BoolArray {
    let data = l.data() & !r.data();
    let bitmap = l.null_bitmap() & r.null_bitmap();
    BoolArray::new(data, bitmap)
}

fn boolarray_lt(l: &BoolArray, r: &BoolArray) -> BoolArray {
    let data = !l.data() & r.data();
    let bitmap = l.null_bitmap() & r.null_bitmap();
    BoolArray::new(data, bitmap)
}

fn boolarray_ge(l: &BoolArray, r: &BoolArray) -> BoolArray {
    let data = l.data() | !r.data();
    let bitmap = l.null_bitmap() & r.null_bitmap();
    BoolArray::new(data, bitmap)
}

fn boolarray_le(l: &BoolArray, r: &BoolArray) -> BoolArray {
    let data = !l.data() | r.data();
    let bitmap = l.null_bitmap() & r.null_bitmap();
    BoolArray::new(data, bitmap)
}

fn boolarray_is_distinct_from(l: &BoolArray, r: &BoolArray) -> BoolArray {
    let data = ((l.data() ^ r.data()) & (l.null_bitmap() & r.null_bitmap()))
        | (l.null_bitmap() ^ r.null_bitmap());
    BoolArray::new(data, Bitmap::ones(l.len()))
}

fn boolarray_is_not_distinct_from(l: &BoolArray, r: &BoolArray) -> BoolArray {
    let data = !(((l.data() ^ r.data()) & (l.null_bitmap() & r.null_bitmap()))
        | (l.null_bitmap() ^ r.null_bitmap()));
    BoolArray::new(data, Bitmap::ones(l.len()))
}

fn boolarray_is_true(a: &BoolArray) -> BoolArray {
    BoolArray::new(a.to_bitmap(), Bitmap::ones(a.len()))
}

fn boolarray_is_not_true(a: &BoolArray) -> BoolArray {
    BoolArray::new(!a.to_bitmap(), Bitmap::ones(a.len()))
}

fn boolarray_is_false(a: &BoolArray) -> BoolArray {
    BoolArray::new(!a.data() & a.null_bitmap(), Bitmap::ones(a.len()))
}

fn boolarray_is_not_false(a: &BoolArray) -> BoolArray {
    BoolArray::new(a.data() | !a.null_bitmap(), Bitmap::ones(a.len()))
}

fn batch_is_null(a: &impl Array) -> BoolArray {
    BoolArray::new(!a.null_bitmap(), Bitmap::ones(a.len()))
}

fn batch_is_not_null(a: &impl Array) -> BoolArray {
    BoolArray::new(a.null_bitmap().clone(), Bitmap::ones(a.len()))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use risingwave_common::types::{Decimal, F32, F64};

    use super::*;

    #[test]
    fn test_comparison() {
        assert!(general_eq::<Decimal, i32, Decimal>(dec("1.0"), 1));
        assert!(!general_ne::<Decimal, i32, Decimal>(dec("1.0"), 1));
        assert!(!general_gt::<Decimal, i32, Decimal>(dec("1.0"), 2));
        assert!(general_le::<Decimal, i32, Decimal>(dec("1.0"), 2));
        assert!(!general_ge::<Decimal, i32, Decimal>(dec("1.0"), 2));
        assert!(general_lt::<Decimal, i32, Decimal>(dec("1.0"), 2));
        assert!(general_is_distinct_from::<Decimal, i32, Decimal>(
            Some(dec("1.0")),
            Some(2)
        ));
        assert!(general_is_distinct_from::<Decimal, i32, Decimal>(
            None,
            Some(1)
        ));
        assert!(!general_is_distinct_from::<Decimal, i32, Decimal>(
            Some(dec("1.0")),
            Some(1)
        ));
        assert!(general_eq::<F32, i32, F64>(1.0.into(), 1));
        assert!(!general_ne::<F32, i32, F64>(1.0.into(), 1));
        assert!(!general_lt::<F32, i32, F64>(1.0.into(), 1));
        assert!(general_le::<F32, i32, F64>(1.0.into(), 1));
        assert!(!general_gt::<F32, i32, F64>(1.0.into(), 1));
        assert!(general_ge::<F32, i32, F64>(1.0.into(), 1));
        assert!(!general_is_distinct_from::<F32, i32, F64>(
            Some(1.0.into()),
            Some(1)
        ));
        assert!(general_eq::<i64, i32, i64>(1i64, 1));
        assert!(!general_ne::<i64, i32, i64>(1i64, 1));
        assert!(!general_lt::<i64, i32, i64>(1i64, 1));
        assert!(general_le::<i64, i32, i64>(1i64, 1));
        assert!(!general_gt::<i64, i32, i64>(1i64, 1));
        assert!(general_ge::<i64, i32, i64>(1i64, 1));
        assert!(!general_is_distinct_from::<i64, i32, i64>(
            Some(1i64),
            Some(1)
        ));
    }

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }
}
