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

use std::fmt::Write;
use std::str::FromStr;

use futures_util::FutureExt;
use itertools::Itertools;
use risingwave_common::array::{ListRef, ListValue, StructRef, StructValue};
use risingwave_common::cast::{
    parse_naive_date, parse_naive_datetime, parse_naive_time, str_to_bytea as str_to_bytea_common,
};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Date, Decimal, Int256, Interval, IntoOrdered, JsonbRef, ScalarImpl, Time, Timestamp,
    Timestamptz, ToText, F32, F64,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr_macro::function;
use risingwave_pb::expr::expr_node::PbType;

use crate::expr::{build_func, Context, Expression, InputRefExpression};
use crate::{ExprError, Result};

/// String literals for bool type.
///
/// See [`https://www.postgresql.org/docs/9.5/datatype-boolean.html`]
const TRUE_BOOL_LITERALS: [&str; 9] = ["true", "tru", "tr", "t", "on", "1", "yes", "ye", "y"];
const FALSE_BOOL_LITERALS: [&str; 10] = [
    "false", "fals", "fal", "fa", "f", "off", "of", "0", "no", "n",
];

#[function("cast(varchar) -> date")]
pub fn str_to_date(elem: &str) -> Result<Date> {
    Ok(Date::new(
        parse_naive_date(elem).map_err(|err| ExprError::Parse(err.into()))?,
    ))
}

#[function("cast(varchar) -> time")]
pub fn str_to_time(elem: &str) -> Result<Time> {
    Ok(Time::new(
        parse_naive_time(elem).map_err(|err| ExprError::Parse(err.into()))?,
    ))
}

#[function("cast(varchar) -> timestamp")]
pub fn str_to_timestamp(elem: &str) -> Result<Timestamp> {
    Ok(Timestamp::new(
        parse_naive_datetime(elem).map_err(|err| ExprError::Parse(err.into()))?,
    ))
}

#[function("cast(varchar) -> *int")]
#[function("cast(varchar) -> decimal")]
#[function("cast(varchar) -> *float")]
#[function("cast(varchar) -> int256")]
#[function("cast(varchar) -> interval")]
#[function("cast(varchar) -> jsonb")]
pub fn str_parse<T>(elem: &str) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    elem.trim()
        .parse()
        .map_err(|err: <T as FromStr>::Err| ExprError::Parse(err.to_string().into()))
}

#[function("cast(int2) -> int256")]
#[function("cast(int4) -> int256")]
#[function("cast(int8) -> int256")]
pub fn to_int256<T: TryInto<Int256>>(elem: T) -> Result<Int256> {
    elem.try_into()
        .map_err(|_| ExprError::CastOutOfRange("int256"))
}

#[function("cast(jsonb) -> boolean")]
pub fn jsonb_to_bool(v: JsonbRef<'_>) -> Result<bool> {
    v.as_bool().map_err(|e| ExprError::Parse(e.into()))
}

/// Note that PostgreSQL casts JSON numbers from arbitrary precision `numeric` but we use `f64`.
/// This is less powerful but still meets RFC 8259 interoperability.
#[function("cast(jsonb) -> int2")]
#[function("cast(jsonb) -> int4")]
#[function("cast(jsonb) -> int8")]
#[function("cast(jsonb) -> decimal")]
#[function("cast(jsonb) -> float4")]
#[function("cast(jsonb) -> float8")]
pub fn jsonb_to_number<T: TryFrom<F64>>(v: JsonbRef<'_>) -> Result<T> {
    v.as_number()
        .map_err(|e| ExprError::Parse(e.into()))?
        .into_ordered()
        .try_into()
        .map_err(|_| ExprError::NumericOutOfRange)
}

/// In `PostgreSQL`, casting from timestamp to date discards the time part.
#[function("cast(timestamp) -> date")]
pub fn timestamp_to_date(elem: Timestamp) -> Date {
    Date(elem.0.date())
}

/// In `PostgreSQL`, casting from timestamp to time discards the date part.
#[function("cast(timestamp) -> time")]
pub fn timestamp_to_time(elem: Timestamp) -> Time {
    Time(elem.0.time())
}

/// In `PostgreSQL`, casting from interval to time discards the days part.
#[function("cast(interval) -> time")]
pub fn interval_to_time(elem: Interval) -> Time {
    let usecs = elem.usecs_of_day();
    let secs = (usecs / 1_000_000) as u32;
    let nano = (usecs % 1_000_000 * 1000) as u32;
    Time::from_num_seconds_from_midnight_uncheck(secs, nano)
}

#[function("cast(int4) -> int2")]
#[function("cast(int8) -> int2")]
#[function("cast(int8) -> int4")]
#[function("cast(float4) -> int2")]
#[function("cast(float8) -> int2")]
#[function("cast(float4) -> int4")]
#[function("cast(float8) -> int4")]
#[function("cast(float4) -> int8")]
#[function("cast(float8) -> int8")]
#[function("cast(float8) -> float4")]
#[function("cast(decimal) -> int2")]
#[function("cast(decimal) -> int4")]
#[function("cast(decimal) -> int8")]
#[function("cast(decimal) -> float4")]
#[function("cast(decimal) -> float8")]
#[function("cast(float4) -> decimal")]
#[function("cast(float8) -> decimal")]
pub fn try_cast<T1, T2>(elem: T1) -> Result<T2>
where
    T1: TryInto<T2> + std::fmt::Debug + Copy,
{
    elem.try_into()
        .map_err(|_| ExprError::CastOutOfRange(std::any::type_name::<T2>()))
}

#[function("cast(boolean) -> int4")]
#[function("cast(int2) -> int4")]
#[function("cast(int2) -> int8")]
#[function("cast(int2) -> float4")]
#[function("cast(int2) -> float8")]
#[function("cast(int2) -> decimal")]
#[function("cast(int4) -> int8")]
#[function("cast(int4) -> float4")]
#[function("cast(int4) -> float8")]
#[function("cast(int4) -> decimal")]
#[function("cast(int8) -> float4")]
#[function("cast(int8) -> float8")]
#[function("cast(int8) -> decimal")]
#[function("cast(float4) -> float8")]
#[function("cast(date) -> timestamp")]
#[function("cast(time) -> interval")]
#[function("cast(varchar) -> varchar")]
#[function("cast(int256) -> float8")]
pub fn cast<T1, T2>(elem: T1) -> T2
where
    T1: Into<T2>,
{
    elem.into()
}

#[function("cast(varchar) -> boolean")]
pub fn str_to_bool(input: &str) -> Result<bool> {
    let trimmed_input = input.trim();
    if TRUE_BOOL_LITERALS
        .iter()
        .any(|s| s.eq_ignore_ascii_case(trimmed_input))
    {
        Ok(true)
    } else if FALSE_BOOL_LITERALS
        .iter()
        .any(|s| trimmed_input.eq_ignore_ascii_case(s))
    {
        Ok(false)
    } else {
        Err(ExprError::Parse("Invalid bool".into()))
    }
}

#[function("cast(int4) -> boolean")]
pub fn int4_to_bool(input: i32) -> Result<bool> {
    Ok(input != 0)
}

// For most of the types, cast them to varchar is similar to return their text format.
// So we use this function to cast type to varchar.
#[function("cast(*int) -> varchar")]
#[function("cast(decimal) -> varchar")]
#[function("cast(*float) -> varchar")]
#[function("cast(int256) -> varchar")]
#[function("cast(time) -> varchar")]
#[function("cast(date) -> varchar")]
#[function("cast(interval) -> varchar")]
#[function("cast(timestamp) -> varchar")]
#[function("cast(jsonb) -> varchar")]
#[function("cast(bytea) -> varchar")]
#[function("cast(list) -> varchar")]
pub fn general_to_text(elem: impl ToText, mut writer: &mut impl Write) {
    elem.write(&mut writer).unwrap();
}

#[function("cast(boolean) -> varchar")]
pub fn bool_to_varchar(input: bool, writer: &mut impl Write) {
    writer
        .write_str(if input { "true" } else { "false" })
        .unwrap();
}

/// `bool_out` is different from `general_to_string<bool>` to produce a single char. `PostgreSQL`
/// uses different variants of bool-to-string in different situations.
#[function("bool_out(boolean) -> varchar")]
pub fn bool_out(input: bool, writer: &mut impl Write) {
    writer.write_str(if input { "t" } else { "f" }).unwrap();
}

#[function("cast(varchar) -> bytea")]
pub fn str_to_bytea(elem: &str) -> Result<Box<[u8]>> {
    str_to_bytea_common(elem).map_err(|err| ExprError::Parse(err.into()))
}

/// A lite version of casting from string to target type. Used by frontend to handle types that have
/// to be created by casting.
///
/// For example, the user can input `1` or `true` directly, but they have to use
/// `'2022-01-01'::date`.
pub fn literal_parsing(
    t: &DataType,
    s: &str,
) -> std::result::Result<ScalarImpl, Option<ExprError>> {
    let scalar = match t {
        DataType::Boolean => str_to_bool(s)?.into(),
        DataType::Int16 => str_parse::<i16>(s)?.into(),
        DataType::Int32 => str_parse::<i32>(s)?.into(),
        DataType::Int64 => str_parse::<i64>(s)?.into(),
        DataType::Int256 => str_parse::<Int256>(s)?.into(),
        DataType::Serial => return Err(None),
        DataType::Decimal => str_parse::<Decimal>(s)?.into(),
        DataType::Float32 => str_parse::<F32>(s)?.into(),
        DataType::Float64 => str_parse::<F64>(s)?.into(),
        DataType::Varchar => s.into(),
        DataType::Date => str_to_date(s)?.into(),
        DataType::Timestamp => str_to_timestamp(s)?.into(),
        // We only handle the case with timezone here, and leave the implicit session timezone case
        // for later phase.
        DataType::Timestamptz => str_parse::<Timestamptz>(s)?.into(),
        DataType::Time => str_to_time(s)?.into(),
        DataType::Interval => str_parse::<Interval>(s)?.into(),
        // Not processing list or struct literal right now. Leave it for later phase (normal backend
        // evaluation).
        DataType::List { .. } => return Err(None),
        DataType::Struct(_) => return Err(None),
        DataType::Jsonb => return Err(None),
        DataType::Bytea => str_to_bytea(s)?.into(),
    };
    Ok(scalar)
}

// TODO(nanderstabel): optimize for multidimensional List. Depth can be given as a parameter to this
// function.
/// Takes a string input in the form of a comma-separated list enclosed in braces, and returns a
/// vector of strings containing the list items.
///
/// # Examples
/// - "{1, 2, 3}" => ["1", "2", "3"]
/// - "{1, {2, 3}}" => ["1", "{2, 3}"]
fn unnest(input: &str) -> Result<Vec<&str>> {
    let trimmed = input.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Err(ExprError::Parse("Input must be braced".into()));
    }
    let trimmed = &trimmed[1..trimmed.len() - 1];

    let mut items = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in trimmed.chars().enumerate() {
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            ',' if depth == 0 => {
                let item = trimmed[start..i].trim();
                items.push(item);
                start = i + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(ExprError::Parse("Unbalanced braces".into()));
    }
    let last = trimmed[start..].trim();
    if !last.is_empty() {
        items.push(last);
    }
    Ok(items)
}

#[function("cast(varchar) -> list")]
fn str_to_list(input: &str, ctx: &Context) -> Result<ListValue> {
    let cast = build_func(
        PbType::Cast,
        ctx.return_type.as_list().clone(),
        vec![InputRefExpression::new(DataType::Varchar, 0).boxed()],
    )
    .unwrap();
    let mut values = vec![];
    for item in unnest(input)? {
        let v = cast
            .eval_row(&OwnedRow::new(vec![Some(item.to_string().into())])) // TODO: optimize
            .now_or_never()
            .unwrap()?;
        values.push(v);
    }
    Ok(ListValue::new(values))
}

/// Cast array with `source_elem_type` into array with `target_elem_type` by casting each element.
#[function("cast(list) -> list")]
fn list_cast(input: ListRef<'_>, ctx: &Context) -> Result<ListValue> {
    let cast = build_func(
        PbType::Cast,
        ctx.return_type.as_list().clone(),
        vec![InputRefExpression::new(ctx.arg_types[0].as_list().clone(), 0).boxed()],
    )
    .unwrap();
    let elements = input.iter();
    let mut values = Vec::with_capacity(elements.len());
    for item in elements {
        let v = cast
            .eval_row(&OwnedRow::new(vec![item.map(|s| s.into_scalar_impl())])) // TODO: optimize
            .now_or_never()
            .unwrap()?;
        values.push(v);
    }
    Ok(ListValue::new(values))
}

/// Cast struct of `source_elem_type` to `target_elem_type` by casting each element.
#[function("cast(struct) -> struct")]
fn struct_cast(input: StructRef<'_>, ctx: &Context) -> Result<StructValue> {
    let fields = (input.iter_fields_ref())
        .zip_eq_fast(ctx.arg_types[0].as_struct().types())
        .zip_eq_fast(ctx.return_type.as_struct().types())
        .map(|((datum_ref, source_field_type), target_field_type)| {
            if source_field_type == target_field_type {
                return Ok(datum_ref.map(|scalar_ref| scalar_ref.into_scalar_impl()));
            }
            let cast = build_func(
                PbType::Cast,
                target_field_type.clone(),
                vec![InputRefExpression::new(source_field_type.clone(), 0).boxed()],
            )
            .unwrap();
            let value = match datum_ref {
                Some(scalar_ref) => cast
                    .eval_row(&OwnedRow::new(vec![Some(scalar_ref.into_scalar_impl())]))
                    .now_or_never()
                    .unwrap()?,
                None => None,
            };
            Ok(value) as Result<_>
        })
        .try_collect()?;
    Ok(StructValue::new(fields))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDateTime;
    use risingwave_common::types::{Scalar, StructType};

    use super::*;

    #[test]
    fn integer_cast_to_bool() {
        use super::*;
        assert!(int4_to_bool(32).unwrap());
        assert!(int4_to_bool(-32).unwrap());
        assert!(!int4_to_bool(0).unwrap());
    }

    #[test]
    fn number_to_string() {
        use super::*;

        macro_rules! test {
            ($fn:ident($value:expr), $right:literal) => {
                let mut writer = String::new();
                $fn($value, &mut writer);
                assert_eq!(writer, $right);
            };
        }

        test!(bool_to_varchar(true), "true");
        test!(bool_to_varchar(true), "true");
        test!(bool_to_varchar(false), "false");

        test!(general_to_text(32), "32");
        test!(general_to_text(-32), "-32");
        test!(general_to_text(i32::MIN), "-2147483648");
        test!(general_to_text(i32::MAX), "2147483647");

        test!(general_to_text(i16::MIN), "-32768");
        test!(general_to_text(i16::MAX), "32767");

        test!(general_to_text(i64::MIN), "-9223372036854775808");
        test!(general_to_text(i64::MAX), "9223372036854775807");

        test!(general_to_text(F64::from(32.12)), "32.12");
        test!(general_to_text(F64::from(-32.14)), "-32.14");

        test!(general_to_text(F32::from(32.12_f32)), "32.12");
        test!(general_to_text(F32::from(-32.14_f32)), "-32.14");

        test!(general_to_text(Decimal::try_from(1.222).unwrap()), "1.222");

        test!(general_to_text(Decimal::NaN), "NaN");
    }

    #[test]
    fn temporal_cast() {
        assert_eq!(
            timestamp_to_date(str_to_timestamp("1999-01-08 04:02").unwrap()),
            str_to_date("1999-01-08").unwrap(),
        );
        assert_eq!(
            timestamp_to_time(str_to_timestamp("1999-01-08 04:02").unwrap()),
            str_to_time("04:02").unwrap(),
        );
        assert_eq!(
            interval_to_time(Interval::from_month_day_usec(1, 2, 61000003)),
            str_to_time("00:01:01.000003").unwrap(),
        );
        assert_eq!(
            interval_to_time(Interval::from_month_day_usec(0, 0, -61000003)),
            str_to_time("23:58:58.999997").unwrap(),
        );
    }

    #[test]
    fn test_unnest() {
        assert_eq!(unnest("{ }").unwrap(), vec![] as Vec<String>);
        assert_eq!(
            unnest("{1, 2, 3}").unwrap(),
            vec!["1".to_string(), "2".to_string(), "3".to_string()]
        );
        assert_eq!(
            unnest("{{1, 2, 3}, {4, 5, 6}}").unwrap(),
            vec!["{1, 2, 3}".to_string(), "{4, 5, 6}".to_string()]
        );
        assert_eq!(
            unnest("{{{1, 2, 3}}, {{4, 5, 6}}}").unwrap(),
            vec!["{{1, 2, 3}}".to_string(), "{{4, 5, 6}}".to_string()]
        );
        assert_eq!(
            unnest("{{{1, 2, 3}, {4, 5, 6}}}").unwrap(),
            vec!["{{1, 2, 3}, {4, 5, 6}}".to_string()]
        );
        assert_eq!(
            unnest("{{{aa, bb, cc}, {dd, ee, ff}}}").unwrap(),
            vec!["{{aa, bb, cc}, {dd, ee, ff}}".to_string()]
        );
    }

    #[test]
    fn test_str_to_list() {
        // Empty List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[]").unwrap(),
        };
        assert_eq!(str_to_list("{}", &ctx).unwrap(), ListValue::new(vec![]));

        let list123 = ListValue::new(vec![
            Some(1.to_scalar_value()),
            Some(2.to_scalar_value()),
            Some(3.to_scalar_value()),
        ]);

        // Single List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[]").unwrap(),
        };
        assert_eq!(str_to_list("{1, 2, 3}", &ctx).unwrap(), list123);

        // Nested List
        let nested_list123 = ListValue::new(vec![Some(ScalarImpl::List(list123))]);
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[][]").unwrap(),
        };
        assert_eq!(str_to_list("{{1, 2, 3}}", &ctx).unwrap(), nested_list123);

        let nested_list445566 = ListValue::new(vec![Some(ScalarImpl::List(ListValue::new(vec![
            Some(44.to_scalar_value()),
            Some(55.to_scalar_value()),
            Some(66.to_scalar_value()),
        ])))]);

        let double_nested_list123_445566 = ListValue::new(vec![
            Some(ScalarImpl::List(nested_list123.clone())),
            Some(ScalarImpl::List(nested_list445566.clone())),
        ]);

        // Double nested List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[][][]").unwrap(),
        };
        assert_eq!(
            str_to_list("{{{1, 2, 3}}, {{44, 55, 66}}}", &ctx).unwrap(),
            double_nested_list123_445566
        );

        // Cast previous double nested lists to double nested varchar lists
        let ctx = Context {
            arg_types: vec![DataType::from_str("int[][]").unwrap()],
            return_type: DataType::from_str("varchar[][]").unwrap(),
        };
        let double_nested_varchar_list123_445566 = ListValue::new(vec![
            Some(ScalarImpl::List(
                list_cast(
                    ListRef::ValueRef {
                        val: &nested_list123,
                    },
                    &ctx,
                )
                .unwrap(),
            )),
            Some(ScalarImpl::List(
                list_cast(
                    ListRef::ValueRef {
                        val: &nested_list445566,
                    },
                    &ctx,
                )
                .unwrap(),
            )),
        ]);

        // Double nested Varchar List
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("varchar[][][]").unwrap(),
        };
        assert_eq!(
            str_to_list("{{{1, 2, 3}}, {{44, 55, 66}}}", &ctx).unwrap(),
            double_nested_varchar_list123_445566
        );
    }

    #[test]
    fn test_invalid_str_to_list() {
        // Unbalanced input
        let ctx = Context {
            arg_types: vec![DataType::Varchar],
            return_type: DataType::from_str("int[]").unwrap(),
        };
        assert!(str_to_list("{{}", &ctx).is_err());
        assert!(str_to_list("{}}", &ctx).is_err());
        assert!(str_to_list("{{1, 2, 3}, {4, 5, 6}", &ctx).is_err());
        assert!(str_to_list("{{1, 2, 3}, 4, 5, 6}}", &ctx).is_err());
    }

    #[test]
    fn test_struct_cast() {
        let ctx = Context {
            arg_types: vec![DataType::Struct(StructType::new(vec![
                ("a", DataType::Varchar),
                ("b", DataType::Float32),
            ]))],
            return_type: DataType::Struct(StructType::new(vec![
                ("a", DataType::Int32),
                ("b", DataType::Int32),
            ])),
        };
        assert_eq!(
            struct_cast(
                StructValue::new(vec![
                    Some("1".into()),
                    Some(F32::from(0.0).to_scalar_value()),
                ])
                .as_scalar_ref(),
                &ctx,
            )
            .unwrap(),
            StructValue::new(vec![
                Some(1i32.to_scalar_value()),
                Some(0i32.to_scalar_value()),
            ])
        );
    }

    #[test]
    fn test_str_to_timestamp() {
        let str1 = "0001-11-15 07:35:40.999999";
        let timestamp1 = str_to_timestamp(str1).unwrap();
        assert_eq!(timestamp1.0.timestamp_micros(), -62108094259000001);

        let str2 = "1969-12-31 23:59:59.999999";
        let timestamp2 = str_to_timestamp(str2).unwrap();
        assert_eq!(timestamp2.0.timestamp_micros(), -1);
    }

    #[test]
    fn test_timestamp() {
        assert_eq!(
            try_cast::<_, Timestamp>(Date::from_ymd_uncheck(1994, 1, 1)).unwrap(),
            Timestamp::new(
                NaiveDateTime::parse_from_str("1994-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        )
    }
}
