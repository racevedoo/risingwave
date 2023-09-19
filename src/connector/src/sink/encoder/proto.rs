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

use bytes::Bytes;
use prost::Message;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value};
use risingwave_common::array::{ArrayError, ArrayResult};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqDebug;

use super::{Result, RowEncoder, SerTo};
use crate::sink::SinkError;

pub struct ProtoEncoder<'a> {
    schema: &'a Schema,
    col_indices: Option<&'a [usize]>,
    descriptor: MessageDescriptor,
}

impl<'a> ProtoEncoder<'a> {
    pub fn new(
        schema: &'a Schema,
        col_indices: Option<&'a [usize]>,
        descriptor: MessageDescriptor,
    ) -> Result<Self> {
        for idx in col_indices.unwrap() {
            let f = &schema[*idx];
            let Some(expected) = descriptor.get_field_by_name(&f.name) else {
                return Err(SinkError::JsonParse(format!(
                    "field {} not in proto",
                    f.name,
                )));
            };
            if !is_valid(&f.data_type, &expected) {
                return Err(SinkError::JsonParse(format!(
                    "field {}:{} cannot output as proto {:?}",
                    f.name, f.data_type, expected
                )));
            }
        }

        Ok(Self {
            schema,
            col_indices,
            descriptor,
        })
    }
}

impl<'a> RowEncoder for ProtoEncoder<'a> {
    type Output = DynamicMessage;

    fn schema(&self) -> &Schema {
        self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        self.col_indices
    }

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> Result<Self::Output> {
        let mut message = DynamicMessage::new(self.descriptor.clone());
        for idx in col_indices {
            let field = &self.schema[idx];
            let key = &field.name;
            let expected = self.descriptor.get_field_by_name(key).unwrap();
            if let Some(scalar) = row.datum_at(idx) {
                let value = scalar_to_proto(field, scalar, &expected)
                    .map_err(|e| SinkError::JsonParse(e.to_string()))?;
                message.set_field(&expected, value);
            }
        }
        Ok(message)
    }
}

impl SerTo<Vec<u8>> for DynamicMessage {
    fn ser_to(self) -> Result<Vec<u8>> {
        Ok(self.encode_to_vec())
    }
}

fn is_valid(_data_type: &DataType, _expected: &FieldDescriptor) -> bool {
    false
}

fn scalar_to_proto(
    field: &Field,
    scalar_ref: ScalarRefImpl<'_>,
    expected: &FieldDescriptor,
) -> ArrayResult<Value> {
    let data_type = field.data_type();

    tracing::debug!("scalar_to_proto: {:?}, {:?}", data_type, scalar_ref);

    let err = || {
        Err(ArrayError::internal(
        format!("scalar_to_proto: unsupported data type: field name: {:?}, logical type: {:?}, physical type: {:?}", field.name, data_type, scalar_ref),
    ))
    };

    let value = match &data_type {
        // Group A: perfect match between RisingWave types and ProtoBuf types
        DataType::Boolean => match expected.kind() {
            Kind::Bool if !expected.is_list() => Value::Bool(scalar_ref.into_bool()),
            _ => return err(),
        },
        DataType::Varchar => match expected.kind() {
            Kind::String if !expected.is_list() => Value::String(scalar_ref.into_utf8().into()),
            _ => return err(),
        },
        DataType::Bytea => match expected.kind() {
            Kind::Bytes if !expected.is_list() => {
                Value::Bytes(Bytes::copy_from_slice(scalar_ref.into_bytea()))
            }
            _ => return err(),
        },
        DataType::Float32 => match expected.kind() {
            Kind::Float if !expected.is_list() => Value::F32(scalar_ref.into_float32().into()),
            _ => return err(),
        },
        DataType::Float64 => match expected.kind() {
            Kind::Double if !expected.is_list() => Value::F64(scalar_ref.into_float64().into()),
            _ => return err(),
        },
        DataType::Int32 => match expected.kind() {
            Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 if !expected.is_list() => {
                Value::I32(scalar_ref.into_int32())
            }
            _ => return err(),
        },
        DataType::Int64 => match expected.kind() {
            Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 if !expected.is_list() => {
                Value::I64(scalar_ref.into_int64())
            }
            _ => return err(),
        },
        DataType::Struct(t_rw) => match expected.kind() {
            Kind::Message(t_pb) if !expected.is_list() => {
                let d = scalar_ref.into_struct();
                let mut message = DynamicMessage::new(t_pb.clone());
                for ((name, ft), fd) in t_rw.iter().zip_eq_debug(d.iter_fields_ref()) {
                    let fe = t_pb.get_field_by_name(name).unwrap();
                    if let Some(fs) = fd {
                        let fv = scalar_to_proto(&Field::with_name(ft.clone(), name), fs, &fe)?;
                        message.set_field(&fe, fv);
                    }
                }
                Value::Message(message)
            }
            _ => return err(),
        },
        DataType::List(t_rw) => match expected.is_list() {
            true => {
                let d = scalar_ref.into_list();
                let vs = d
                    .iter()
                    .map(|d| {
                        scalar_to_proto(
                            &Field::with_name((**t_rw).clone(), field.name.clone()),
                            d.unwrap(),
                            expected,
                        )
                        .unwrap()
                    })
                    .collect();
                Value::List(vs)
            }
            false => return err(),
        },
        // Group B: match between RisingWave types and ProtoBuf Well-Known types
        DataType::Timestamptz => todo!(),
        DataType::Jsonb => todo!(),
        // Group C: experimental
        DataType::Int16 => todo!(),
        DataType::Date | DataType::Timestamp | DataType::Time | DataType::Decimal => todo!(),
        DataType::Interval => todo!(),
        // Group D: unsupported
        DataType::Serial | DataType::Int256 => {
            return err();
        }
    };

    Ok(value)
}

#[cfg(test)]
mod tests {

    use risingwave_common::types::{DataType, Interval, ScalarImpl, Time, Timestamp};

    use super::*;

    fn mock_pb() -> FieldDescriptor {
        todo!()
    }

    fn any_value(_: impl std::fmt::Display) -> Value {
        todo!()
    }

    #[test]
    fn test_to_proto_basic_type() {
        let mock_field = Field {
            data_type: DataType::Boolean,
            name: Default::default(),
            sub_fields: Default::default(),
            type_name: Default::default(),
        };
        let expected = mock_pb();
        let boolean_value = scalar_to_proto(
            &Field {
                data_type: DataType::Boolean,
                ..mock_field.clone()
            },
            ScalarImpl::Bool(false).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(boolean_value, Value::Bool(false));

        let int32_value = scalar_to_proto(
            &Field {
                data_type: DataType::Int32,
                ..mock_field.clone()
            },
            ScalarImpl::Int32(16).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(int32_value, Value::I32(16));

        let int64_value = scalar_to_proto(
            &Field {
                data_type: DataType::Int64,
                ..mock_field.clone()
            },
            ScalarImpl::Int64(std::i64::MAX).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(int64_value, Value::I64(i64::MAX));

        // https://github.com/debezium/debezium/blob/main/debezium-core/src/main/java/io/debezium/time/ZonedTimestamp.java
        let tstz_inner = "2018-01-26T18:30:09.453Z".parse().unwrap();
        let tstz_value = scalar_to_proto(
            &Field {
                data_type: DataType::Timestamptz,
                ..mock_field.clone()
            },
            ScalarImpl::Timestamptz(tstz_inner).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(tstz_value, any_value("2018-01-26 18:30:09.453000"));

        let ts_value = scalar_to_proto(
            &Field {
                data_type: DataType::Timestamp,
                ..mock_field.clone()
            },
            ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(1000, 0)).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(ts_value, any_value(1000 * 1000));

        let ts_value = scalar_to_proto(
            &Field {
                data_type: DataType::Timestamp,
                ..mock_field.clone()
            },
            ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(1000, 0)).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(
            ts_value,
            any_value("1970-01-01 00:16:40.000000".to_string())
        );

        // Represents the number of microseconds past midnigh, io.debezium.time.Time
        let time_value = scalar_to_proto(
            &Field {
                data_type: DataType::Time,
                ..mock_field.clone()
            },
            ScalarImpl::Time(Time::from_num_seconds_from_midnight_uncheck(1000, 0))
                .as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(time_value, any_value(1000 * 1000));

        let interval_value = scalar_to_proto(
            &Field {
                data_type: DataType::Interval,
                ..mock_field
            },
            ScalarImpl::Interval(Interval::from_month_day_usec(13, 2, 1000000))
                .as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(interval_value, any_value("P1Y1M2DT0H0M1S"));
    }
}
