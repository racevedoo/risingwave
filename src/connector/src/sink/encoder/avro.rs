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

use apache_avro::schema::Schema as AvroSchema;
use apache_avro::types::{Record, Value};
use apache_avro::Writer;
use risingwave_common::array::{ArrayError, ArrayResult};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqDebug;

use super::{Result, RowEncoder, SerTo};
use crate::sink::SinkError;

pub struct AvroEncoder<'a> {
    schema: &'a Schema,
    col_indices: Option<&'a [usize]>,
    avro_schema: &'a AvroSchema,
}

impl<'a> AvroEncoder<'a> {
    pub fn new(
        schema: &'a Schema,
        col_indices: Option<&'a [usize]>,
        avro_schema: &'a AvroSchema,
    ) -> Result<Self> {
        let AvroSchema::Record { fields, lookup, .. } = avro_schema else {
            return Err(SinkError::JsonParse(format!(
                "not an avro record: {:?}",
                avro_schema
            )));
        };
        for idx in col_indices.unwrap() {
            let f = &schema[*idx];
            let Some(expected) = lookup.get(&f.name).map(|i| &fields[*i]) else {
                return Err(SinkError::JsonParse(format!(
                    "field {} not in avro",
                    f.name,
                )));
            };
            if !is_valid(&f.data_type, &expected.schema) {
                return Err(SinkError::JsonParse(format!(
                    "field {}:{} cannot output as avro {:?}",
                    f.name, f.data_type, expected
                )));
            }
        }

        Ok(Self {
            schema,
            col_indices,
            avro_schema,
        })
    }
}

impl<'a> RowEncoder for AvroEncoder<'a> {
    type Output = (Record<'a>, &'a AvroSchema);

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
        let mut record = Record::new(self.avro_schema).unwrap();
        let AvroSchema::Record { fields, lookup, .. } = self.avro_schema else {
            unreachable!()
        };

        for idx in col_indices {
            let field = &self.schema[idx];
            let key = &field.name;
            let expected = &fields[lookup[key]];
            if let Some(scalar) = row.datum_at(idx) {
                let value = scalar_to_avro(field, scalar, &expected.schema)
                    .map_err(|e| SinkError::JsonParse(e.to_string()))?;
                record.put(key, value);
            }
        }
        Ok((record, self.avro_schema))
    }
}

impl<'a> SerTo<Vec<u8>> for (Record<'a>, &'a AvroSchema) {
    fn ser_to(self) -> Result<Vec<u8>> {
        let mut w = Writer::new(self.1, Vec::new());
        w.append(self.0).unwrap();
        Ok(w.into_inner().unwrap())
    }
}

fn is_valid(_data_type: &DataType, _expected: &AvroSchema) -> bool {
    false
}

fn scalar_to_avro(
    field: &Field,
    scalar_ref: ScalarRefImpl<'_>,
    expected: &AvroSchema,
) -> ArrayResult<Value> {
    let data_type = field.data_type();

    tracing::debug!("scalar_to_avro: {:?}, {:?}", data_type, scalar_ref);

    let err = || {
        Err(ArrayError::internal(
        format!("scalar_to_avro: unsupported data type: field name: {:?}, logical type: {:?}, physical type: {:?}", field.name, data_type, scalar_ref),
    ))
    };

    let value = match &data_type {
        // Group A: perfect match between RisingWave types and Avro types
        DataType::Boolean => match expected {
            AvroSchema::Boolean => Value::Boolean(scalar_ref.into_bool()),
            _ => return err(),
        },
        DataType::Varchar => match expected {
            AvroSchema::String => Value::String(scalar_ref.into_utf8().into()),
            _ => return err(),
        },
        DataType::Bytea => match expected {
            AvroSchema::Bytes => Value::Bytes(scalar_ref.into_bytea().into()),
            _ => return err(),
        },
        DataType::Float32 => match expected {
            AvroSchema::Float => Value::Float(scalar_ref.into_float32().into()),
            _ => return err(),
        },
        DataType::Float64 => match expected {
            AvroSchema::Double => Value::Double(scalar_ref.into_float64().into()),
            _ => return err(),
        },
        DataType::Int32 => match expected {
            AvroSchema::Int => Value::Int(scalar_ref.into_int32()),
            _ => return err(),
        },
        DataType::Int64 => match expected {
            AvroSchema::Long => Value::Long(scalar_ref.into_int64()),
            _ => return err(),
        },
        DataType::Struct(t_rw) => match expected {
            AvroSchema::Record { fields, lookup, .. } => {
                let d = scalar_ref.into_struct();
                let mut record = Record::new(expected).unwrap();
                for ((name, ft), fd) in t_rw.iter().zip_eq_debug(d.iter_fields_ref()) {
                    let fe = &fields[lookup[name]];
                    if let Some(fs) = fd {
                        let fv =
                            scalar_to_avro(&Field::with_name(ft.clone(), name), fs, &fe.schema)?;
                        record.put(name, fv);
                    }
                }
                record.into()
            }
            _ => return err(),
        },
        DataType::List(t_rw) => match expected {
            AvroSchema::Array(elem) => {
                let d = scalar_ref.into_list();
                let vs = d
                    .iter()
                    .map(|d| {
                        scalar_to_avro(
                            &Field::with_name((**t_rw).clone(), field.name.clone()),
                            d.unwrap(),
                            elem,
                        )
                        .unwrap()
                    })
                    .collect();
                Value::Array(vs)
            }
            _ => return err(),
        },
        // Group B: match between RisingWave types and Avro logical types
        DataType::Timestamptz => match expected {
            AvroSchema::TimestampMicros => {
                Value::TimestampMicros(scalar_ref.into_timestamptz().timestamp_micros())
            }
            AvroSchema::TimestampMillis => {
                Value::TimestampMillis(scalar_ref.into_timestamptz().timestamp_millis())
            }
            _ => return err(),
        },
        DataType::Timestamp => todo!(),
        DataType::Date => todo!(),
        DataType::Time => todo!(),
        DataType::Interval => match expected {
            AvroSchema::Duration => {
                use apache_avro::{Days, Duration, Millis, Months};
                let iv = scalar_ref.into_interval();
                Value::Duration(Duration::new(
                    Months::new(iv.months().try_into().unwrap()),
                    Days::new(iv.days().try_into().unwrap()),
                    Millis::new((iv.usecs() / 1000).try_into().unwrap()),
                ))
            }
            _ => return err(),
        },
        // Group C: experimental
        DataType::Int16 => todo!(),
        DataType::Decimal => todo!(),
        DataType::Jsonb => todo!(),
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

    fn mock_avro() -> AvroSchema {
        todo!()
    }

    fn any_value(_: impl std::fmt::Display) -> Value {
        todo!()
    }

    #[test]
    fn test_to_avro_basic_type() {
        let mock_field = Field {
            data_type: DataType::Boolean,
            name: Default::default(),
            sub_fields: Default::default(),
            type_name: Default::default(),
        };
        let expected = mock_avro();
        let boolean_value = scalar_to_avro(
            &Field {
                data_type: DataType::Boolean,
                ..mock_field.clone()
            },
            ScalarImpl::Bool(false).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(boolean_value, Value::Boolean(false));

        let int32_value = scalar_to_avro(
            &Field {
                data_type: DataType::Int32,
                ..mock_field.clone()
            },
            ScalarImpl::Int32(16).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(int32_value, Value::Int(16));

        let int64_value = scalar_to_avro(
            &Field {
                data_type: DataType::Int64,
                ..mock_field.clone()
            },
            ScalarImpl::Int64(std::i64::MAX).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(int64_value, Value::Long(i64::MAX));

        // https://github.com/debezium/debezium/blob/main/debezium-core/src/main/java/io/debezium/time/ZonedTimestamp.java
        let tstz_inner = "2018-01-26T18:30:09.453Z".parse().unwrap();
        let tstz_value = scalar_to_avro(
            &Field {
                data_type: DataType::Timestamptz,
                ..mock_field.clone()
            },
            ScalarImpl::Timestamptz(tstz_inner).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(tstz_value, any_value("2018-01-26 18:30:09.453000"));

        let ts_value = scalar_to_avro(
            &Field {
                data_type: DataType::Timestamp,
                ..mock_field.clone()
            },
            ScalarImpl::Timestamp(Timestamp::from_timestamp_uncheck(1000, 0)).as_scalar_ref_impl(),
            &expected,
        )
        .unwrap();
        assert_eq!(ts_value, any_value(1000 * 1000));

        let ts_value = scalar_to_avro(
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
        let time_value = scalar_to_avro(
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

        let interval_value = scalar_to_avro(
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
