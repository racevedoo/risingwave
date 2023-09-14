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

use risingwave_common::types::DataType;
use risingwave_expr_macro::function;

#[function("format_type(int4, int4) -> varchar")]
pub fn format_type(oid: Option<i32>, _typemod: Option<i32>) -> Option<Box<str>> {
    // since we don't support type modifier, ignore it.
    oid.map(|i| {
        DataType::from_oid(i)
            .map(|dt| format!("{}", dt).into_boxed_str())
            .unwrap_or("???".into())
    })
}
