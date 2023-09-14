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

#![allow(non_snake_case)] // for `ctor` generated code
#![feature(let_chains)]
#![feature(assert_matches)]
#![feature(lint_reasons)]
#![feature(iterator_try_collect)]
#![feature(exclusive_range_pattern)]
#![feature(lazy_cell)]
#![feature(round_ties_even)]
#![feature(generators)]
#![feature(test)]
#![feature(arc_unwrap_or_clone)]

pub mod aggregate;
mod error;
pub mod expr;
pub mod function;
pub mod scalar;
pub mod sig;
pub mod table_function;
pub mod window_function;

pub use error::{ExprError, Result};
use risingwave_common::{bail, ensure};
pub use risingwave_expr_macro::*;
