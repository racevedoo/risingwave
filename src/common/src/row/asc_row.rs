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

use std::ops::Deref;

use super::{OwnedRow, Row};
use crate::util::sort_util::{compare_rows, partial_compare_rows, OrderType};

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct AscRow<R: Row>(R);

impl<R: Row> AscRow<R> {
    pub fn new(row: R) -> Self {
        Self(row)
    }

    pub fn into_inner(self) -> R {
        self.0
    }
}

impl<R: Row> From<R> for AscRow<R> {
    fn from(row: R) -> Self {
        Self::new(row)
    }
}

impl<R: Row> Deref for AscRow<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<R: Row> Row for AscRow<R> {
    type Iter<'a> = R::Iter<'a> where R: 'a;

    deref_forward_row! {}

    fn into_owned_row(self) -> OwnedRow {
        self.0.into_owned_row()
    }
}

impl<R: Row> PartialOrd for AscRow<R> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        partial_compare_rows(&self.0, &other.0, &vec![OrderType::ascending(); self.len()])
    }
}

impl<R: Row> Ord for AscRow<R> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        compare_rows(&self.0, &other.0, &vec![OrderType::ascending(); self.len()])
    }
}
