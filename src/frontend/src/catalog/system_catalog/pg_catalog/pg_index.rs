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

use std::convert::Into;
use std::sync::LazyLock;

use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::{BuiltinView, SystemCatalogColumnsDef};

pub const PG_INDEX_TABLE_NAME: &str = "pg_index";
pub static PG_INDEX_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> = LazyLock::new(|| {
    vec![
        (DataType::Int32, "indexrelid"),
        (DataType::Int32, "indrelid"),
        (DataType::Int16, "indnatts"),
        // We return false as default to indicate that this is NOT a unique index
        (DataType::Boolean, "indisunique"),
        (DataType::List(Box::new(DataType::Int16)), "indkey"),
        (DataType::List(Box::new(DataType::Int16)), "indoption"),
        // None. We don't have `pg_node_tree` type yet, so we use `text` instead.
        (DataType::Varchar, "indexprs"),
        // None. We don't have `pg_node_tree` type yet, so we use `text` instead.
        (DataType::Varchar, "indpred"),
        // TODO: we return false as the default value.
        (DataType::Boolean, "indisprimary"),
    ]
});

/// The catalog `pg_index` contains part of the information about indexes.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-index.html`]
pub static PG_INDEX: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: PG_INDEX_TABLE_NAME,
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &PG_INDEX_COLUMNS,
    sql: "SELECT id AS indexrelid, \
                primary_table_id AS indrelid, \
                ARRAY_LENGTH(original_column_ids)::smallint AS indnatts, \
                false AS indisunique, \
                original_column_ids AS indkey, \
                ARRAY[]::smallint[] as indoption, \
                NULL AS indexprs, \
                NULL AS indpred, \
                FALSE AS indisprimary \
            FROM rw_catalog.rw_indexes"
        .into(),
});
