// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Paimon system tables (`<table>$<name>`) as DataFusion table providers.
//!
//! Mirrors Java [SystemTableLoader](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/table/system/SystemTableLoader.java):
//! `TABLES` maps each system-table name to its builder function.

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use paimon::catalog::{Catalog, Identifier};
use paimon::table::Table;

use crate::error::to_datafusion_error;

mod options;

type Builder = fn(Table) -> DFResult<Arc<dyn TableProvider>>;

const TABLES: &[(&str, Builder)] = &[("options", options::build)];

/// Returns true if `name` is a recognised Paimon system table suffix.
pub(crate) fn is_registered(name: &str) -> bool {
    TABLES.iter().any(|(n, _)| name.eq_ignore_ascii_case(n))
}

/// Wraps an already-loaded base table as the system table `name`.
fn wrap_to_system_table(name: &str, base_table: Table) -> Option<DFResult<Arc<dyn TableProvider>>> {
    TABLES
        .iter()
        .find(|(n, _)| name.eq_ignore_ascii_case(n))
        .map(|(_, build)| build(base_table))
}

/// Loads `<base>$<system_name>` from the catalog and wraps it as a system
/// table provider.
///
/// - Unknown `system_name` → `Ok(None)` (DataFusion reports "table not found")
/// - Base table missing    → `Err(Plan)` so users can distinguish it from an
///   unknown system name
pub(crate) async fn load(
    catalog: Arc<dyn Catalog>,
    database: String,
    base: String,
    system_name: String,
) -> DFResult<Option<Arc<dyn TableProvider>>> {
    if !is_registered(&system_name) {
        return Ok(None);
    }
    let identifier = Identifier::new(database, base.clone());
    match catalog.get_table(&identifier).await {
        Ok(table) => wrap_to_system_table(&system_name, table)
            .expect("is_registered guarantees a builder")
            .map(Some),
        Err(paimon::Error::TableNotExist { .. }) => Err(DataFusionError::Plan(format!(
            "Cannot read system table `${system_name}`: \
             base table `{base}` does not exist"
        ))),
        Err(e) => Err(to_datafusion_error(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::is_registered;

    #[test]
    fn is_registered_is_case_insensitive() {
        assert!(is_registered("options"));
        assert!(is_registered("Options"));
        assert!(is_registered("OPTIONS"));
        assert!(!is_registered("nonsense"));
    }
}
