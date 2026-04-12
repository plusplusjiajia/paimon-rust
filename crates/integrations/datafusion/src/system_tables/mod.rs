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
//! a single table maps each system-table name to a builder function. Add a new
//! system table by dropping a file under this module and appending one entry to
//! `TABLES`.

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use paimon::table::Table;

mod options;

type Builder = fn(Table) -> DFResult<Arc<dyn TableProvider>>;

const TABLES: &[(&str, Builder)] = &[("options", options::build)];

/// Returns true if `name` is a recognised Paimon system table suffix.
pub(crate) fn is_registered(name: &str) -> bool {
    TABLES.iter().any(|(n, _)| name.eq_ignore_ascii_case(n))
}

/// Builds a system table provider for `name`, or `None` if unrecognised.
pub(crate) fn build(name: &str, table: Table) -> Option<DFResult<Arc<dyn TableProvider>>> {
    TABLES
        .iter()
        .find(|(n, _)| name.eq_ignore_ascii_case(n))
        .map(|(_, build)| build(table))
}
