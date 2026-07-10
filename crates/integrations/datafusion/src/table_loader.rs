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

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result as DFResult};
use paimon::catalog::{Catalog, Identifier};
use paimon::table::Table;

use crate::error::to_datafusion_error;

pub(crate) async fn load_table_for_read(
    catalog: &Arc<dyn Catalog>,
    identifier: &Identifier,
) -> DFResult<(Table, Identifier, Option<String>)> {
    let parsed = identifier
        .parsed_object_name()
        .map_err(to_datafusion_error)?;
    let base_identifier = Identifier::new(
        identifier.database().to_string(),
        parsed.table().to_string(),
    );
    let mut table = catalog
        .get_table(&base_identifier)
        .await
        .map_err(to_datafusion_error)?;
    let system_table = parsed.system_table().map(str::to_string);
    if let Some(branch) = parsed.branch() {
        let is_branches_table = system_table
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case("branches"));
        if is_branches_table {
            return Ok((table, base_identifier, system_table));
        }
        table = table
            .copy_with_branch(branch)
            .await
            .map_err(to_datafusion_error)?;
    }
    Ok((table, base_identifier, system_table))
}

pub(crate) async fn load_data_table_for_read(
    catalog: &Arc<dyn Catalog>,
    identifier: &Identifier,
    caller: &str,
) -> DFResult<Table> {
    let (table, _, system_table) = load_table_for_read(catalog, identifier).await?;
    if system_table.is_some() {
        return Err(DataFusionError::Plan(format!(
            "{caller} requires a data table"
        )));
    }
    Ok(table)
}
