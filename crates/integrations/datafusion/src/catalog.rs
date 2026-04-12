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

//! Paimon catalog integration for DataFusion.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use paimon::catalog::{Catalog, Identifier, SYSTEM_BRANCH_PREFIX, SYSTEM_TABLE_SPLITTER};

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::system_tables;
use crate::table::PaimonTableProvider;

/// Parse a Paimon object name into `(base_table, optional system_table_name)`.
///
/// Mirrors Java [Identifier.splitObjectName](https://github.com/apache/paimon/blob/release-1.3/paimon-api/src/main/java/org/apache/paimon/catalog/Identifier.java).
///
/// - `t` → `("t", None)`
/// - `t$options` → `("t", Some("options"))`
/// - `t$branch_main` → `("t", None)` (branch reference, not a system table)
/// - `t$branch_main$options` → `("t", Some("options"))` (branch + system table)
fn split_object_name(name: &str) -> (&str, Option<&str>) {
    let mut parts = name.splitn(3, SYSTEM_TABLE_SPLITTER);
    let base = parts.next().unwrap_or(name);
    match (parts.next(), parts.next()) {
        (None, _) => (base, None),
        (Some(second), None) => {
            if second.starts_with(SYSTEM_BRANCH_PREFIX) {
                (base, None)
            } else {
                (base, Some(second))
            }
        }
        (Some(second), Some(third)) => {
            if second.starts_with(SYSTEM_BRANCH_PREFIX) {
                (base, Some(third))
            } else {
                // `$` is legal in table names, so `t$foo$bar` falls through as
                // plain `t` and errors later as "table not found".
                (base, None)
            }
        }
    }
}

/// Provides an interface to manage and access multiple schemas (databases)
/// within a Paimon [`Catalog`].
///
/// This provider uses lazy loading - databases and tables are fetched
/// on-demand from the catalog, ensuring data is always fresh.
pub struct PaimonCatalogProvider {
    /// Reference to the Paimon catalog.
    catalog: Arc<dyn Catalog>,
}

impl Debug for PaimonCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonCatalogProvider").finish()
    }
}

impl PaimonCatalogProvider {
    /// Creates a new [`PaimonCatalogProvider`].
    ///
    /// All data is loaded lazily when accessed.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        PaimonCatalogProvider { catalog }
    }
}

impl CatalogProvider for PaimonCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let catalog = Arc::clone(&self.catalog);
        block_on_with_runtime(
            async move { catalog.list_databases().await.unwrap_or_default() },
            "paimon catalog access thread panicked",
        )
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let catalog = Arc::clone(&self.catalog);
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                match catalog.get_database(&name).await {
                    Ok(_) => Some(
                        Arc::new(PaimonSchemaProvider::new(Arc::clone(&catalog), name))
                            as Arc<dyn SchemaProvider>,
                    ),
                    Err(paimon::Error::DatabaseNotExist { .. }) => None,
                    Err(_) => None,
                }
            },
            "paimon catalog access thread panicked",
        )
    }
}

/// Represents a [`SchemaProvider`] for the Paimon [`Catalog`], managing
/// access to table providers within a specific database.
///
/// Tables are loaded lazily when accessed via the `table()` method.
pub struct PaimonSchemaProvider {
    /// Reference to the Paimon catalog.
    catalog: Arc<dyn Catalog>,
    /// Database name this schema represents.
    database: String,
}

impl Debug for PaimonSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PaimonSchemaProvider")
            .field("database", &self.database)
            .finish()
    }
}

impl PaimonSchemaProvider {
    /// Creates a new [`PaimonSchemaProvider`] for the given database.
    pub fn new(catalog: Arc<dyn Catalog>, database: String) -> Self {
        PaimonSchemaProvider { catalog, database }
    }

    /// Resolves `<base>$<system_name>` into a system table provider.
    ///
    /// Unknown system names return `Ok(None)` (DataFusion reports "table not
    /// found"). When the system name is registered but the base table is
    /// missing, an explicit error is returned so users can tell the two cases
    /// apart in error messages.
    async fn load_system_table(
        &self,
        base: &str,
        system_name: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if !system_tables::is_registered(system_name) {
            return Ok(None);
        }

        let catalog = Arc::clone(&self.catalog);
        let database = self.database.clone();
        let base_owned = base.to_string();
        let system_name_owned = system_name.to_string();
        await_with_runtime(async move {
            let identifier = Identifier::new(database, base_owned.clone());
            match catalog.get_table(&identifier).await {
                Ok(table) => system_tables::build(&system_name_owned, table)
                    .expect("is_registered guarantees a builder")
                    .map(Some),
                Err(paimon::Error::TableNotExist { .. }) => {
                    Err(datafusion::error::DataFusionError::Plan(format!(
                        "Cannot read system table `${system_name_owned}`: \
                         base table `{base_owned}` does not exist"
                    )))
                }
                Err(e) => Err(to_datafusion_error(e)),
            }
        })
        .await
    }
}

#[async_trait]
impl SchemaProvider for PaimonSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let catalog = Arc::clone(&self.catalog);
        let database = self.database.clone();
        block_on_with_runtime(
            async move { catalog.list_tables(&database).await.unwrap_or_default() },
            "paimon catalog access thread panicked",
        )
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let (base, system_name) = split_object_name(name);
        if let Some(system_name) = system_name {
            return self.load_system_table(base, system_name).await;
        }

        let catalog = Arc::clone(&self.catalog);
        let identifier = Identifier::new(self.database.clone(), base);
        await_with_runtime(async move {
            match catalog.get_table(&identifier).await {
                Ok(table) => {
                    let provider = PaimonTableProvider::try_new(table)?;
                    Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>))
                }
                Err(paimon::Error::TableNotExist { .. }) => Ok(None),
                Err(e) => Err(to_datafusion_error(e)),
            }
        })
        .await
    }

    fn table_exist(&self, name: &str) -> bool {
        // Malformed `t$foo$bar` (no `branch_` segment) falls through as plain `t`,
        // matching `table()`.
        let (base, system_name) = split_object_name(name);
        if let Some(system_name) = system_name {
            if !system_tables::is_registered(system_name) {
                return false;
            }
        }

        let catalog = Arc::clone(&self.catalog);
        let identifier = Identifier::new(self.database.clone(), base.to_string());
        block_on_with_runtime(
            async move {
                match catalog.get_table(&identifier).await {
                    Ok(_) => true,
                    Err(paimon::Error::TableNotExist { .. }) => false,
                    Err(_) => false,
                }
            },
            "paimon catalog access thread panicked",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::split_object_name;

    #[test]
    fn plain_table_name() {
        assert_eq!(split_object_name("orders"), ("orders", None));
    }

    #[test]
    fn system_table_only() {
        assert_eq!(
            split_object_name("orders$options"),
            ("orders", Some("options"))
        );
    }

    #[test]
    fn branch_reference_is_not_a_system_table() {
        assert_eq!(split_object_name("orders$branch_main"), ("orders", None));
    }

    #[test]
    fn branch_plus_system_table() {
        assert_eq!(
            split_object_name("orders$branch_main$options"),
            ("orders", Some("options"))
        );
    }

    #[test]
    fn three_parts_without_branch_prefix_is_not_a_system_table() {
        assert_eq!(split_object_name("orders$foo$bar"), ("orders", None));
    }
}
