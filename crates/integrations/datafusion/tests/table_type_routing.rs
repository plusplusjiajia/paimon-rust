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

//! Declared-type table routing: tables declared as a non-Paimon type (e.g.
//! `iceberg-table`) resolve through the registered engine; everything else
//! takes the Paimon path unchanged.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use paimon::catalog::{Catalog, Database, Identifier, RoutedTableLoad};
use paimon::spec::{Schema as PaimonSchema, SchemaChange};
use paimon::table::Table;
use paimon::{CatalogOptions, FileSystemCatalog, Options, Result as PaimonResult};
use paimon_datafusion::{SQLContext, TableEngineResolver};
use tempfile::TempDir;

const CATALOG: &str = "cat";
const DB: &str = "shared_db";
const ICEBERG_TABLE_TYPE: &str = "iceberg-table";

/// A filesystem-backed catalog that declares table types the way a REST
/// catalog does through the `type` table option.
#[derive(Debug)]
struct TypedTestCatalog {
    inner: Arc<FileSystemCatalog>,
    declared_types: HashMap<String, String>,
}

#[async_trait]
impl Catalog for TypedTestCatalog {
    async fn list_databases(&self) -> PaimonResult<Vec<String>> {
        self.inner.list_databases().await
    }

    async fn create_database(
        &self,
        name: &str,
        ignore_if_exists: bool,
        properties: HashMap<String, String>,
    ) -> PaimonResult<()> {
        self.inner
            .create_database(name, ignore_if_exists, properties)
            .await
    }

    async fn get_database(&self, name: &str) -> PaimonResult<Database> {
        self.inner.get_database(name).await
    }

    async fn drop_database(
        &self,
        name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> PaimonResult<()> {
        self.inner
            .drop_database(name, ignore_if_not_exists, cascade)
            .await
    }

    async fn get_table(&self, identifier: &Identifier) -> PaimonResult<Table> {
        // Mirror the REST catalog: declared-non-Paimon tables fail closed.
        if let Some(declared) = self.declared_types.get(identifier.object()) {
            return Err(paimon::Error::Unsupported {
                message: format!(
                    "table '{}' is declared '{declared}' and cannot be read as a Paimon table",
                    identifier.full_name()
                ),
            });
        }
        self.inner.get_table(identifier).await
    }

    async fn load_table_routing(
        &self,
        identifier: &Identifier,
        non_paimon_types: &HashSet<String>,
    ) -> PaimonResult<RoutedTableLoad> {
        if let Some(declared) = self.declared_types.get(identifier.object()) {
            let declared = declared.to_ascii_lowercase();
            if non_paimon_types.contains(&declared) {
                return Ok(RoutedTableLoad::NonPaimon(declared));
            }
        }
        Ok(RoutedTableLoad::Paimon(Box::new(
            self.get_table(identifier).await?,
        )))
    }

    async fn list_tables(&self, database_name: &str) -> PaimonResult<Vec<String>> {
        let mut names = self.inner.list_tables(database_name).await?;
        names.extend(self.declared_types.keys().cloned());
        Ok(names)
    }

    async fn create_table(
        &self,
        identifier: &Identifier,
        creation: PaimonSchema,
        ignore_if_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .create_table(identifier, creation, ignore_if_exists)
            .await
    }

    async fn drop_table(
        &self,
        identifier: &Identifier,
        ignore_if_not_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .drop_table(identifier, ignore_if_not_exists)
            .await
    }

    async fn rename_table(
        &self,
        from: &Identifier,
        to: &Identifier,
        ignore_if_not_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .rename_table(from, to, ignore_if_not_exists)
            .await
    }

    async fn alter_table(
        &self,
        identifier: &Identifier,
        changes: Vec<SchemaChange>,
        ignore_if_not_exists: bool,
    ) -> PaimonResult<()> {
        self.inner
            .alter_table(identifier, changes, ignore_if_not_exists)
            .await
    }
}

/// A stand-in engine: serves a fixed two-row in-memory table for `it`.
#[derive(Debug)]
struct FakeEngineResolver;

#[async_trait]
impl TableEngineResolver for FakeEngineResolver {
    async fn resolve_table(
        &self,
        _database: &str,
        table: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if table != "it" {
            return Ok(None);
        }
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 3])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .map_err(DataFusionError::from)?;
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        Ok(Some(Arc::new(table)))
    }
}

struct TestEnv {
    // Owns the warehouse directory for the duration of the test.
    _paimon_dir: TempDir,
    ctx: SQLContext,
}

/// One SQLContext with catalog `cat`: a Paimon table `cat.shared_db.pt`
/// (two rows, two snapshots) plus a declared `iceberg-table`
/// `cat.shared_db.it` routed to a fake engine.
async fn setup() -> TestEnv {
    let paimon_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", paimon_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let fs_catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let typed_catalog = Arc::new(TypedTestCatalog {
        inner: fs_catalog,
        declared_types: HashMap::from([
            ("it".to_string(), ICEBERG_TABLE_TYPE.to_string()),
            // Unknown to the engine: existence mirrors the resolver's miss.
            ("ghost".to_string(), ICEBERG_TABLE_TYPE.to_string()),
            // Write-statement target: raw get_table paths fail closed.
            ("ft".to_string(), ICEBERG_TABLE_TYPE.to_string()),
        ]),
    });
    let mut ctx = SQLContext::new();
    ctx.register_catalog(CATALOG, typed_catalog).await.unwrap();
    ctx.sql(&format!("CREATE SCHEMA {CATALOG}.{DB}"))
        .await
        .unwrap();
    ctx.sql(&format!(
        "CREATE TABLE {CATALOG}.{DB}.pt (id INT NOT NULL, name STRING)"
    ))
    .await
    .unwrap();
    // Two separate INSERTs -> two snapshots, so time travel has history.
    for stmt in [
        format!("INSERT INTO {CATALOG}.{DB}.pt VALUES (1, 'a')"),
        format!("INSERT INTO {CATALOG}.{DB}.pt VALUES (2, 'b')"),
    ] {
        ctx.sql(&stmt).await.unwrap().collect().await.unwrap();
    }

    ctx.register_catalog_table_engine(CATALOG, ICEBERG_TABLE_TYPE, Arc::new(FakeEngineResolver))
        .unwrap();

    TestEnv {
        _paimon_dir: paimon_dir,
        ctx,
    }
}

fn column_i32(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|b| {
            let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn paimon_path_still_serves_paimon_tables() {
    let env = setup().await;
    let batches = env
        .ctx
        .sql(&format!("SELECT id FROM {CATALOG}.{DB}.pt ORDER BY id"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_i32(&batches), vec![1, 2]);
}

#[tokio::test]
async fn declared_non_paimon_table_routes_to_engine() {
    let env = setup().await;
    let df = env
        .ctx
        .sql(&format!("SELECT id, payload FROM {CATALOG}.{DB}.it"))
        .await
        .unwrap();
    // Schema comes from the engine — `payload` only exists there.
    let names: Vec<String> = df
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["id".to_string(), "payload".to_string()]);
    let batches = df.collect().await.unwrap();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2);
}

#[tokio::test]
async fn cross_engine_join_plans_and_runs() {
    let env = setup().await;
    let batches = env
        .ctx
        .sql(&format!(
            "SELECT p.id FROM {CATALOG}.{DB}.pt p JOIN {CATALOG}.{DB}.it i ON p.id = i.id"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // pt has ids {1,2}; the engine table has {1,3}.
    assert_eq!(column_i32(&batches), vec![1]);
}

#[tokio::test]
async fn missing_table_still_errors() {
    let env = setup().await;
    let Err(err) = env
        .ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.does_not_exist"))
        .await
    else {
        panic!("query against a missing table must fail");
    };
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("does_not_exist") || msg.contains("not found"),
        "{msg}"
    );
}

// Downcast-based paths — time travel, temp tables — must keep working
// with engines registered.
#[tokio::test]
async fn time_travel_still_works_with_engines_registered() {
    let env = setup().await;
    let batches = env
        .ctx
        .sql(&format!("SELECT id FROM {CATALOG}.{DB}.pt VERSION AS OF 1"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // Snapshot 1 holds only the first INSERT.
    assert_eq!(column_i32(&batches), vec![1]);
}

#[tokio::test]
async fn temp_tables_still_work_with_engines_registered() {
    let env = setup().await;
    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();
    env.ctx
        .register_temp_table(format!("{CATALOG}.{DB}.tmp_t"), Arc::new(mem))
        .expect("temp table registration must survive engine registration");
    assert!(env
        .ctx
        .temp_table_exist(format!("{CATALOG}.{DB}.tmp_t"))
        .unwrap());
}

#[tokio::test]
async fn schemas_and_ddl_behave_with_engines_registered() {
    let env = setup().await;
    let provider = env.ctx.ctx().catalog(CATALOG).unwrap();
    assert!(provider.schema(DB).is_some());
    assert!(provider.schema("no_such_db").is_none());
    env.ctx
        .sql(&format!("CREATE SCHEMA {CATALOG}.fresh_db"))
        .await
        .expect("CREATE SCHEMA must work with engines registered");
    env.ctx
        .sql(&format!(
            "CREATE TABLE {CATALOG}.fresh_db.t (id INT NOT NULL)"
        ))
        .await
        .expect("CREATE TABLE in the fresh schema must work");
}

#[tokio::test]
async fn table_names_come_from_the_catalog_listing() {
    let env = setup().await;
    let provider = env.ctx.ctx().catalog(CATALOG).unwrap();
    let schema = provider.schema(DB).unwrap();
    let mut names = schema.table_names();
    names.sort();
    names.dedup();
    assert!(names.contains(&"pt".to_string()), "{names:?}");
    assert!(names.contains(&"it".to_string()), "{names:?}");
}

// Engine failures surface as the engine's error, never as "not found".
#[derive(Debug)]
struct BrokenResolver;

#[async_trait]
impl TableEngineResolver for BrokenResolver {
    async fn resolve_table(
        &self,
        _database: &str,
        _table: &str,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Execution(
            "engine backend exploded".to_string(),
        ))
    }
}

#[tokio::test]
async fn engine_errors_are_surfaced() {
    let env = setup().await;
    env.ctx
        .register_catalog_table_engine(CATALOG, ICEBERG_TABLE_TYPE, Arc::new(BrokenResolver))
        .unwrap();
    let Err(err) = env
        .ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.it"))
        .await
    else {
        panic!("broken engine must fail loudly");
    };
    let msg = err.to_string();
    assert!(msg.contains("engine backend exploded"), "{msg}");
}

// A declared non-Paimon type with no registered engine takes the Paimon path.
#[tokio::test]
async fn undeclared_engine_type_takes_paimon_path() {
    let paimon_dir = TempDir::new().unwrap();
    let warehouse = format!("file://{}", paimon_dir.path().display());
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let fs_catalog = Arc::new(FileSystemCatalog::new(options).unwrap());
    let typed_catalog = Arc::new(TypedTestCatalog {
        inner: fs_catalog,
        declared_types: HashMap::from([("ot".to_string(), "object-table".to_string())]),
    });
    let mut ctx = SQLContext::new();
    ctx.register_catalog(CATALOG, typed_catalog).await.unwrap();
    ctx.sql(&format!("CREATE SCHEMA {CATALOG}.{DB}"))
        .await
        .unwrap();
    // No engine registered at all: `ot` resolves through Paimon and fails as
    // a plain missing table, exactly like before routing existed.
    let Err(err) = ctx.sql(&format!("SELECT * FROM {CATALOG}.{DB}.ot")).await else {
        panic!("undeclared engine type must fall through to Paimon");
    };
    let msg = err.to_string().to_lowercase();
    assert!(msg.contains("ot") || msg.contains("not found"), "{msg}");
}

// Write statements against routed tables must fail closed.
#[tokio::test]
async fn writes_to_routed_tables_fail_closed() {
    let env = setup().await;
    let Err(err) = env
        .ctx
        .sql(&format!("UPDATE {CATALOG}.{DB}.ft SET id = 1"))
        .await
    else {
        panic!("UPDATE on a routed table must fail");
    };
    let msg = err.to_string();
    assert!(msg.contains("cannot be read as a Paimon table"), "{msg}");
    assert!(msg.contains(ICEBERG_TABLE_TYPE), "{msg}");
}

// System tables are Paimon-only; routed tables get a clear error.
#[tokio::test]
async fn system_tables_on_routed_tables_error() {
    let env = setup().await;
    let Err(err) = env
        .ctx
        .sql(&format!("SELECT * FROM {CATALOG}.{DB}.\"it$snapshots\""))
        .await
    else {
        panic!("system table on a routed table must fail");
    };
    let msg = err.to_string();
    assert!(msg.contains("cannot be read as a Paimon table"), "{msg}");
}

// Existence mirrors resolution: engine hit -> true, miss -> false.
#[tokio::test]
async fn table_exist_mirrors_the_resolver() {
    let env = setup().await;
    let provider = env.ctx.ctx().catalog(CATALOG).unwrap();
    let schema = provider.schema(DB).unwrap();
    assert!(schema.table_exist("it"));
    assert!(!schema.table_exist("ghost"));
    assert!(!schema.table_exist("it$snapshots"));
}

// Only declared non-Paimon table types may be routed.
#[tokio::test]
async fn paimon_managed_types_cannot_be_routed() {
    let env = setup().await;
    let Err(err) =
        env.ctx
            .register_catalog_table_engine(CATALOG, "lance-table", Arc::new(FakeEngineResolver))
    else {
        panic!("registering an engine for a Paimon-managed type must fail");
    };
    let msg = err.to_string();
    assert!(msg.contains("Paimon-managed"), "{msg}");
}

// Registration canonicalizes the type key.
#[tokio::test]
async fn mixed_case_registration_still_routes() {
    let env = setup().await;
    env.ctx
        .register_catalog_table_engine(CATALOG, "ICEBERG-TABLE", Arc::new(FakeEngineResolver))
        .unwrap();
    let batches = env
        .ctx
        .sql(&format!("SELECT id FROM {CATALOG}.{DB}.it ORDER BY id"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(column_i32(&batches), vec![1, 3]);
}

// Resolver failures must not masquerade as misses in existence checks.
#[tokio::test]
async fn table_exist_preserves_resolver_failures() {
    let env = setup().await;
    env.ctx
        .register_catalog_table_engine(CATALOG, ICEBERG_TABLE_TYPE, Arc::new(BrokenResolver))
        .unwrap();
    let provider = env.ctx.ctx().catalog(CATALOG).unwrap();
    let schema = provider.schema(DB).unwrap();
    // Conservatively "exists"; the follow-up table() surfaces the error.
    assert!(schema.table_exist("it"));
}

// DML against a routed table is rejected even when the engine's provider
// is writable (the fake resolver returns a writable MemTable).
#[tokio::test]
async fn insert_into_routed_table_is_rejected() {
    let env = setup().await;
    let err = async {
        env.ctx
            .sql(&format!("INSERT INTO {CATALOG}.{DB}.it VALUES (9, 'z')"))
            .await?
            .collect()
            .await
    }
    .await
    .expect_err("insert into a routed table must fail");
    assert!(
        err.to_string()
            .contains("write is not supported for routed 'iceberg-table' tables"),
        "{err}"
    );
}
