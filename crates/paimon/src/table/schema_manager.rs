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

//! Schema manager for reading versioned table schemas.
//!
//! Reference: [org.apache.paimon.schema.SchemaManager](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java)

use crate::io::FileIO;
use crate::spec::TableSchema;
use crate::table::{list_prefixed_i64_ids, LIST_FETCH_CONCURRENCY};
use futures::{StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const SCHEMA_DIR: &str = "schema";
const SCHEMA_PREFIX: &str = "schema-";

/// Manager for versioned table schema files.
///
/// Each table stores schema versions as JSON files under `{table_path}/schema/schema-{id}`.
/// When a schema evolution occurs (e.g. ADD COLUMN, ALTER COLUMN TYPE), a new schema file
/// is written with an incremented ID. Data files record which schema they were written with
/// via `DataFileMeta.schema_id`.
///
/// The schema cache is shared across clones via `Arc`, so multiple readers
/// (e.g. parallel split streams) benefit from a single cache.
///
/// Reference: [org.apache.paimon.schema.SchemaManager](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java)
#[derive(Debug, Clone)]
pub struct SchemaManager {
    file_io: FileIO,
    table_path: String,
    /// Shared cache of loaded schemas by ID.
    cache: Arc<Mutex<HashMap<i64, Arc<TableSchema>>>>,
}

impl SchemaManager {
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Path to the schema directory (e.g. `{table_path}/schema`).
    fn schema_directory(&self) -> String {
        format!("{}/{}", self.table_path.trim_end_matches('/'), SCHEMA_DIR)
    }

    /// Path to a specific schema file (e.g. `{table_path}/schema/schema-0`).
    fn schema_path(&self, schema_id: i64) -> String {
        format!("{}/{}{}", self.schema_directory(), SCHEMA_PREFIX, schema_id)
    }

    /// List all schema IDs sorted ascending.
    pub async fn list_all_ids(&self) -> crate::Result<Vec<i64>> {
        list_prefixed_i64_ids(&self.file_io, &self.schema_directory(), SCHEMA_PREFIX).await
    }

    /// List all schemas sorted by id ascending. Schema files that disappear
    /// between the directory listing and the per-schema read are silently
    /// dropped; JSON parse failures and id-mismatch errors still propagate.
    pub async fn list_all(&self) -> crate::Result<Vec<Arc<TableSchema>>> {
        let ids = self.list_all_ids().await?;
        futures::stream::iter(ids)
            .map(|id| self.find_schema(id))
            .buffered(LIST_FETCH_CONCURRENCY)
            .try_filter_map(|s| async move { Ok(s) })
            .try_collect()
            .await
    }

    /// Load a schema by ID. Returns cached version if available.
    ///
    /// The cache is shared across all clones of this `SchemaManager`, so loading
    /// a schema in one stream makes it available to all other streams reading
    /// from the same table.
    ///
    /// Reference: [SchemaManager.schema(long)](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java)
    pub async fn schema(&self, schema_id: i64) -> crate::Result<Arc<TableSchema>> {
        self.find_schema(schema_id)
            .await?
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "schema file does not exist: {}",
                    self.schema_path(schema_id)
                ),
                source: None,
            })
    }

    /// Like [`schema`](Self::schema) but returns `None` when the schema file
    /// is missing, for callers that tolerate expiry races.
    pub async fn find_schema(&self, schema_id: i64) -> crate::Result<Option<Arc<TableSchema>>> {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(schema) = cache.get(&schema_id) {
                return Ok(Some(schema.clone()));
            }
        }

        let path = self.schema_path(schema_id);
        let input = self.file_io.new_input(&path)?;
        let bytes = match input.read().await {
            Ok(b) => b,
            Err(e) if e.is_not_found() => return Ok(None),
            Err(e) => return Err(e),
        };
        let schema: TableSchema =
            serde_json::from_slice(&bytes).map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to parse schema file: {path}"),
                source: Some(Box::new(e)),
            })?;
        if schema.id() != schema_id {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "schema file id mismatch: in file name is {schema_id}, but file contains schema id {}",
                    schema.id()
                ),
                source: None,
            });
        }
        let schema = Arc::new(schema);

        {
            let mut cache = self.cache.lock().unwrap();
            cache.entry(schema_id).or_insert_with(|| schema.clone());
        }

        Ok(Some(schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use bytes::Bytes;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    async fn write_schema_marker(file_io: &FileIO, dir: &str, id: i64) {
        write_schema_file(file_io, dir, id, id).await;
    }

    #[tokio::test]
    async fn test_list_all_ids_empty_when_directory_missing() {
        let file_io = test_file_io();
        let sm = SchemaManager::new(file_io, "memory:/test_schema_missing".to_string());
        assert!(sm.list_all_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_all_ids_returns_sorted_ids() {
        let file_io = test_file_io();
        let table_path = "memory:/test_schema_sorted";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        for id in [3, 1, 2] {
            write_schema_marker(&file_io, &dir, id).await;
        }

        let sm = SchemaManager::new(file_io, table_path.to_string());
        let ids = sm.list_all_ids().await.unwrap();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    async fn write_schema_file(file_io: &FileIO, dir: &str, file_id: i64, content_id: i64) {
        let schema = crate::spec::Schema::builder().build().unwrap();
        let table_schema = TableSchema::new(content_id, &schema);
        let json = serde_json::to_vec(&table_schema).unwrap();
        let path = format!("{dir}/{SCHEMA_PREFIX}{file_id}");
        let out = file_io.new_output(&path).unwrap();
        out.write(Bytes::from(json)).await.unwrap();
    }

    #[tokio::test]
    async fn test_schema_rejects_id_mismatch() {
        let file_io = test_file_io();
        let table_path = "memory:/test_schema_mismatch";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        write_schema_file(&file_io, &dir, 1, 2).await;

        let sm = SchemaManager::new(file_io, table_path.to_string());
        let err = sm.schema(1).await.unwrap_err();
        assert!(
            format!("{err}").contains("schema file id mismatch"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_list_all_validates_id_match() {
        let file_io = test_file_io();
        let table_path = "memory:/test_schema_list_all_mismatch";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        write_schema_file(&file_io, &dir, 0, 0).await;
        write_schema_file(&file_io, &dir, 1, 99).await;

        let sm = SchemaManager::new(file_io, table_path.to_string());
        assert!(sm.list_all().await.is_err());
    }

    #[tokio::test]
    async fn test_list_all_ids_skips_unrelated_files() {
        let file_io = test_file_io();
        let table_path = "memory:/test_schema_filter";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        write_schema_marker(&file_io, &dir, 0).await;
        let junk = file_io
            .new_output(&format!("{dir}/{SCHEMA_PREFIX}foo"))
            .unwrap();
        junk.write(Bytes::from("{}")).await.unwrap();
        let other = file_io.new_output(&format!("{dir}/README")).unwrap();
        other.write(Bytes::from("hi")).await.unwrap();

        let sm = SchemaManager::new(file_io, table_path.to_string());
        assert_eq!(sm.list_all_ids().await.unwrap(), vec![0]);
    }
}
