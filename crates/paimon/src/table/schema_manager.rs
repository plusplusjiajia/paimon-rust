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
use opendal::raw::get_basename;
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

    /// Return the schema with the highest id, or `None` if the directory is
    /// empty/missing. Re-scans on every call so schema evolution is observable.
    ///
    /// Mirrors Java [SchemaManager.latest()](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java).
    pub async fn latest(&self) -> crate::Result<Option<Arc<TableSchema>>> {
        let max_id = self
            .file_io
            .list_status(&self.schema_directory())
            .await?
            .into_iter()
            .filter(|s| !s.is_dir)
            .filter_map(|s| {
                get_basename(s.path.as_str())
                    .strip_prefix(SCHEMA_PREFIX)?
                    .parse::<i64>()
                    .ok()
            })
            .max();
        match max_id {
            Some(id) => Ok(Some(self.schema(id).await?)),
            None => Ok(None),
        }
    }

    /// Load a schema by ID. Returns cached version if available.
    ///
    /// The cache is shared across all clones of this `SchemaManager`, so loading
    /// a schema in one stream makes it available to all other streams reading
    /// from the same table.
    ///
    /// Reference: [SchemaManager.schema(long)](https://github.com/apache/paimon/blob/release-1.3/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java)
    pub async fn schema(&self, schema_id: i64) -> crate::Result<Arc<TableSchema>> {
        // Fast path: check cache under a short lock.
        {
            let cache = self.cache.lock().unwrap();
            if let Some(schema) = cache.get(&schema_id) {
                return Ok(schema.clone());
            }
        }

        // Cache miss — load from file (no lock held during I/O).
        let path = self.schema_path(schema_id);
        let input = self.file_io.new_input(&path)?;
        let bytes = input.read().await?;
        let schema: TableSchema =
            serde_json::from_slice(&bytes).map_err(|e| crate::Error::DataInvalid {
                message: format!("Failed to parse schema file: {path}"),
                source: Some(Box::new(e)),
            })?;
        let schema = Arc::new(schema);

        // Insert into shared cache (short lock).
        {
            let mut cache = self.cache.lock().unwrap();
            cache.entry(schema_id).or_insert_with(|| schema.clone());
        }

        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use bytes::Bytes;

    fn memory_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    async fn write_schema_file(file_io: &FileIO, dir: &str, id: i64) {
        let schema = crate::spec::Schema::builder().build().unwrap();
        let table_schema = TableSchema::new(id, &schema);
        let json = serde_json::to_vec(&table_schema).unwrap();
        let path = format!("{dir}/{SCHEMA_PREFIX}{id}");
        let out = file_io.new_output(&path).unwrap();
        out.write(Bytes::from(json)).await.unwrap();
    }

    #[tokio::test]
    async fn latest_returns_none_when_directory_missing() {
        let file_io = memory_file_io();
        let sm = SchemaManager::new(file_io, "memory:/latest_missing".to_string());
        assert!(sm.latest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn latest_returns_none_for_empty_directory() {
        let file_io = memory_file_io();
        let table_path = "memory:/latest_empty";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();

        let sm = SchemaManager::new(file_io, table_path.to_string());
        assert!(sm.latest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn latest_returns_schema_with_max_id() {
        let file_io = memory_file_io();
        let table_path = "memory:/latest_max";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        for id in [0, 2, 1] {
            write_schema_file(&file_io, &dir, id).await;
        }

        let sm = SchemaManager::new(file_io, table_path.to_string());
        let latest = sm.latest().await.unwrap().expect("latest schema");
        assert_eq!(latest.id(), 2);
    }

    #[tokio::test]
    async fn latest_ignores_unrelated_files() {
        let file_io = memory_file_io();
        let table_path = "memory:/latest_filter";
        let dir = format!("{table_path}/{SCHEMA_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        write_schema_file(&file_io, &dir, 0).await;
        let junk = file_io
            .new_output(&format!("{dir}/{SCHEMA_PREFIX}foo"))
            .unwrap();
        junk.write(Bytes::from("{}")).await.unwrap();
        let other = file_io.new_output(&format!("{dir}/README")).unwrap();
        other.write(Bytes::from("hi")).await.unwrap();

        let sm = SchemaManager::new(file_io, table_path.to_string());
        let latest = sm.latest().await.unwrap().expect("latest schema");
        assert_eq!(latest.id(), 0);
    }
}
