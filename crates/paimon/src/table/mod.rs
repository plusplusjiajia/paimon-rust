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

//! Table API for Apache Paimon

pub(crate) mod bin_pack;
mod bucket_filter;
mod commit_message;
#[cfg(feature = "fulltext")]
mod full_text_search_builder;
pub(crate) mod global_index_scanner;
mod read_builder;
pub(crate) mod rest_env;
pub(crate) mod row_id_predicate;
pub(crate) mod schema_manager;
pub(crate) mod snapshot_commit;
mod snapshot_manager;
mod source;
mod stats_filter;
pub(crate) mod table_commit;
mod table_scan;
mod tag_manager;
mod write_builder;

use crate::Result;
use arrow_array::RecordBatch;
pub use commit_message::CommitMessage;
#[cfg(feature = "fulltext")]
pub use full_text_search_builder::FullTextSearchBuilder;
use futures::stream::BoxStream;
pub use read_builder::{ReadBuilder, TableRead};
pub use rest_env::RESTEnv;
pub use schema_manager::SchemaManager;
pub use snapshot_commit::{RESTSnapshotCommit, RenamingSnapshotCommit, SnapshotCommit};
pub use snapshot_manager::SnapshotManager;
pub use source::{
    merge_row_ranges, DataSplit, DataSplitBuilder, DeletionFile, PartitionBucket, Plan, RowRange,
};
pub use table_commit::TableCommit;
pub use table_scan::TableScan;
pub use tag_manager::{Tag, TagManager};
pub use write_builder::WriteBuilder;

use crate::catalog::Identifier;
use crate::io::FileIO;
use crate::spec::TableSchema;
use std::collections::HashMap;

/// Max in-flight per-entry fetches in `list_all`-style batch reads.
pub(crate) const LIST_FETCH_CONCURRENCY: usize = 32;

/// List file names directly under `dir`, strip `prefix`, parse the remainder
/// as `i64`, and return the sorted ids. Missing dir → empty. Entries whose
/// suffix is not a valid `i64` (non-numeric, overflow, empty) are silently
/// skipped — callers needing detection should walk [`FileIO::list_status`].
pub(crate) async fn list_prefixed_i64_ids(
    file_io: &FileIO,
    dir: &str,
    prefix: &str,
) -> Result<Vec<i64>> {
    let statuses = file_io.list_status_or_empty(dir).await?;
    let mut ids: Vec<i64> = statuses
        .into_iter()
        .filter(|s| !s.is_dir)
        .filter_map(|s| {
            crate::io::path_basename(&s.path)
                .strip_prefix(prefix)?
                .parse::<i64>()
                .ok()
        })
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

/// Table represents a table in the catalog.
#[derive(Debug, Clone)]
pub struct Table {
    file_io: FileIO,
    identifier: Identifier,
    location: String,
    schema: TableSchema,
    schema_manager: SchemaManager,
    snapshot_manager: SnapshotManager,
    tag_manager: TagManager,
    rest_env: Option<RESTEnv>,
}

impl Table {
    /// Create a new table.
    pub fn new(
        file_io: FileIO,
        identifier: Identifier,
        location: String,
        schema: TableSchema,
        rest_env: Option<RESTEnv>,
    ) -> Self {
        let schema_manager = SchemaManager::new(file_io.clone(), location.clone());
        let snapshot_manager = SnapshotManager::new(file_io.clone(), location.clone());
        let tag_manager = TagManager::new(file_io.clone(), location.clone());
        Self {
            file_io,
            identifier,
            location,
            schema,
            schema_manager,
            snapshot_manager,
            tag_manager,
            rest_env,
        }
    }

    /// Get the table's identifier.
    pub fn identifier(&self) -> &Identifier {
        &self.identifier
    }

    /// Get the table's location.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Get the table's schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Get the FileIO instance for this table.
    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    /// Get the SchemaManager for this table.
    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }

    pub fn snapshot_manager(&self) -> &SnapshotManager {
        &self.snapshot_manager
    }

    pub fn tag_manager(&self) -> &TagManager {
        &self.tag_manager
    }

    /// Create a read builder for scan/read.
    ///
    /// Reference: [pypaimon FileStoreTable.new_read_builder](https://github.com/apache/paimon/blob/release-1.3/paimon-python/pypaimon/table/file_store_table.py).
    pub fn new_read_builder(&self) -> ReadBuilder<'_> {
        ReadBuilder::new(self)
    }

    /// Create a full-text search builder.
    ///
    /// Reference: [FullTextSearchBuilderImpl](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/table/source/FullTextSearchBuilderImpl.java)
    #[cfg(feature = "fulltext")]
    pub fn new_full_text_search_builder(&self) -> FullTextSearchBuilder<'_> {
        FullTextSearchBuilder::new(self)
    }

    /// Create a write builder for write/commit.
    ///
    /// Reference: [pypaimon FileStoreTable.new_write_builder](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/table/file_store_table.py).
    pub fn new_write_builder(&self) -> WriteBuilder<'_> {
        WriteBuilder::new(self)
    }

    /// Create a copy of this table with extra options merged into the schema.
    pub fn copy_with_options(&self, extra: HashMap<String, String>) -> Self {
        Self {
            file_io: self.file_io.clone(),
            identifier: self.identifier.clone(),
            location: self.location.clone(),
            schema: self.schema.copy_with_options(extra),
            schema_manager: self.schema_manager.clone(),
            snapshot_manager: self.snapshot_manager.clone(),
            tag_manager: self.tag_manager.clone(),
            rest_env: self.rest_env.clone(),
        }
    }
}

/// A stream of arrow [`RecordBatch`]es.
pub type ArrowRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;
