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

//! Tag manager for reading tag metadata using FileIO.
//!
//! Reference: [org.apache.paimon.utils.TagManager](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/TagManager.java)
//! and [pypaimon.tag.tag_manager.TagManager](https://github.com/apache/paimon/blob/master/paimon-python/pypaimon/tag/tag_manager.py).

use crate::io::FileIO;
use crate::spec::Snapshot;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

const TAG_DIR: &str = "tag";
const TAG_PREFIX: &str = "tag-";

/// Snapshot extended with tag-specific metadata.
///
/// Reference: [Tag.java](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/tag/Tag.java)
//
// `serde(flatten)` requires that `Snapshot` does NOT use
// `#[serde(deny_unknown_fields)]`, otherwise the tag-only fields below would
// fail Snapshot deserialization.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Tag {
    #[serde(flatten)]
    snapshot: Snapshot,
    #[serde(
        rename = "tagCreateTime",
        skip_serializing_if = "Option::is_none",
        default
    )]
    tag_create_time: Option<NaiveDateTime>,
    /// Raw ISO-8601 duration (e.g. `PT1H30M`); not parsed to avoid pulling in
    /// a duration crate just for `$tags` exposure.
    #[serde(
        rename = "tagTimeRetained",
        skip_serializing_if = "Option::is_none",
        default
    )]
    tag_time_retained: Option<String>,
}

impl Tag {
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    pub fn tag_create_time(&self) -> Option<NaiveDateTime> {
        self.tag_create_time
    }

    pub fn tag_time_retained(&self) -> Option<&str> {
        self.tag_time_retained.as_deref()
    }
}

/// Manager for tag files using unified FileIO.
///
/// Tags are named snapshots stored as JSON files at `{table_path}/tag/tag-{name}`.
/// The tag file format is identical to a Snapshot JSON file.
///
/// Reference: [org.apache.paimon.utils.TagManager](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/utils/TagManager.java)
#[derive(Debug, Clone)]
pub struct TagManager {
    file_io: FileIO,
    table_path: String,
}

impl TagManager {
    pub fn new(file_io: FileIO, table_path: String) -> Self {
        Self {
            file_io,
            table_path,
        }
    }

    /// Path to the tag directory (e.g. `table_path/tag`).
    pub fn tag_directory(&self) -> String {
        format!("{}/{}", self.table_path, TAG_DIR)
    }

    /// Path to the tag file for the given name (e.g. `tag/tag-my_tag`).
    pub fn tag_path(&self, tag_name: &str) -> String {
        format!("{}/{}{}", self.tag_directory(), TAG_PREFIX, tag_name)
    }

    /// Check if a tag exists.
    pub async fn tag_exists(&self, tag_name: &str) -> crate::Result<bool> {
        let path = self.tag_path(tag_name);
        let input = self.file_io.new_input(&path)?;
        input.exists().await
    }

    /// List all tags sorted by name ascending.
    pub async fn list_all(&self) -> crate::Result<Vec<(String, Tag)>> {
        let dir = self.tag_directory();
        // See SnapshotManager::list_all for why we don't precheck exists().
        let statuses = match self.file_io.list_status(&dir).await {
            Ok(s) => s,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };
        let mut names: Vec<String> = statuses
            .into_iter()
            .filter_map(|status| {
                if status.is_dir {
                    return None;
                }
                let name = status.path.rsplit('/').next().unwrap_or(&status.path);
                name.strip_prefix(TAG_PREFIX).map(|s| s.to_string())
            })
            .collect();
        names.sort_unstable();

        let tags = futures::future::try_join_all(names.iter().map(|n| self.get_tag(n))).await?;
        Ok(names
            .into_iter()
            .zip(tags)
            .filter_map(|(n, t)| t.map(|t| (n, t)))
            .collect())
    }

    /// Get the tag for a name, or None if the tag file does not exist.
    ///
    /// Reads directly and catches NotFound to avoid a separate exists() IO round-trip.
    pub async fn get_tag(&self, tag_name: &str) -> crate::Result<Option<Tag>> {
        let path = self.tag_path(tag_name);
        let input = self.file_io.new_input(&path)?;
        let bytes = match input.read().await {
            Ok(b) => b,
            Err(crate::Error::IoUnexpected { ref source, .. })
                if source.kind() == opendal::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let tag: Tag = serde_json::from_slice(&bytes).map_err(|e| crate::Error::DataInvalid {
            message: format!("tag '{tag_name}' JSON invalid: {e}"),
            source: Some(Box::new(e)),
        })?;
        Ok(Some(tag))
    }

    /// Get the snapshot portion of a tag, dropping tag-specific metadata.
    pub async fn get_snapshot(&self, tag_name: &str) -> crate::Result<Option<Snapshot>> {
        Ok(self.get_tag(tag_name).await?.map(|t| t.snapshot))
    }
}
