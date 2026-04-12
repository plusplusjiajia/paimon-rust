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

use serde::{Deserialize, Serialize};

const TAG_DIR: &str = "tag";
const TAG_PREFIX: &str = "tag-";

/// Snapshot extended with tag-specific metadata. Both tag fields are kept as
/// raw strings to tolerate any format Java's `LocalDateTime.toString()` /
/// ISO-8601 duration emits.
///
/// Reference: [Tag.java](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/tag/Tag.java)
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Tag {
    #[serde(flatten)]
    snapshot: Snapshot,
    #[serde(
        rename = "tagCreateTime",
        skip_serializing_if = "Option::is_none",
        default
    )]
    tag_create_time: Option<String>,
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

    pub fn tag_create_time(&self) -> Option<&str> {
        self.tag_create_time.as_deref()
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

    /// List all tags sorted lexicographically by name. Tags deleted between
    /// the directory listing and the per-tag read are silently dropped.
    pub async fn list_all(&self) -> crate::Result<Vec<(String, Tag)>> {
        let statuses = self
            .file_io
            .list_status_or_empty(&self.tag_directory())
            .await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use bytes::Bytes;
    use std::env::current_dir;

    fn test_file_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn load_fixture(name: &str) -> String {
        let path = current_dir()
            .unwrap()
            .join(format!("tests/fixtures/tag/{name}.json"));
        String::from_utf8(std::fs::read(&path).unwrap()).unwrap()
    }

    #[test]
    fn test_tag_deserialize_java_fixture() {
        let tag: Tag = serde_json::from_str(&load_fixture("tag-2024-01-01")).unwrap();
        assert_eq!(tag.snapshot().id(), 2);
        assert_eq!(tag.tag_create_time(), Some("2024-01-01T12:34:56.789"));
        assert_eq!(tag.tag_time_retained(), Some("PT1H30M"));

        let round_trip = serde_json::to_string(&tag).unwrap();
        let back: Tag = serde_json::from_str(&round_trip).unwrap();
        assert_eq!(tag, back);
    }

    #[test]
    fn test_tag_deserialize_without_tag_fields() {
        let tag: Tag = serde_json::from_str(&load_fixture("tag-minimal")).unwrap();
        assert_eq!(tag.snapshot().id(), 1);
        assert!(tag.tag_create_time().is_none());
        assert!(tag.tag_time_retained().is_none());
    }

    #[tokio::test]
    async fn test_list_all_empty_when_directory_missing() {
        let file_io = test_file_io();
        let tm = TagManager::new(file_io, "memory:/test_tag_missing".to_string());
        assert!(tm.list_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_all_returns_sorted_tags() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_sorted";
        let dir = format!("{table_path}/{TAG_DIR}");
        file_io.mkdirs(&dir).await.unwrap();

        let payload = load_fixture("tag-minimal");
        for name in ["v1", "rc2", "alpha"] {
            let path = format!("{dir}/{TAG_PREFIX}{name}");
            let out = file_io.new_output(&path).unwrap();
            out.write(Bytes::from(payload.clone())).await.unwrap();
        }

        let tm = TagManager::new(file_io, table_path.to_string());
        let tags = tm.list_all().await.unwrap();
        let names: Vec<&str> = tags.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["alpha", "rc2", "v1"]);
    }

    #[tokio::test]
    async fn test_get_snapshot_drops_tag_fields() {
        let file_io = test_file_io();
        let table_path = "memory:/test_tag_get_snapshot";
        let dir = format!("{table_path}/{TAG_DIR}");
        file_io.mkdirs(&dir).await.unwrap();
        let out = file_io
            .new_output(&format!("{dir}/{TAG_PREFIX}t1"))
            .unwrap();
        out.write(Bytes::from(load_fixture("tag-2024-01-01")))
            .await
            .unwrap();

        let tm = TagManager::new(file_io, table_path.to_string());
        let snap = tm.get_snapshot("t1").await.unwrap().unwrap();
        assert_eq!(snap.id(), 2);
    }
}
