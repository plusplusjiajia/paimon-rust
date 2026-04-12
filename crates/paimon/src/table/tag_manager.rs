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

use crate::io::{path_basename, FileIO};
use crate::spec::Snapshot;
use crate::table::LIST_FETCH_CONCURRENCY;

use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};

const TAG_DIR: &str = "tag";
const TAG_PREFIX: &str = "tag-";

/// Snapshot extended with tag-specific metadata. Tag time fields are kept as
/// raw strings to tolerate Java's `LocalDateTime.toString()` / ISO-8601 output.
///
/// `#[serde(flatten)]` forbids `Snapshot` from using `deny_unknown_fields` and
/// from adding fields named `tagCreateTime` / `tagTimeRetained`.
///
/// Reference: [Tag.java](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/tag/Tag.java)
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Tag {
    /// Populated from the file name after deserialization; absent in the JSON.
    #[serde(skip, default)]
    name: String,
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
    pub fn name(&self) -> &str {
        &self.name
    }

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

// `name` comes from the file path, not the JSON, so it is excluded from
// equality. Any future `Hash` impl must mirror this exclusion.
impl PartialEq for Tag {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot == other.snapshot
            && self.tag_create_time == other.tag_create_time
            && self.tag_time_retained == other.tag_time_retained
    }
}

impl Eq for Tag {}

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
    pub async fn list_all(&self) -> crate::Result<Vec<Tag>> {
        let statuses = self
            .file_io
            .list_status_or_empty(&self.tag_directory())
            .await?;
        let mut names: Vec<String> = statuses
            .into_iter()
            .filter(|s| !s.is_dir)
            .filter_map(|s| {
                path_basename(&s.path)
                    .strip_prefix(TAG_PREFIX)
                    .map(String::from)
            })
            .collect();
        names.sort_unstable();

        futures::stream::iter(names)
            .map(|name| async move { self.get_tag(&name).await })
            .buffered(LIST_FETCH_CONCURRENCY)
            .try_filter_map(|t| async move { Ok(t) })
            .try_collect()
            .await
    }

    /// Get the tag for a name, or None if the tag file does not exist.
    ///
    /// Reads directly and catches NotFound to avoid a separate exists() IO round-trip.
    pub async fn get_tag(&self, tag_name: &str) -> crate::Result<Option<Tag>> {
        let path = self.tag_path(tag_name);
        let input = self.file_io.new_input(&path)?;
        let bytes = match input.read().await {
            Ok(b) => b,
            Err(e) if e.is_not_found() => return Ok(None),
            Err(e) => return Err(e),
        };
        let mut tag: Tag =
            serde_json::from_slice(&bytes).map_err(|e| crate::Error::DataInvalid {
                message: format!("tag '{tag_name}' JSON invalid: {e}"),
                source: Some(Box::new(e)),
            })?;
        tag.name = tag_name.to_owned();
        Ok(Some(tag))
    }

    /// Get the snapshot portion of a tag, dropping tag-specific metadata.
    pub async fn get_snapshot(&self, tag_name: &str) -> crate::Result<Option<Snapshot>> {
        Ok(self.get_tag(tag_name).await?.map(|t| t.snapshot))
    }

    #[deprecated(since = "0.1.0", note = "renamed to get_snapshot")]
    pub async fn get(&self, tag_name: &str) -> crate::Result<Option<Snapshot>> {
        self.get_snapshot(tag_name).await
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

    /// `Tag::eq` must ignore the synthetic `name` field, otherwise round-trip
    /// comparison and downstream uniqueness checks would silently misbehave.
    #[test]
    fn test_tag_eq_ignores_name() {
        let json = load_fixture("tag-2024-01-01");
        let mut a: Tag = serde_json::from_str(&json).unwrap();
        let mut b: Tag = serde_json::from_str(&json).unwrap();
        a.name = "v1".into();
        b.name = "v2".into();
        assert_eq!(a, b);
    }

    /// `Tag` flattens `Snapshot`, which only works as long as `Snapshot`
    /// tolerates unknown JSON keys. If a future change adds
    /// `#[serde(deny_unknown_fields)]` to `Snapshot`, this test fails and
    /// the brittle assumption is caught at the source.
    #[test]
    fn test_snapshot_tolerates_unknown_fields() {
        let json = serde_json::json!({
            "version": 3,
            "id": 1,
            "schemaId": 0,
            "baseManifestList": "base",
            "deltaManifestList": "delta",
            "commitUser": "u",
            "commitIdentifier": 1,
            "commitKind": "APPEND",
            "timeMillis": 1000,
            "tagCreateTime": "2024-01-01T00:00",
            "tagTimeRetained": "PT1H"
        });
        let res: Result<Snapshot, _> = serde_json::from_value(json);
        assert!(
            res.is_ok(),
            "Snapshot must tolerate unknown fields: {res:?}"
        );
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
        let names: Vec<&str> = tags.iter().map(|t| t.name()).collect();
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
