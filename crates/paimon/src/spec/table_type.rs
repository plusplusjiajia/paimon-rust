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

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use crate::error::Error;

/// Type of the table, declared by the `type` table option.
///
/// Mirrors `org.apache.paimon.TableType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TableType {
    /// Normal Paimon table.
    #[default]
    Table,
    /// A directory containing multiple files of the same format.
    FormatTable,
    /// A normal Paimon table combined with materialized SQL.
    MaterializedTable,
    /// A normal Paimon table combined with an object location.
    ObjectTable,
    /// A lance table, see <https://lancedb.github.io/lance/>.
    LanceTable,
    /// An iceberg table, see <https://iceberg.apache.org/>.
    IcebergTable,
}

impl TableType {
    /// The `type` option value of this table type.
    pub fn as_str(&self) -> &'static str {
        match self {
            TableType::Table => "table",
            TableType::FormatTable => "format-table",
            TableType::MaterializedTable => "materialized-table",
            TableType::ObjectTable => "object-table",
            TableType::LanceTable => "lance-table",
            TableType::IcebergTable => "iceberg-table",
        }
    }

    /// Whether the Paimon reader cannot serve this type, so it must be
    /// routed to a table engine (see
    /// [`Catalog::load_table_routing`](crate::catalog::Catalog::load_table_routing)).
    pub fn requires_table_engine(&self) -> bool {
        matches!(self, TableType::IcebergTable)
    }
}

impl Display for TableType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TableType {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        [
            TableType::Table,
            TableType::FormatTable,
            TableType::MaterializedTable,
            TableType::ObjectTable,
            TableType::LanceTable,
            TableType::IcebergTable,
        ]
        .into_iter()
        .find(|table_type| value.eq_ignore_ascii_case(table_type.as_str()))
        .ok_or_else(|| Error::Unsupported {
            message: format!("unknown table type: {value}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_declared_value() {
        for table_type in [
            TableType::Table,
            TableType::FormatTable,
            TableType::MaterializedTable,
            TableType::ObjectTable,
            TableType::LanceTable,
            TableType::IcebergTable,
        ] {
            assert_eq!(
                TableType::from_str(table_type.as_str()).unwrap(),
                table_type
            );
            assert_eq!(
                TableType::from_str(&table_type.as_str().to_uppercase()).unwrap(),
                table_type
            );
        }
    }

    #[test]
    fn rejects_unknown_value() {
        let err = TableType::from_str("delta-table").expect_err("unknown type must not parse");
        assert!(err.to_string().contains("delta-table"), "{err}");
    }

    #[test]
    fn only_iceberg_requires_an_engine() {
        assert!(TableType::IcebergTable.requires_table_engine());
        for table_type in [
            TableType::Table,
            TableType::FormatTable,
            TableType::MaterializedTable,
            TableType::ObjectTable,
            TableType::LanceTable,
        ] {
            assert!(!table_type.requires_table_engine(), "{table_type}");
        }
    }
}
