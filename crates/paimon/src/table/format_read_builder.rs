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

//! Read builder for Java-compatible `type=format-table` metadata.

use super::partition_filter::PartitionFilter;
use super::read_builder::resolve_projected_fields;
use super::read_builder::split_scan_predicates;
use super::{Table, TableRead, TableScan};
use crate::spec::{DataField, Predicate};
use crate::table::source::RowRange;
use crate::Result;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub(crate) struct FormatReadBuilder<'a> {
    table: &'a Table,
    read_type: Option<Vec<DataField>>,
    partition_filter: Option<PartitionFilter>,
    data_predicates: Vec<Predicate>,
    limit: Option<usize>,
    filter_columns: HashSet<usize>,
}

impl<'a> FormatReadBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Self {
        Self {
            table,
            read_type: None,
            partition_filter: None,
            data_predicates: Vec::new(),
            limit: None,
            filter_columns: HashSet::new(),
        }
    }

    pub(crate) fn with_projection(&mut self, columns: &[&str]) -> Result<&mut Self> {
        let projection_names = columns.iter().map(|c| (*c).to_string()).collect::<Vec<_>>();
        self.read_type = Some(resolve_projected_fields(
            self.table.identifier().full_name(),
            self.table.schema().fields(),
            &projection_names,
        )?);
        Ok(self)
    }

    pub(crate) fn with_read_type(&mut self, read_type: Vec<DataField>) -> &mut Self {
        self.read_type = Some(read_type);
        self
    }

    pub(crate) fn with_filter(&mut self, filter: Predicate) -> &mut Self {
        // Capture the full predicate's columns before it is split, so masked and
        // out-of-scope partition keys can't prune on their raw value.
        self.filter_columns.clear();
        filter.collect_leaf_field_indices(&mut self.filter_columns);
        let (partition_predicate, data_predicates) = split_scan_predicates(self.table, filter);
        self.partition_filter = partition_predicate.map(|pred| {
            PartitionFilter::from_predicate(pred, &self.table.schema().partition_fields())
        });
        self.data_predicates = data_predicates;
        self
    }

    pub(crate) fn is_exact_filter_pushdown(&self, _filter: &Predicate) -> bool {
        false
    }

    pub(crate) fn with_row_ranges(&mut self, _ranges: Vec<RowRange>) -> &mut Self {
        self
    }

    pub(crate) fn with_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = Some(limit);
        self
    }

    pub(crate) fn new_scan(&self) -> TableScan<'a> {
        TableScan::new(
            self.table,
            self.partition_filter.clone(),
            Vec::new(),
            None,
            self.limit,
            None,
        )
        .with_query_auth_scope(self.filter_columns.clone(), self.projected_schema_indices())
    }

    /// Table-schema indices of the projected columns (`None` = all).
    fn projected_schema_indices(&self) -> Option<Vec<usize>> {
        self.read_type.as_ref().map(|fields| {
            fields
                .iter()
                .filter_map(|f| {
                    self.table
                        .schema()
                        .fields()
                        .iter()
                        .position(|s| s.id() == f.id())
                })
                .collect()
        })
    }

    pub(crate) fn new_read(&self) -> Result<TableRead<'a>> {
        // Query-auth is enforced in `TableRead::to_arrow` off the grant stamped
        // on the splits by planning; no gate needed here (see the Paimon
        // `PaimonReadBuilder::new_read`).
        let read_type = match &self.read_type {
            None => self.table.schema().fields().to_vec(),
            Some(fields) => fields.clone(),
        };
        Ok(TableRead::new_format(
            self.table,
            read_type,
            self.data_predicates.clone(),
            self.limit,
        ))
    }
}
