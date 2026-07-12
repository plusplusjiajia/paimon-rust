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

//! Query-auth enforcement: apply the REST server's per-user row filter and
//! column masking exactly to read output. Parsing of the Java `Predicate` /
//! `Transform` JSON lives in [`crate::spec`]; anything unrecognised is an
//! error, so callers keep the table fail-closed.

use crate::arrow::residual::{
    boolean_mask_from_predicate, evaluate_column_predicate, literal_scalar_for_arrow_filter,
    sanitize_filter_mask,
};
use crate::spec::{
    DataField, DataType, Datum, Predicate, PredicateOperator, Transform, TransformInput,
};
use crate::{Error, Result};
use arrow_arith::boolean::{and_kleene, not, or_kleene};
use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use std::collections::HashSet;

/// Row filters and column masks the REST server granted the current user for a
/// specific set of columns. `authorized = None` means all columns were approved
/// (the request used `select = all`); `Some(set)` scopes the grant to those
/// table-schema indices. Only the REST catalog constructs grants.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryAuthGrant {
    filters: Vec<Predicate>,
    masks: Vec<ColumnMask>,
    authorized: Option<HashSet<usize>>,
}

impl QueryAuthGrant {
    pub(crate) fn new(
        filters: Vec<Predicate>,
        masks: Vec<ColumnMask>,
        authorized: Option<HashSet<usize>>,
    ) -> Self {
        Self {
            filters,
            masks,
            authorized,
        }
    }

    /// Fully unrestricted: every column approved, no filter, no masking.
    pub(crate) fn is_unrestricted(&self) -> bool {
        self.authorized.is_none() && self.filters.is_empty() && self.masks.is_empty()
    }

    pub(crate) fn filters(&self) -> &[Predicate] {
        &self.filters
    }

    pub(crate) fn masks(&self) -> &[ColumnMask] {
        &self.masks
    }

    /// Whether every table-schema index in `columns` was authorized. A grant
    /// scoped to a subset does not authorize columns outside it, so a wider
    /// projection or a predicate on an un-approved column fails closed.
    pub(crate) fn authorizes_columns(&self, columns: impl IntoIterator<Item = usize>) -> bool {
        match &self.authorized {
            None => true,
            Some(set) => columns.into_iter().all(|c| set.contains(&c)),
        }
    }

    /// Whether this grant carries a row filter. Such a filter is applied as a
    /// residual pass in `TableRead::to_arrow`, so split row counts, count
    /// statistics, and count-based limit pushdown are no longer exact.
    pub(crate) fn has_row_filter(&self) -> bool {
        !self.filters.is_empty()
    }

    /// Table-schema indices of columns this grant masks (empty when no masking).
    /// Callers must not push predicates on these columns to scan pruning, which
    /// would leak the raw value via row presence.
    pub(crate) fn masked_columns(&self) -> Vec<usize> {
        self.masks.iter().map(|m| m.column).collect()
    }

    /// The first of `columns` (table-schema indices) this grant does not
    /// authorize, if any.
    pub(crate) fn first_unauthorized(
        &self,
        columns: impl IntoIterator<Item = usize>,
    ) -> Option<usize> {
        columns.into_iter().find(|c| !self.authorizes_columns([*c]))
    }

    /// Field IDs the grant must physically read (row-filter columns, mask
    /// targets, and mask inputs). Scan projection planning must include these so
    /// data-evolution column-slice pruning does not drop files that hold them
    /// (an omitted column would read as null and wrongly satisfy `IS_NULL`).
    pub(crate) fn read_field_ids(&self, fields: &[DataField]) -> Vec<i32> {
        let mut indices = HashSet::new();
        for filter in &self.filters {
            filter.collect_leaf_field_indices(&mut indices);
        }
        for mask in &self.masks {
            indices.insert(mask.column);
            mask.transform.collect_field_indices(&mut indices);
        }
        indices
            .into_iter()
            .filter_map(|i| fields.get(i).map(|f| f.id()))
            .collect()
    }
}

/// Mask one column (by table-schema index) with a transform.
#[derive(Debug, Clone)]
pub(crate) struct ColumnMask {
    pub(crate) column: usize,
    pub(crate) transform: Transform,
}

fn unsupported(message: String) -> Error {
    Error::Unsupported { message }
}

fn field_name(fields: &[DataField], column: usize) -> &str {
    fields.get(column).map(|f| f.name()).unwrap_or("?")
}

/// Fail-closed error for a caller predicate on a masked column (would leak the
/// raw value via row selection). Shared by every builder/scan choke point.
pub(crate) fn masked_filter_error(fields: &[DataField], column: usize) -> Error {
    unsupported(format!(
        "cannot filter on masked column `{}`",
        field_name(fields, column)
    ))
}

/// Fail-closed error for a read that touches a column outside the grant's scope.
pub(crate) fn unauthorized_column_error(fields: &[DataField], column: usize) -> Error {
    unsupported(format!(
        "query-auth read touches column `{}` outside the authorized set",
        field_name(fields, column)
    ))
}

/// Live query-auth scope check shared by the read/scan gates: fail closed when
/// the caller filter references a masked column (pruning on its raw value would
/// leak it) or when the projection/filter touches a column outside the grant's
/// authorized scope. `projected = None` means all columns.
pub(crate) fn scope_check(
    grant: &QueryAuthGrant,
    fields: &[DataField],
    filter_columns: &HashSet<usize>,
    projected: Option<Vec<usize>>,
) -> crate::Result<()> {
    if let Some(column) = grant
        .masked_columns()
        .into_iter()
        .find(|c| filter_columns.contains(c))
    {
        return Err(masked_filter_error(fields, column));
    }
    let projected = projected.unwrap_or_else(|| (0..fields.len()).collect());
    if let Some(column) =
        grant.first_unauthorized(projected.into_iter().chain(filter_columns.iter().copied()))
    {
        return Err(unauthorized_column_error(fields, column));
    }
    Ok(())
}

/// Parse the auth response's JSON filter strings into predicates whose leaf
/// indices refer to `fields` (table-schema order). Empty strings are skipped
/// (Java parity); anything unparseable is an error.
pub(crate) fn parse_auth_filters(
    filters: &[String],
    fields: &[DataField],
) -> Result<Vec<Predicate>> {
    filters
        .iter()
        .filter(|f| !f.trim().is_empty())
        .map(|f| Predicate::from_rest_json(f, fields))
        .collect()
}

// ==================== Exact evaluation ====================

/// Exactly evaluate the ANDed `predicates` against `batch` (whose columns
/// correspond 1:1 to `batch_fields`; leaf indices refer to `schema_fields`)
/// and drop non-matching rows. Unlike the pruning evaluators, anything that
/// cannot be evaluated is an error — a security filter must not fall open.
pub(crate) fn strict_filter_batch(
    batch: &RecordBatch,
    predicates: &[Predicate],
    schema_fields: &[DataField],
    batch_fields: &[DataField],
) -> Result<RecordBatch> {
    let mut combined: Option<BooleanArray> = None;
    for predicate in predicates {
        let mask = strict_mask(batch, predicate, schema_fields, batch_fields)?;
        combined = Some(match combined {
            Some(existing) => kleene(and_kleene(&existing, &mask))?,
            None => mask,
        });
    }
    let Some(mask) = combined else {
        return Ok(batch.clone());
    };
    let mask = sanitize_filter_mask(mask);
    arrow_select::filter::filter_record_batch(batch, &mask).map_err(|e| Error::DataInvalid {
        message: format!("failed to apply query-auth row filter: {e}"),
        source: Some(Box::new(e)),
    })
}

fn strict_mask(
    batch: &RecordBatch,
    predicate: &Predicate,
    schema_fields: &[DataField],
    batch_fields: &[DataField],
) -> Result<BooleanArray> {
    match predicate {
        Predicate::AlwaysTrue => Ok(BooleanArray::from(vec![true; batch.num_rows()])),
        Predicate::AlwaysFalse => Ok(BooleanArray::from(vec![false; batch.num_rows()])),
        Predicate::And(children) => fold_masks(batch, children, schema_fields, batch_fields, true),
        Predicate::Or(children) => fold_masks(batch, children, schema_fields, batch_fields, false),
        Predicate::Not(inner) => {
            let mask = strict_mask(batch, inner, schema_fields, batch_fields)?;
            kleene(not(&mask))
        }
        Predicate::Leaf {
            index,
            op,
            literals,
            ..
        } => {
            let field = schema_fields.get(*index).ok_or_else(|| {
                unsupported(format!(
                    "query-auth filter references unknown field #{index}"
                ))
            })?;
            let position = batch_fields
                .iter()
                .position(|f| f.id() == field.id() && f.name() == field.name())
                .ok_or_else(|| {
                    unsupported(format!(
                        "query-auth filter field `{}` missing from read",
                        field.name()
                    ))
                })?;
            strict_leaf_mask(batch.column(position), field.data_type(), *op, literals)
        }
    }
}

fn fold_masks(
    batch: &RecordBatch,
    children: &[Predicate],
    schema_fields: &[DataField],
    batch_fields: &[DataField],
    use_and: bool,
) -> Result<BooleanArray> {
    let mut combined: Option<BooleanArray> = None;
    for child in children {
        let mask = strict_mask(batch, child, schema_fields, batch_fields)?;
        combined = Some(match combined {
            Some(existing) if use_and => kleene(and_kleene(&existing, &mask))?,
            Some(existing) => kleene(or_kleene(&existing, &mask))?,
            None => mask,
        });
    }
    combined.ok_or_else(|| unsupported("query-auth filter has an empty compound".to_string()))
}

fn strict_leaf_mask(
    column: &ArrayRef,
    data_type: &DataType,
    op: PredicateOperator,
    literals: &[Datum],
) -> Result<BooleanArray> {
    let scalar = |literal: &Datum| -> Result<arrow_array::Scalar<ArrayRef>> {
        literal_scalar_for_arrow_filter(literal, data_type)?.ok_or_else(|| {
            unsupported(format!(
                "query-auth filter literal is not comparable to type {data_type:?}"
            ))
        })
    };
    match op {
        PredicateOperator::IsNull => Ok(boolean_mask_from_predicate(column.len(), |row| {
            column.is_null(row)
        })),
        PredicateOperator::IsNotNull => Ok(boolean_mask_from_predicate(column.len(), |row| {
            column.is_valid(row)
        })),
        PredicateOperator::In | PredicateOperator::NotIn => {
            // Kleene IN: OR of equalities; x NOT IN (..) = NOT(IN), nulls stay null.
            let mut combined = BooleanArray::from(vec![false; column.len()]);
            for literal in literals {
                let eq = kleene(evaluate_column_predicate(
                    column,
                    &scalar(literal)?,
                    PredicateOperator::Eq,
                ))?;
                combined = kleene(or_kleene(&combined, &eq))?;
            }
            if matches!(op, PredicateOperator::NotIn) {
                combined = kleene(not(&combined))?;
                if literals.is_empty() {
                    // `x NOT IN ()` is true only for non-null rows.
                    combined =
                        boolean_mask_from_predicate(column.len(), |row| column.is_valid(row));
                }
            }
            Ok(combined)
        }
        PredicateOperator::Eq
        | PredicateOperator::NotEq
        | PredicateOperator::Lt
        | PredicateOperator::LtEq
        | PredicateOperator::Gt
        | PredicateOperator::GtEq
        | PredicateOperator::StartsWith
        | PredicateOperator::EndsWith
        | PredicateOperator::Contains
        | PredicateOperator::Like => {
            let literal = literals.first().ok_or_else(|| {
                unsupported("query-auth filter comparison without literal".to_string())
            })?;
            kleene(evaluate_column_predicate(column, &scalar(literal)?, op))
        }
        PredicateOperator::Between | PredicateOperator::NotBetween => {
            let (Some(low), Some(high)) = (literals.first(), literals.get(1)) else {
                return Err(unsupported(
                    "query-auth BETWEEN filter without bounds".to_string(),
                ));
            };
            let lo = kleene(evaluate_column_predicate(
                column,
                &scalar(low)?,
                PredicateOperator::GtEq,
            ))?;
            let hi = kleene(evaluate_column_predicate(
                column,
                &scalar(high)?,
                PredicateOperator::LtEq,
            ))?;
            let between = kleene(and_kleene(&lo, &hi))?;
            if matches!(op, PredicateOperator::NotBetween) {
                kleene(not(&between))
            } else {
                Ok(between)
            }
        }
    }
}

fn kleene(
    result: std::result::Result<BooleanArray, arrow_schema::ArrowError>,
) -> Result<BooleanArray> {
    result.map_err(|e| Error::DataInvalid {
        message: format!("failed to evaluate query-auth row filter: {e}"),
        source: Some(Box::new(e)),
    })
}

// ==================== Column masking ====================

fn mask_err(detail: impl std::fmt::Display) -> Error {
    unsupported(format!("cannot parse query-auth column masking: {detail}"))
}

/// Parse the auth response's `columnMasking` map (column name -> Java
/// `Transform` JSON) against `fields` (table-schema order).
pub(crate) fn parse_column_masking(
    masking: &std::collections::HashMap<String, String>,
    fields: &[DataField],
) -> Result<Vec<ColumnMask>> {
    let mut masks = Vec::with_capacity(masking.len());
    for (column, json) in masking {
        let target = fields
            .iter()
            .position(|f| f.name() == column)
            .ok_or_else(|| mask_err(format!("unknown field `{column}`")))?;
        let transform = Transform::from_rest_json(json, fields)?;
        // The masked value replaces the column in place, so its type must match
        // the column's; a type-changing transform (e.g. `CAST(id AS STRING)` on
        // an INT column) cannot be represented and must fail closed rather than
        // be cast back to the raw type. Types are compared via their arrow
        // representation (nullability-agnostic).
        if let Some(out) = mask_output_type(&transform, fields) {
            let target_type = crate::arrow::paimon_type_to_arrow(fields[target].data_type())?;
            if out != target_type {
                return Err(mask_err(format!(
                    "masking `{column}` produces {out:?} but the column is {target_type:?}"
                )));
            }
        }
        // A mask that can yield null on a NOT NULL column would leave the output
        // batch's schema claiming non-nullable, letting engines fold
        // `col IS [NOT] NULL` before the masked-predicate guard. Fail closed.
        if !fields[target].data_type().is_nullable() && mask_can_be_null(&transform, fields) {
            return Err(mask_err(format!(
                "masking `{column}` can produce null but the column is NOT NULL"
            )));
        }
        masks.push(ColumnMask {
            column: target,
            transform,
        });
    }
    // Deterministic order regardless of map iteration.
    masks.sort_by_key(|m| m.column);
    Ok(masks)
}

/// Whether a mask transform can produce a null value.
fn mask_can_be_null(transform: &Transform, fields: &[DataField]) -> bool {
    let field_nullable = |index: &usize| fields[*index].data_type().is_nullable();
    let input_nullable = |inputs: &[TransformInput]| {
        inputs.iter().any(|i| match i {
            TransformInput::Literal(literal) => literal.is_none(),
            TransformInput::Field(index) => field_nullable(index),
        })
    };
    match transform {
        Transform::Null => true,
        Transform::FieldRef(index) | Transform::Cast(index, _) => field_nullable(index),
        // CONCAT_WS skips null values, but a null separator still yields null.
        Transform::Upper(inputs)
        | Transform::Lower(inputs)
        | Transform::Concat(inputs)
        | Transform::ConcatWs(inputs) => input_nullable(inputs),
    }
}

/// Arrow output type of a mask transform, or `None` when it always matches the
/// target column (the `NULL` transform builds a null of the column's own type).
fn mask_output_type(transform: &Transform, fields: &[DataField]) -> Option<arrow_schema::DataType> {
    let of = |index: &usize| crate::arrow::paimon_type_to_arrow(fields[*index].data_type()).ok();
    match transform {
        Transform::Null => None,
        Transform::FieldRef(index) => of(index),
        Transform::Cast(_, to) => crate::arrow::paimon_type_to_arrow(to).ok(),
        Transform::Upper(_)
        | Transform::Lower(_)
        | Transform::Concat(_)
        | Transform::ConcatWs(_) => Some(arrow_schema::DataType::Utf8),
    }
}

/// Overwrite masked columns of `batch` (whose columns correspond 1:1 to
/// `batch_fields`). Masks whose target column is not in the batch are skipped
/// (Java parity); anything that cannot be evaluated is an error.
pub(crate) fn mask_batch(
    batch: &RecordBatch,
    masks: &[ColumnMask],
    schema_fields: &[DataField],
    batch_fields: &[DataField],
) -> Result<RecordBatch> {
    use arrow_array::new_null_array;

    // Nothing to mask (e.g. a `COUNT(*)` read projects no columns); return the
    // batch unchanged, preserving its row count even with zero columns.
    if masks.is_empty() {
        return Ok(batch.clone());
    }

    // All batch positions holding a given table-schema field (a projection may
    // repeat a column, so every copy must be masked, not just the first).
    let positions_of = |schema_index: usize| -> Result<Vec<usize>> {
        let field = schema_fields.get(schema_index).ok_or_else(|| {
            unsupported(format!(
                "query-auth mask references unknown field #{schema_index}"
            ))
        })?;
        Ok(batch_fields
            .iter()
            .enumerate()
            .filter(|(_, f)| f.id() == field.id() && f.name() == field.name())
            .map(|(pos, _)| pos)
            .collect())
    };
    let input_column = |schema_index: usize| -> Result<ArrayRef> {
        positions_of(schema_index)?
            .first()
            .map(|pos| batch.column(*pos).clone())
            .ok_or_else(|| unsupported("query-auth mask input missing from read".to_string()))
    };

    let mut columns = batch.columns().to_vec();
    for mask in masks {
        let targets = positions_of(mask.column)?;
        let Some(&first) = targets.first() else {
            continue; // masked column not projected
        };
        let target_type = batch.schema().field(first).data_type().clone();
        let masked: ArrayRef = match &mask.transform {
            Transform::Null => new_null_array(&target_type, batch.num_rows()),
            Transform::FieldRef(index) => input_column(*index)?,
            Transform::Cast(index, to) => {
                let to_arrow = crate::arrow::paimon_type_to_arrow(to)?;
                cast_masked(&input_column(*index)?, &to_arrow)?
            }
            Transform::Upper(inputs) => string_mask(batch, inputs, &input_column, |v| {
                Some(v.first()?.as_ref().map(|s| s.to_uppercase()))
            })?,
            Transform::Lower(inputs) => string_mask(batch, inputs, &input_column, |v| {
                Some(v.first()?.as_ref().map(|s| s.to_lowercase()))
            })?,
            // SQL semantics: CONCAT is null if any input is null; CONCAT_WS
            // uses the first input as separator and skips null values.
            Transform::Concat(inputs) => string_mask(batch, inputs, &input_column, |v| {
                Some(
                    v.iter()
                        .cloned()
                        .collect::<Option<Vec<_>>>()
                        .map(|p| p.concat()),
                )
            })?,
            Transform::ConcatWs(inputs) => string_mask(batch, inputs, &input_column, |v| {
                let (sep, rest) = v.split_first()?;
                Some(
                    sep.as_ref()
                        .map(|sep| rest.iter().flatten().cloned().collect::<Vec<_>>().join(sep)),
                )
            })?,
        };
        // Parse-time type checks guarantee a compatible type, so this only
        // aligns arrow representations. Mask every copy of the target column.
        let masked = cast_masked(&masked, &target_type)?;
        for pos in targets {
            columns[pos] = masked.clone();
        }
    }
    RecordBatch::try_new_with_options(
        batch.schema(),
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| Error::DataInvalid {
        message: format!("failed to apply query-auth column masking: {e}"),
        source: Some(Box::new(e)),
    })
}

fn cast_masked(array: &ArrayRef, to: &arrow_schema::DataType) -> Result<ArrayRef> {
    if array.data_type() == to {
        return Ok(array.clone());
    }
    arrow_cast::cast(array, to).map_err(|e| {
        unsupported(format!(
            "query-auth mask value of type {:?} cannot be cast to column type {to:?}: {e}",
            array.data_type()
        ))
    })
}

/// Evaluate a string transform row by row. `combine` receives the resolved
/// inputs (None = SQL NULL) and returns the masked value; a `None` from
/// `combine` means the transform is malformed for this input arity.
fn string_mask(
    batch: &RecordBatch,
    inputs: &[TransformInput],
    input_column: &dyn Fn(usize) -> Result<ArrayRef>,
    combine: impl Fn(&[Option<String>]) -> Option<Option<String>>,
) -> Result<ArrayRef> {
    use arrow_array::{Array, StringArray};
    use std::sync::Arc;

    // Resolve field inputs once, as string arrays (None = literal slot).
    let resolved: Vec<Option<ArrayRef>> = inputs
        .iter()
        .map(|input| match input {
            TransformInput::Literal(_) => Ok(None),
            TransformInput::Field(index) => {
                cast_masked(&input_column(*index)?, &arrow_schema::DataType::Utf8).map(Some)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let mut values: Vec<Option<String>> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let row_inputs = inputs
            .iter()
            .zip(&resolved)
            .map(|(input, array)| match (input, array) {
                (TransformInput::Literal(s), _) => Ok(s.clone()),
                (TransformInput::Field(_), Some(array)) => {
                    let strings = array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| unsupported("mask input is not a string".to_string()))?;
                    Ok((!strings.is_null(row)).then(|| strings.value(row).to_string()))
                }
                (TransformInput::Field(_), None) => unreachable!("field inputs are resolved above"),
            })
            .collect::<Result<Vec<_>>>()?;
        let value = combine(&row_inputs)
            .ok_or_else(|| unsupported("query-auth string mask is malformed".to_string()))?;
        values.push(value);
    }
    Ok(Arc::new(StringArray::from(values)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{IntType, VarCharType};
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn fields() -> Vec<DataField> {
        vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(
                1,
                "name".to_string(),
                DataType::VarChar(VarCharType::new(255).unwrap()),
            ),
        ]
    }

    fn leaf_json(function: &str, field: &str, literals: &str) -> String {
        format!(
            r#"{{"kind":"LEAF","transform":{{"name":"FIELD_REF","fieldRef":{{"index":0,"name":"{field}","type":"INT"}}}},"function":"{function}","literals":{literals}}}"#
        )
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    Some("d"),
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_strict_filter_batch_filters_rows() {
        let fields = fields();
        let filters =
            parse_auth_filters(&[leaf_json("GREATER_THAN", "id", "[1]")], &fields).unwrap();
        let filtered = strict_filter_batch(&batch(), &filters, &fields, &fields).unwrap();
        // id > 1 keeps rows 2 and 4; the NULL row is excluded.
        assert_eq!(filtered.num_rows(), 2);
    }

    #[test]
    fn test_strict_filter_not_excludes_nulls() {
        let fields = fields();
        // NOT (id = 2): NULL rows must stay excluded (SQL three-valued logic).
        let json = format!(
            r#"{{"kind":"COMPOUND","function":"AND","children":[{}]}}"#,
            leaf_json("NOT_EQUAL", "id", "[2]")
        );
        let filters = parse_auth_filters(&[json], &fields).unwrap();
        let filtered = strict_filter_batch(&batch(), &filters, &fields, &fields).unwrap();
        assert_eq!(
            filtered.num_rows(),
            2,
            "rows 1 and 4 only, not the NULL row"
        );
    }

    /// Java #7034 baseline: filter each supported literal type exactly.
    #[test]
    fn test_strict_filter_batch_typed_matrix() {
        use crate::spec::{BigIntType, BooleanType, DoubleType, FloatType};
        use arrow_array::{BooleanArray, Float32Array, Float64Array, Int64Array};

        let fields = vec![
            DataField::new(0, "id".to_string(), DataType::Int(IntType::new())),
            DataField::new(1, "age".to_string(), DataType::BigInt(BigIntType::new())),
            DataField::new(2, "salary".to_string(), DataType::Double(DoubleType::new())),
            DataField::new(
                3,
                "is_active".to_string(),
                DataType::Boolean(BooleanType::new()),
            ),
            DataField::new(4, "score".to_string(), DataType::Float(FloatType::new())),
            DataField::new(
                5,
                "name".to_string(),
                DataType::VarChar(VarCharType::new(255).unwrap()),
            ),
        ];
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("age", ArrowDataType::Int64, true),
            ArrowField::new("salary", ArrowDataType::Float64, true),
            ArrowField::new("is_active", ArrowDataType::Boolean, true),
            ArrowField::new("score", ArrowDataType::Float32, true),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![25, 30, 35, 28])),
                Arc::new(Float64Array::from(vec![50000.0, 60000.0, 70000.0, 55000.0])),
                Arc::new(BooleanArray::from(vec![true, false, true, true])),
                Arc::new(Float32Array::from(vec![85.5, 90.0, 95.5, 88.0])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David"])),
            ],
        )
        .unwrap();
        fn typed_leaf(function: &str, field: &str, ftype: &str, literals: &str) -> String {
            format!(
                r#"{{"kind":"LEAF","transform":{{"name":"FIELD_REF","fieldRef":{{"index":0,"name":"{field}","type":"{ftype}"}}}},"function":"{function}","literals":{literals}}}"#
            )
        }
        // (filter, expected surviving ids) — mirrors Java MockRESTCatalogTest.
        let cases: Vec<(String, Vec<i32>)> = vec![
            (typed_leaf("GREATER_THAN", "id", "INT", "[2]"), vec![3, 4]),
            (
                typed_leaf("GREATER_OR_EQUAL", "age", "BIGINT", "[30]"),
                vec![2, 3],
            ),
            (
                typed_leaf("GREATER_THAN", "salary", "DOUBLE", "[55000.0]"),
                vec![2, 3],
            ),
            (
                typed_leaf("EQUAL", "is_active", "BOOLEAN", "[true]"),
                vec![1, 3, 4],
            ),
            (
                typed_leaf("GREATER_OR_EQUAL", "score", "FLOAT", "[90.0]"),
                vec![2, 3],
            ),
            (
                typed_leaf("EQUAL", "name", "STRING", "[\"Alice\"]"),
                vec![1],
            ),
            (
                // Two predicates ANDed by the grant list semantics.
                format!(
                    r#"{{"kind":"COMPOUND","function":"AND","children":[{},{}]}}"#,
                    typed_leaf("GREATER_OR_EQUAL", "age", "BIGINT", "[30]"),
                    typed_leaf("EQUAL", "is_active", "BOOLEAN", "[true]")
                ),
                vec![3],
            ),
        ];
        for (json, expected) in cases {
            let filters = parse_auth_filters(std::slice::from_ref(&json), &fields).unwrap();
            let filtered = strict_filter_batch(&batch, &filters, &fields, &fields).unwrap();
            let ids = filtered
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec();
            assert_eq!(ids, expected, "for {json}");
        }
    }

    fn masking(column: &str, json: &str) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::from([(column.to_string(), json.to_string())])
    }

    #[test]
    fn test_parse_column_masking() {
        let fields = fields();
        let masks = parse_column_masking(&masking("name", r#"{"name":"NULL"}"#), &fields).unwrap();
        assert!(matches!(masks[0].transform, Transform::Null));
        assert_eq!(masks[0].column, 1);

        let upper = r#"{"name":"UPPER","inputs":[{"index":1,"name":"name","type":"STRING"}]}"#;
        let masks = parse_column_masking(&masking("name", upper), &fields).unwrap();
        assert!(matches!(&masks[0].transform, Transform::Upper(inputs) if inputs.len() == 1));

        // Unknown transform / unknown column / bad JSON: all fail closed.
        for (column, json) in [
            ("name", r#"{"name":"ROT13"}"#),
            ("missing", r#"{"name":"NULL"}"#),
            ("name", "not json"),
        ] {
            assert!(parse_column_masking(&masking(column, json), &fields).is_err());
        }
    }

    #[test]
    fn test_mask_batch_null_and_string_transforms() {
        use arrow_array::Array;
        let fields = fields();

        // NULL mask: the whole column becomes null.
        let masks = parse_column_masking(&masking("name", r#"{"name":"NULL"}"#), &fields).unwrap();
        let masked = mask_batch(&batch(), &masks, &fields, &fields).unwrap();
        assert_eq!(masked.column(1).null_count(), 4);
        assert_eq!(masked.column(0).null_count(), 1, "other columns untouched");

        // CONCAT_WS("-", literal, field): "x-a", "x-b", ...
        let concat =
            r#"{"name":"CONCAT_WS","inputs":["-","x",{"index":1,"name":"name","type":"STRING"}]}"#;
        let masks = parse_column_masking(&masking("name", concat), &fields).unwrap();
        let masked = mask_batch(&batch(), &masks, &fields, &fields).unwrap();
        let names = masked
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "x-a");

        // Masked column absent from the batch: skipped.
        let one_col = batch().project(&[0]).unwrap();
        let one_field = vec![fields[0].clone()];
        let masks = parse_column_masking(&masking("name", r#"{"name":"NULL"}"#), &fields).unwrap();
        assert_eq!(
            mask_batch(&one_col, &masks, &fields, &one_field)
                .unwrap()
                .num_columns(),
            1
        );
    }

    #[test]
    fn test_reject_type_changing_cast_mask() {
        let fields = fields();
        // CAST(id AS STRING) on an INT column changes type -> fail closed.
        let cast =
            r#"{"name":"CAST","fieldRef":{"index":0,"name":"id","type":"INT"},"type":"STRING"}"#;
        assert!(parse_column_masking(&masking("id", cast), &fields).is_err());
        // A string transform on a non-string column also fails closed.
        let upper = r#"{"name":"UPPER","inputs":[{"index":0,"name":"id","type":"INT"}]}"#;
        assert!(parse_column_masking(&masking("id", upper), &fields).is_err());
    }

    #[test]
    fn test_mask_batch_masks_every_duplicate_target() {
        use arrow_array::Array;
        let fields = fields();
        let masks = parse_column_masking(&masking("name", r#"{"name":"NULL"}"#), &fields).unwrap();
        // A projection that repeats the masked column: both copies must be masked.
        let base = batch();
        let dup = base.project(&[1, 1]).unwrap();
        let dup_fields = vec![fields[1].clone(), fields[1].clone()];
        let masked = mask_batch(&dup, &masks, &fields, &dup_fields).unwrap();
        assert_eq!(masked.column(0).null_count(), 4);
        assert_eq!(masked.column(1).null_count(), 4);
    }

    #[test]
    fn test_grant_authorized_column_scope() {
        // A grant scoped to a subset is not globally unrestricted and rejects
        // columns outside the approved set.
        let grant = QueryAuthGrant::new(Vec::new(), Vec::new(), Some(HashSet::from([0])));
        assert!(!grant.is_unrestricted());
        assert!(grant.authorizes_columns([0]));
        assert!(!grant.authorizes_columns([1]));
        // `None` (all columns) with no filter/mask is fully unrestricted.
        let all = QueryAuthGrant::new(Vec::new(), Vec::new(), None);
        assert!(all.is_unrestricted());
        assert!(all.authorizes_columns([0, 1, 99]));
    }
}
