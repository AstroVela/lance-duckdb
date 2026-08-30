use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;

use arrow_array::builder::{FixedSizeListBuilder, Float32Builder, Float64Builder};
#[cfg(feature = "vane-distributed")]
use arrow_array::MapArray;
use arrow_array::{
    make_array, Array, FixedSizeListArray, Float32Array, Float64Array, LargeListArray, ListArray,
    RecordBatch, RecordBatchReader, StructArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
#[cfg(feature = "vane-distributed")]
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{CommitBuilder, Dataset, InsertBuilder, WriteMode, WriteParams};
use lance::io::{ObjectStoreParams, StorageOptionsAccessor};

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;

use super::session::record_commit;
#[cfg(feature = "vane-distributed")]
use super::util::with_explicit_aws_credentials;
use super::util::{cstr_to_str, optional_session_handle, slice_from_ptr, FfiError, FfiResult};

#[repr(C)]
struct RawArrowArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *mut *const c_void,
    children: *mut *mut RawArrowArray,
    dictionary: *mut RawArrowArray,
    release: Option<unsafe extern "C" fn(arg1: *mut RawArrowArray)>,
    private_data: *mut c_void,
}

struct ReceiverRecordBatchReader {
    schema: SchemaRef,
    receiver: Receiver<RecordBatch>,
}

impl ReceiverRecordBatchReader {
    fn new(schema: SchemaRef, receiver: Receiver<RecordBatch>) -> Self {
        Self { schema, receiver }
    }
}

impl Iterator for ReceiverRecordBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(batch) => Some(Ok(batch)),
            Err(_) => None,
        }
    }
}

impl RecordBatchReader for ReceiverRecordBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct WriterHandle {
    input_schema: SchemaRef,
    data_type: DataType,
    state: Mutex<WriterState>,
    batches_sent: AtomicU64,
}

enum WriterResult {
    Committed,
    Uncommitted(Box<lance::dataset::transaction::Transaction>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterKind {
    Committed,
    Uncommitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VectorListKind {
    List,
    LargeList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VectorElementType {
    Float32,
    Float64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VectorConversion {
    col_idx: usize,
    dim: usize,
    list_kind: VectorListKind,
    element_type: VectorElementType,
}

struct WriterState {
    kind: WriterKind,
    path: String,
    #[cfg(feature = "vane-distributed")]
    dataset: Option<Arc<Dataset>>,
    #[cfg(feature = "vane-distributed")]
    frozen_target_schema: Option<SchemaRef>,
    params: WriteParams,

    vector_candidates: Vec<VectorConversion>,
    buffered_batches: Vec<RecordBatch>,

    output_schema: Option<SchemaRef>,
    output_sender: Option<SyncSender<RecordBatch>>,
    output_join: Option<JoinHandle<Result<WriterResult, String>>>,
}

impl Drop for WriterHandle {
    fn drop(&mut self) {
        let (sender, join) = {
            let mut guard = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            (guard.output_sender.take(), guard.output_join.take())
        };
        drop(sender);
        if let Some(join) = join {
            let _ = join.join();
        }
    }
}

const MAX_VECTOR_DIM_INFERENCE_BATCHES: usize = 4;

fn is_variable_list_vector_type(dt: &DataType) -> Option<(VectorListKind, VectorElementType)> {
    match dt {
        DataType::List(field) => match field.data_type() {
            DataType::Float32 => Some((VectorListKind::List, VectorElementType::Float32)),
            DataType::Float64 => Some((VectorListKind::List, VectorElementType::Float64)),
            _ => None,
        },
        DataType::LargeList(field) => match field.data_type() {
            DataType::Float32 => Some((VectorListKind::LargeList, VectorElementType::Float32)),
            DataType::Float64 => Some((VectorListKind::LargeList, VectorElementType::Float64)),
            _ => None,
        },
        _ => None,
    }
}

fn infer_vector_dim_from_array(
    array: &dyn Array,
    list_kind: VectorListKind,
) -> Option<Result<usize, String>> {
    match list_kind {
        VectorListKind::List => {
            let list = array.as_any().downcast_ref::<ListArray>()?;
            for i in 0..list.len() {
                if list.is_null(i) {
                    continue;
                }
                let dim = list.value_length(i) as usize;
                if dim == 0 {
                    return Some(Err("vector dim must be non-zero".to_string()));
                }
                return Some(Ok(dim));
            }
            None
        }
        VectorListKind::LargeList => {
            let list = array.as_any().downcast_ref::<LargeListArray>()?;
            for i in 0..list.len() {
                if list.is_null(i) {
                    continue;
                }
                let dim = list.value_length(i) as usize;
                if dim == 0 {
                    return Some(Err("vector dim must be non-zero".to_string()));
                }
                return Some(Ok(dim));
            }
            None
        }
    }
}

fn validate_list_vector_dim(
    array: &dyn Array,
    list_kind: VectorListKind,
    expected_dim: usize,
) -> Result<(), String> {
    match list_kind {
        VectorListKind::List => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "vector column is not ListArray".to_string())?;
            for i in 0..list.len() {
                if list.is_null(i) {
                    continue;
                }
                let dim = list.value_length(i) as usize;
                if dim != expected_dim {
                    return Err(format!(
                        "vector dim mismatch: expected {expected_dim} got {dim}"
                    ));
                }
            }
            Ok(())
        }
        VectorListKind::LargeList => {
            let list = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| "vector column is not LargeListArray".to_string())?;
            for i in 0..list.len() {
                if list.is_null(i) {
                    continue;
                }
                let dim = list.value_length(i) as usize;
                if dim != expected_dim {
                    return Err(format!(
                        "vector dim mismatch: expected {expected_dim} got {dim}"
                    ));
                }
            }
            Ok(())
        }
    }
}

fn convert_list_array_to_fixed_size(
    array: &dyn Array,
    list_kind: VectorListKind,
    element_type: VectorElementType,
    dim: usize,
) -> Result<FixedSizeListArray, String> {
    let dim_i32 = i32::try_from(dim).map_err(|_| "vector dim is too large".to_string())?;

    match (list_kind, element_type) {
        (VectorListKind::List, VectorElementType::Float32) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "vector column is not ListArray".to_string())?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "vector values are not Float32".to_string())?;
            let field = match list.data_type() {
                DataType::List(field) => field.clone(),
                _ => return Err("vector column has unexpected data type".to_string()),
            };

            let mut builder =
                FixedSizeListBuilder::with_capacity(Float32Builder::new(), dim_i32, list.len())
                    .with_field(field);
            let offsets = list.value_offsets();
            for (i, start) in offsets.iter().take(list.len()).enumerate() {
                if list.is_null(i) {
                    for _ in 0..dim {
                        builder.values().append_null();
                    }
                    builder.append(false);
                    continue;
                }
                let len = list.value_length(i) as usize;
                if len != dim {
                    return Err(format!("vector dim mismatch: expected {dim} got {len}"));
                }
                let start = *start as usize;
                for j in 0..dim {
                    let idx = start + j;
                    if idx >= values.len() {
                        return Err("vector offsets are out of bounds".to_string());
                    }
                    if values.is_null(idx) {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(values.value(idx));
                    }
                }
                builder.append(true);
            }
            Ok(builder.finish())
        }
        (VectorListKind::List, VectorElementType::Float64) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| "vector column is not ListArray".to_string())?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "vector values are not Float64".to_string())?;
            let field = match list.data_type() {
                DataType::List(field) => field.clone(),
                _ => return Err("vector column has unexpected data type".to_string()),
            };

            let mut builder =
                FixedSizeListBuilder::with_capacity(Float64Builder::new(), dim_i32, list.len())
                    .with_field(field);
            let offsets = list.value_offsets();
            for (i, start) in offsets.iter().take(list.len()).enumerate() {
                if list.is_null(i) {
                    for _ in 0..dim {
                        builder.values().append_null();
                    }
                    builder.append(false);
                    continue;
                }
                let len = list.value_length(i) as usize;
                if len != dim {
                    return Err(format!("vector dim mismatch: expected {dim} got {len}"));
                }
                let start = *start as usize;
                for j in 0..dim {
                    let idx = start + j;
                    if idx >= values.len() {
                        return Err("vector offsets are out of bounds".to_string());
                    }
                    if values.is_null(idx) {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(values.value(idx));
                    }
                }
                builder.append(true);
            }
            Ok(builder.finish())
        }
        (VectorListKind::LargeList, VectorElementType::Float32) => {
            let list = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| "vector column is not LargeListArray".to_string())?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "vector values are not Float32".to_string())?;
            let field = match list.data_type() {
                DataType::LargeList(field) => field.clone(),
                _ => return Err("vector column has unexpected data type".to_string()),
            };

            let mut builder =
                FixedSizeListBuilder::with_capacity(Float32Builder::new(), dim_i32, list.len())
                    .with_field(field);
            let offsets = list.value_offsets();
            for (i, start) in offsets.iter().take(list.len()).enumerate() {
                if list.is_null(i) {
                    for _ in 0..dim {
                        builder.values().append_null();
                    }
                    builder.append(false);
                    continue;
                }
                let len = list.value_length(i) as usize;
                if len != dim {
                    return Err(format!("vector dim mismatch: expected {dim} got {len}"));
                }
                let start = *start as usize;
                for j in 0..dim {
                    let idx = start + j;
                    if idx >= values.len() {
                        return Err("vector offsets are out of bounds".to_string());
                    }
                    if values.is_null(idx) {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(values.value(idx));
                    }
                }
                builder.append(true);
            }
            Ok(builder.finish())
        }
        (VectorListKind::LargeList, VectorElementType::Float64) => {
            let list = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| "vector column is not LargeListArray".to_string())?;
            let values = list
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "vector values are not Float64".to_string())?;
            let field = match list.data_type() {
                DataType::LargeList(field) => field.clone(),
                _ => return Err("vector column has unexpected data type".to_string()),
            };

            let mut builder =
                FixedSizeListBuilder::with_capacity(Float64Builder::new(), dim_i32, list.len())
                    .with_field(field);
            let offsets = list.value_offsets();
            for (i, start) in offsets.iter().take(list.len()).enumerate() {
                if list.is_null(i) {
                    for _ in 0..dim {
                        builder.values().append_null();
                    }
                    builder.append(false);
                    continue;
                }
                let len = list.value_length(i) as usize;
                if len != dim {
                    return Err(format!("vector dim mismatch: expected {dim} got {len}"));
                }
                let start = *start as usize;
                for j in 0..dim {
                    let idx = start + j;
                    if idx >= values.len() {
                        return Err("vector offsets are out of bounds".to_string());
                    }
                    if values.is_null(idx) {
                        builder.values().append_null();
                    } else {
                        builder.values().append_value(values.value(idx));
                    }
                }
                builder.append(true);
            }
            Ok(builder.finish())
        }
    }
}

fn build_output_schema(
    input_schema: &SchemaRef,
    conversions: &[VectorConversion],
) -> Result<SchemaRef, String> {
    if conversions.is_empty() {
        return Ok(input_schema.clone());
    }
    let mut fields = input_schema.fields().as_ref().to_vec();
    for conv in conversions {
        let idx = conv.col_idx;
        if idx >= fields.len() {
            return Err("vector column index is out of bounds".to_string());
        }
        let original = fields[idx].as_ref();
        let (list_kind, element_type) = is_variable_list_vector_type(original.data_type())
            .ok_or_else(|| "vector column has unexpected data type".to_string())?;
        if list_kind != conv.list_kind || element_type != conv.element_type {
            return Err("vector column has unexpected data type".to_string());
        }
        let child_field = match original.data_type() {
            DataType::List(field) | DataType::LargeList(field) => field.clone(),
            _ => return Err("vector column has unexpected data type".to_string()),
        };
        let dim_i32 = i32::try_from(conv.dim).map_err(|_| "vector dim is too large".to_string())?;
        fields[idx] = Arc::new(Field::new(
            original.name(),
            DataType::FixedSizeList(child_field, dim_i32),
            original.is_nullable(),
        ));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn convert_record_batch(
    input_batch: &RecordBatch,
    output_schema: &SchemaRef,
    conversions: &[VectorConversion],
) -> Result<RecordBatch, String> {
    #[cfg(not(feature = "vane-distributed"))]
    if conversions.is_empty() {
        return RecordBatch::try_new(output_schema.clone(), input_batch.columns().to_vec())
            .map_err(|e| e.to_string());
    }
    let mut cols = input_batch.columns().to_vec();
    for conv in conversions {
        let arr = cols
            .get(conv.col_idx)
            .ok_or_else(|| "vector column index is out of bounds".to_string())?
            .as_ref();
        validate_list_vector_dim(arr, conv.list_kind, conv.dim)?;
        let fixed =
            convert_list_array_to_fixed_size(arr, conv.list_kind, conv.element_type, conv.dim)?;
        cols[conv.col_idx] = Arc::new(fixed);
    }
    #[cfg(feature = "vane-distributed")]
    {
        if cols.len() != output_schema.fields().len() {
            return Err("writer output schema has a different field count".to_string());
        }
        for (column, field) in cols.iter_mut().zip(output_schema.fields()) {
            if column.data_type() != field.data_type() {
                // The frozen-target validator admits only representation-level
                // Arrow normalization and the explicit vector conversion. It
                // rejects semantic casts (for example Int32 to Utf8) before a
                // worker accepts any batch.
                *column = arrow::compute::cast(column.as_ref(), field.data_type())
                    .map_err(|err| err.to_string())?;
            }
        }
    }
    RecordBatch::try_new(output_schema.clone(), cols).map_err(|e| e.to_string())
}

#[cfg(feature = "vane-distributed")]
fn select_writer_output_schema(state: &WriterState, inferred_schema: SchemaRef) -> SchemaRef {
    if let Some(target_schema) = &state.frozen_target_schema {
        return target_schema.clone();
    }
    inferred_schema
}

#[cfg(feature = "vane-distributed")]
fn frozen_target_container_field_name_compatible(input: &str, target: &str) -> bool {
    fn is_synthetic(name: &str) -> bool {
        name.is_empty() || matches!(name, "item" | "l")
    }

    input == target || (is_synthetic(input) && is_synthetic(target))
}

#[cfg(feature = "vane-distributed")]
fn frozen_target_container_field_compatible(input: &Field, target: &Field) -> bool {
    frozen_target_container_field_name_compatible(input.name(), target.name())
        && input.is_nullable() == target.is_nullable()
        && frozen_target_type_compatible(input.data_type(), target.data_type())
}

#[cfg(feature = "vane-distributed")]
fn frozen_target_vector_dimension(input_type: &DataType, target_type: &DataType) -> Option<usize> {
    let input_child = match input_type {
        DataType::List(child) | DataType::LargeList(child)
            if matches!(child.data_type(), DataType::Float32 | DataType::Float64) =>
        {
            child
        }
        _ => return None,
    };
    let DataType::FixedSizeList(target_child, dimension) = target_type else {
        return None;
    };
    if *dimension <= 0 || !frozen_target_container_field_compatible(input_child, target_child) {
        return None;
    }
    usize::try_from(*dimension).ok()
}

#[cfg(feature = "vane-distributed")]
fn frozen_target_field_compatible(input: &Field, target: &Field) -> bool {
    input.name() == target.name()
        && input.is_nullable() == target.is_nullable()
        && frozen_target_type_compatible(input.data_type(), target.data_type())
}

#[cfg(feature = "vane-distributed")]
fn frozen_target_type_compatible(input: &DataType, target: &DataType) -> bool {
    if input == target || frozen_target_vector_dimension(input, target).is_some() {
        return true;
    }
    match (input, target) {
        // DuckDB can export the same VARCHAR or BLOB logical type with 32-bit,
        // 64-bit, or view offsets according to client properties. Lance stores
        // the canonical non-view representation, so these are encoding-only
        // differences rather than schema evolution.
        (
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        )
        | (
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
        ) => true,
        (
            DataType::List(input_field) | DataType::LargeList(input_field),
            DataType::List(target_field) | DataType::LargeList(target_field),
        ) => frozen_target_container_field_compatible(input_field, target_field),
        (
            DataType::FixedSizeList(input_field, input_dimension),
            DataType::FixedSizeList(target_field, target_dimension),
        ) => {
            input_dimension == target_dimension
                && frozen_target_container_field_compatible(input_field, target_field)
        }
        (DataType::Struct(input_fields), DataType::Struct(target_fields)) => {
            input_fields.len() == target_fields.len()
                && input_fields
                    .iter()
                    .zip(target_fields)
                    .all(|(input, target)| frozen_target_field_compatible(input, target))
        }
        (DataType::Map(input_field, input_sorted), DataType::Map(target_field, target_sorted)) => {
            input_sorted == target_sorted
                && frozen_target_field_compatible(input_field, target_field)
        }
        _ => false,
    }
}

#[cfg(feature = "vane-distributed")]
fn configure_frozen_target_schema(
    input_schema: &SchemaRef,
    target_schema: &Schema,
    vector_candidates: &mut Vec<VectorConversion>,
) -> FfiResult<()> {
    if target_schema.fields().len() != input_schema.fields().len() {
        return Err(FfiError::new(
            ErrorCode::DistributedWrite,
            "distributed Lance worker input field count does not match the frozen target",
        ));
    }

    for (target, input) in target_schema.fields().iter().zip(input_schema.fields()) {
        if target.name() != input.name() {
            return Err(FfiError::new(
                ErrorCode::DistributedWrite,
                "distributed Lance worker input field names do not match the frozen target",
            ));
        }
        if target.is_nullable() != input.is_nullable() {
            return Err(FfiError::new(
                ErrorCode::DistributedWrite,
                format!(
                    "distributed Lance worker input field '{}' is {}, but the frozen target field is {}",
                    input.name(),
                    if input.is_nullable() {
                        "nullable"
                    } else {
                        "non-nullable"
                    },
                    if target.is_nullable() {
                        "nullable"
                    } else {
                        "non-nullable"
                    }
                ),
            ));
        }
        if !frozen_target_type_compatible(input.data_type(), target.data_type()) {
            return Err(FfiError::new(
                ErrorCode::DistributedWrite,
                format!(
                    "distributed Lance worker input field '{}' has type {:?}, but the frozen target has type {:?}",
                    input.name(),
                    input.data_type(),
                    target.data_type()
                ),
            ));
        }
    }

    // Variable float lists are normally candidates for fixed-size vector
    // inference. An existing frozen target is authoritative: retain and seed
    // only the candidates whose target is a fixed-size vector. Exact List or
    // LargeList targets must remain variable instead of being converted and
    // cast back to their original type.
    let mut configured_candidates = Vec::new();
    for mut candidate in std::mem::take(vector_candidates) {
        let input = input_schema.field(candidate.col_idx);
        let target = target_schema.field(candidate.col_idx);
        if let Some(dimension) =
            frozen_target_vector_dimension(input.data_type(), target.data_type())
        {
            candidate.dim = dimension;
            configured_candidates.push(candidate);
        }
    }
    *vector_candidates = configured_candidates;
    Ok(())
}

#[cfg(feature = "vane-distributed")]
fn frozen_target_value_error(path: &str, message: &str) -> FfiError {
    FfiError::new(
        ErrorCode::DistributedWrite,
        format!("distributed Lance worker input field '{path}' {message}"),
    )
}

#[cfg(feature = "vane-distributed")]
fn validate_frozen_target_field_values(
    field: &Field,
    array: &dyn Array,
    path: &str,
) -> FfiResult<()> {
    if !field.is_nullable() && array.null_count() > 0 {
        return Err(frozen_target_value_error(
            path,
            "contains a null, but the frozen target field is non-nullable",
        ));
    }

    // Mirror lance-file's FileWriter::verify_field_nullability: it recursively
    // checks every physical child array without applying ancestor validity
    // masks. This makes Vane reject frozen-target batches at ingestion with
    // native Lance semantics.
    match field.data_type() {
        DataType::Struct(fields) => {
            let values = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    frozen_target_value_error(path, "has an invalid nested Arrow type")
                })?;
            if fields.len() != values.num_columns() {
                return Err(frozen_target_value_error(
                    path,
                    "has an invalid nested Arrow field count",
                ));
            }
            for (child_field, child_array) in fields.iter().zip(values.columns()) {
                let child_path = format!("{path}.{}", child_field.name());
                validate_frozen_target_field_values(
                    child_field,
                    child_array.as_ref(),
                    &child_path,
                )?;
            }
        }
        DataType::List(child_field) => {
            let values = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                frozen_target_value_error(path, "has an invalid nested Arrow type")
            })?;
            let child_path = format!("{path}.{}", child_field.name());
            validate_frozen_target_field_values(
                child_field,
                values.values().as_ref(),
                &child_path,
            )?;
        }
        DataType::LargeList(child_field) => {
            let values = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| {
                    frozen_target_value_error(path, "has an invalid nested Arrow type")
                })?;
            let child_path = format!("{path}.{}", child_field.name());
            validate_frozen_target_field_values(
                child_field,
                values.values().as_ref(),
                &child_path,
            )?;
        }
        DataType::FixedSizeList(child_field, _) => {
            let values = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    frozen_target_value_error(path, "has an invalid nested Arrow type")
                })?;
            let child_path = format!("{path}.{}", child_field.name());
            validate_frozen_target_field_values(
                child_field,
                values.values().as_ref(),
                &child_path,
            )?;
        }
        DataType::Map(entries_field, _) => {
            let values = array.as_any().downcast_ref::<MapArray>().ok_or_else(|| {
                frozen_target_value_error(path, "has an invalid nested Arrow type")
            })?;
            let child_path = format!("{path}.{}", entries_field.name());
            validate_frozen_target_field_values(entries_field, values.entries(), &child_path)?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(feature = "vane-distributed")]
fn validate_frozen_target_values(batch: &RecordBatch) -> FfiResult<()> {
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        validate_frozen_target_field_values(field, column.as_ref(), field.name())?;
    }
    Ok(())
}

fn spawn_writer_thread(
    kind: WriterKind,
    path: String,
    #[cfg(feature = "vane-distributed")] dataset: Option<Arc<Dataset>>,
    params: WriteParams,
    schema: SchemaRef,
    receiver: Receiver<RecordBatch>,
) -> JoinHandle<Result<WriterResult, String>> {
    std::thread::spawn(move || -> Result<WriterResult, String> {
        let reader = ReceiverRecordBatchReader::new(schema, receiver);
        match kind {
            WriterKind::Committed => {
                let fut = Dataset::write(reader, &path, Some(params));
                match runtime::block_on(fut) {
                    Ok(Ok(_)) => Ok(WriterResult::Committed),
                    Ok(Err(err)) => Err(err.to_string()),
                    Err(err) => Err(format!("runtime: {err}")),
                }
            }
            WriterKind::Uncommitted => {
                let source: Box<dyn RecordBatchReader + Send> = Box::new(reader);
                #[cfg(not(feature = "vane-distributed"))]
                let builder = InsertBuilder::new(path.as_str()).with_params(&params);
                #[cfg(not(feature = "vane-distributed"))]
                let fut = builder.execute_uncommitted_stream(source);
                #[cfg(feature = "vane-distributed")]
                let fut = async move {
                    match dataset {
                        Some(dataset) => {
                            InsertBuilder::new(dataset)
                                .with_params(&params)
                                .execute_uncommitted_stream(source)
                                .await
                        }
                        None => {
                            InsertBuilder::new(path.as_str())
                                .with_params(&params)
                                .execute_uncommitted_stream(source)
                                .await
                        }
                    }
                };
                match runtime::block_on(fut) {
                    Ok(Ok(txn)) => Ok(WriterResult::Uncommitted(Box::new(txn))),
                    Ok(Err(err)) => Err(err.to_string()),
                    Err(err) => Err(format!("runtime: {err}")),
                }
            }
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_open_writer_with_storage_options(
    path: *const c_char,
    mode: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    max_rows_per_file: u64,
    max_rows_per_group: u64,
    max_bytes_per_file: u64,
    data_storage_version: *const c_char,
    session: *mut c_void,
    schema: *const c_void,
) -> *mut c_void {
    match open_writer_inner(
        path,
        mode,
        option_keys,
        option_values,
        options_len,
        max_rows_per_file,
        max_rows_per_group,
        max_bytes_per_file,
        data_storage_version,
        session,
        schema,
    ) {
        Ok(handle) => {
            clear_last_error();
            Box::into_raw(Box::new(handle)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_open_uncommitted_writer_with_storage_options(
    path: *const c_char,
    mode: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    max_rows_per_file: u64,
    max_rows_per_group: u64,
    max_bytes_per_file: u64,
    data_storage_version: *const c_char,
    session: *mut c_void,
    schema: *const c_void,
) -> *mut c_void {
    match open_uncommitted_writer_inner(
        path,
        mode,
        option_keys,
        option_values,
        options_len,
        max_rows_per_file,
        max_rows_per_group,
        max_bytes_per_file,
        data_storage_version,
        session,
        schema,
    ) {
        Ok(handle) => {
            clear_last_error();
            Box::into_raw(Box::new(handle)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

#[cfg(feature = "vane-distributed")]
#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub unsafe extern "C" fn lance_open_distributed_uncommitted_writer_with_storage_options(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    expected_version: u64,
    expected_generation: *const c_char,
    expected_creation_uuid: *const c_char,
    operation_id: *const c_char,
    query_id: *const c_char,
    task_attempt_id: *const c_char,
    max_rows_per_file: u64,
    max_rows_per_group: u64,
    max_bytes_per_file: u64,
    session: *mut c_void,
    schema: *const c_void,
) -> *mut c_void {
    match open_distributed_uncommitted_writer_inner(
        path,
        option_keys,
        option_values,
        options_len,
        expected_version,
        expected_generation,
        expected_creation_uuid,
        operation_id,
        query_id,
        task_attempt_id,
        max_rows_per_file,
        max_rows_per_group,
        max_bytes_per_file,
        session,
        schema,
    ) {
        Ok(handle) => {
            clear_last_error();
            Box::into_raw(Box::new(handle)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

#[cfg(feature = "vane-distributed")]
pub(super) fn distributed_optional_cstr(
    value: *const c_char,
    name: &'static str,
) -> FfiResult<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    // SAFETY: the enclosing FFI call guarantees that a non-null value points
    // to a valid NUL-terminated C string for the duration of this call.
    let value = unsafe { cstr_to_str(value, name)? };
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(value.to_string()))
}

#[cfg(feature = "vane-distributed")]
pub(super) unsafe fn distributed_storage_options(
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
) -> FfiResult<HashMap<String, String>> {
    if options_len > 0 && (option_keys.is_null() || option_values.is_null()) {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "option_keys/option_values is null with non-zero length",
        ));
    }
    let keys = if options_len == 0 {
        &[][..]
    } else {
        // SAFETY: non-zero arrays were checked non-null above, and the FFI
        // caller guarantees option_keys contains options_len readable entries.
        unsafe { slice_from_ptr(option_keys, options_len, "option_keys")? }
    };
    let values = if options_len == 0 {
        &[][..]
    } else {
        // SAFETY: non-zero arrays were checked non-null above, and the FFI
        // caller guarantees option_values contains options_len readable entries.
        unsafe { slice_from_ptr(option_values, options_len, "option_values")? }
    };
    let mut result = HashMap::new();
    for (index, (&key, &value)) in keys.iter().zip(values.iter()).enumerate() {
        if key.is_null() || value.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                format!("option key/value is null at index {index}"),
            ));
        }
        // SAFETY: key was checked non-null above, and the FFI caller guarantees
        // each option key is a valid NUL-terminated C string.
        let key = unsafe { CStr::from_ptr(key) }.to_str().map_err(|err| {
            FfiError::new(ErrorCode::Utf8, format!("option_keys[{index}] utf8: {err}"))
        })?;
        // SAFETY: value was checked non-null above, and the FFI caller guarantees
        // each option value is a valid NUL-terminated C string.
        let value = unsafe { CStr::from_ptr(value) }.to_str().map_err(|err| {
            FfiError::new(
                ErrorCode::Utf8,
                format!("option_values[{index}] utf8: {err}"),
            )
        })?;
        result.insert(key.to_string(), value.to_string());
    }
    Ok(result)
}

#[cfg(feature = "vane-distributed")]
#[allow(clippy::too_many_arguments)]
fn open_distributed_uncommitted_writer_inner(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    expected_version: u64,
    expected_generation: *const c_char,
    expected_creation_uuid: *const c_char,
    operation_id: *const c_char,
    query_id: *const c_char,
    task_attempt_id: *const c_char,
    max_rows_per_file: u64,
    max_rows_per_group: u64,
    max_bytes_per_file: u64,
    session: *mut c_void,
    schema: *const c_void,
) -> FfiResult<WriterHandle> {
    // SAFETY: the enclosing FFI call guarantees that path points to a valid
    // NUL-terminated C string for the duration of this call.
    let path_value = unsafe { cstr_to_str(path, "path")? }.to_string();
    // SAFETY: the enclosing FFI call guarantees that operation_id points to a
    // valid NUL-terminated C string for the duration of this call.
    let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
    // SAFETY: the enclosing FFI call guarantees that query_id points to a
    // valid NUL-terminated C string for the duration of this call.
    let query_id = unsafe { cstr_to_str(query_id, "query_id")? }.to_string();
    // SAFETY: the enclosing FFI call guarantees that task_attempt_id points to
    // a valid NUL-terminated C string for the duration of this call.
    let task_attempt_id = unsafe { cstr_to_str(task_attempt_id, "task_attempt_id")? }.to_string();
    if expected_version == 0
        || operation_id.is_empty()
        || query_id.is_empty()
        || task_attempt_id.is_empty()
    {
        return Err(FfiError::new(
            ErrorCode::DistributedWrite,
            "distributed Lance writer has incomplete frozen identity",
        ));
    }
    let expected_generation =
        distributed_optional_cstr(expected_generation, "expected_generation")?;
    let expected_creation_uuid =
        distributed_optional_cstr(expected_creation_uuid, "expected_creation_uuid")?;
    if expected_generation.is_some() == expected_creation_uuid.is_some() {
        return Err(FfiError::new(
            ErrorCode::DistributedWrite,
            "distributed Lance writer requires exactly one generation identity",
        ));
    }

    // SAFETY: the enclosing FFI call guarantees that the option arrays contain
    // options_len readable C-string pointers; the helper validates null state.
    let storage_options =
        unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
    // SAFETY: session is either null or a live session handle returned by this
    // module, as required by the enclosing FFI call.
    let session_handle = unsafe { optional_session_handle(session)? };
    let dataset = match runtime::block_on(async {
        let mut builder = DatasetBuilder::from_uri(path_value.as_str());
        builder = with_explicit_aws_credentials(builder, &storage_options);
        builder = builder.with_storage_options(storage_options);
        if let Some(session) = session_handle.clone() {
            builder = builder.with_session(session);
        }
        builder.load().await
    }) {
        Ok(Ok(dataset)) => dataset,
        Ok(Err(err)) => {
            return Err(FfiError::new(
                ErrorCode::DistributedWrite,
                format!("open frozen distributed Lance target: {err}"),
            ))
        }
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };
    if dataset.version_id() != expected_version {
        return Err(FfiError::new(
            ErrorCode::DistributedWrite,
            format!(
                "distributed Lance target version changed: expected {expected_version}, got {}",
                dataset.version_id()
            ),
        ));
    }
    if let Some(expected_generation) = expected_generation {
        let identity = match runtime::block_on(super::dataset::dataset_snapshot_identity(&dataset))
        {
            Ok(Ok(identity)) => format!("snapshot|{identity}"),
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        if identity != expected_generation {
            return Err(FfiError::new(
                ErrorCode::DistributedWrite,
                "distributed Lance target generation changed",
            ));
        }
    }
    if let Some(expected_creation_uuid) = expected_creation_uuid {
        let expected_suffix = format!("-{expected_creation_uuid}.txn");
        if !dataset
            .manifest()
            .transaction_file
            .as_deref()
            .is_some_and(|path| path.ends_with(expected_suffix.as_str()))
        {
            return Err(FfiError::new(
                ErrorCode::DistributedWrite,
                "prepared distributed Lance CTAS generation does not match its operation",
            ));
        }
    }

    let append_mode = std::ffi::CString::new("append").map_err(|err| {
        FfiError::new(
            ErrorCode::DistributedWrite,
            format!("construct append mode: {err}"),
        )
    })?;
    let handle = open_uncommitted_writer_inner(
        path,
        append_mode.as_ptr(),
        option_keys,
        option_values,
        options_len,
        max_rows_per_file,
        max_rows_per_group,
        max_bytes_per_file,
        ptr::null(),
        session,
        schema,
    )?;
    let target_schema: Schema = dataset.schema().into();
    let mut transaction_properties = HashMap::new();
    transaction_properties.insert("vane.operation_id".to_string(), operation_id);
    transaction_properties.insert("vane.query_id".to_string(), query_id);
    transaction_properties.insert("vane.task_attempt_id".to_string(), task_attempt_id);
    {
        let mut state = handle
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        configure_frozen_target_schema(
            &handle.input_schema,
            &target_schema,
            &mut state.vector_candidates,
        )?;
        state.dataset = Some(Arc::new(dataset));
        state.frozen_target_schema = Some(Arc::new(target_schema));
        state.params.transaction_properties = Some(Arc::new(transaction_properties));
        state.params.skip_auto_cleanup = true;
    }
    Ok(handle)
}

fn parse_data_storage_version_arg(
    data_storage_version: *const c_char,
) -> FfiResult<Option<String>> {
    if data_storage_version.is_null() {
        return Ok(None);
    }

    let raw = unsafe { cstr_to_str(data_storage_version, "data_storage_version")? };
    let token = raw.trim();
    if token.is_empty() {
        return Err(FfiError::new(
            ErrorCode::DatasetWriteOpen,
            "data_storage_version cannot be empty",
        ));
    }

    let lower = token.to_ascii_lowercase();
    let normalized = match lower.as_str() {
        "v2_0" | "v2.0" | "2_0" => "2.0".to_string(),
        "v2_1" | "v2.1" | "2_1" => "2.1".to_string(),
        "v2_2" | "v2.2" | "2_2" => "2.2".to_string(),
        _ => lower,
    };
    Ok(Some(normalized))
}

#[allow(clippy::too_many_arguments)]
fn open_uncommitted_writer_inner(
    path: *const c_char,
    mode: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    max_rows_per_file: u64,
    max_rows_per_group: u64,
    max_bytes_per_file: u64,
    data_storage_version: *const c_char,
    session: *mut c_void,
    schema: *const c_void,
) -> FfiResult<WriterHandle> {
    let path = unsafe { cstr_to_str(path, "path")? }.to_string();
    let mode = unsafe { cstr_to_str(mode, "mode")? };

    if schema.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "schema is null"));
    }

    if options_len > 0 && (option_keys.is_null() || option_values.is_null()) {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "option_keys/option_values is null with non-zero length",
        ));
    }

    let keys = if options_len == 0 {
        &[][..]
    } else {
        unsafe { slice_from_ptr(option_keys, options_len, "option_keys")? }
    };
    let values = if options_len == 0 {
        &[][..]
    } else {
        unsafe { slice_from_ptr(option_values, options_len, "option_values")? }
    };

    let mut storage_options = HashMap::<String, String>::new();
    for (idx, (&key_ptr, &val_ptr)) in keys.iter().zip(values.iter()).enumerate() {
        if key_ptr.is_null() || val_ptr.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                format!("option key/value is null at index {idx}"),
            ));
        }
        let key = unsafe { CStr::from_ptr(key_ptr) }.to_str().map_err(|err| {
            FfiError::new(ErrorCode::Utf8, format!("option_keys[{idx}] utf8: {err}"))
        })?;
        let value = unsafe { CStr::from_ptr(val_ptr) }.to_str().map_err(|err| {
            FfiError::new(ErrorCode::Utf8, format!("option_values[{idx}] utf8: {err}"))
        })?;
        storage_options.insert(key.to_string(), value.to_string());
    }

    let ffi_schema = unsafe { &*(schema as *const arrow_schema::ffi::FFI_ArrowSchema) };
    let data_type = DataType::try_from(ffi_schema).map_err(|err| {
        FfiError::new(ErrorCode::DatasetWriteOpen, format!("schema import: {err}"))
    })?;
    let DataType::Struct(fields) = &data_type else {
        return Err(FfiError::new(
            ErrorCode::DatasetWriteOpen,
            "schema must be a struct",
        ));
    };
    let schema: SchemaRef = std::sync::Arc::new(Schema::new(fields.clone()));

    let write_mode = WriteMode::try_from(mode).map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetWriteOpen,
            format!("invalid write mode '{mode}': {err}"),
        )
    })?;

    let max_rows_per_file = usize::try_from(max_rows_per_file).map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetWriteOpen,
            format!("invalid max_rows_per_file: {err}"),
        )
    })?;
    let max_rows_per_group = usize::try_from(max_rows_per_group).map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetWriteOpen,
            format!("invalid max_rows_per_group: {err}"),
        )
    })?;
    let max_bytes_per_file = usize::try_from(max_bytes_per_file).map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetWriteOpen,
            format!("invalid max_bytes_per_file: {err}"),
        )
    })?;
    let data_storage_version = parse_data_storage_version_arg(data_storage_version)?
        .map(|value| {
            value.parse().map_err(|err| {
                FfiError::new(
                    ErrorCode::DatasetWriteOpen,
                    format!("invalid data_storage_version '{value}': {err}"),
                )
            })
        })
        .transpose()?;
    let session = unsafe { optional_session_handle(session)? };

    let mut store_params = ObjectStoreParams::default();
    if !storage_options.is_empty() {
        store_params.storage_options_accessor = Some(Arc::new(
            StorageOptionsAccessor::with_static_options(storage_options),
        ));
    }

    let params = WriteParams {
        mode: write_mode,
        max_rows_per_file,
        max_rows_per_group,
        max_bytes_per_file,
        data_storage_version,
        session,
        store_params: Some(store_params),
        ..Default::default()
    };
    let mut vector_candidates = Vec::<VectorConversion>::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some((list_kind, element_type)) = is_variable_list_vector_type(field.data_type()) {
            vector_candidates.push(VectorConversion {
                col_idx: idx,
                dim: 0,
                list_kind,
                element_type,
            });
        }
    }

    Ok(WriterHandle {
        input_schema: schema.clone(),
        data_type,
        state: Mutex::new(WriterState {
            kind: WriterKind::Uncommitted,
            path,
            #[cfg(feature = "vane-distributed")]
            dataset: None,
            #[cfg(feature = "vane-distributed")]
            frozen_target_schema: None,
            params,
            vector_candidates,
            buffered_batches: Vec::new(),
            output_schema: None,
            output_sender: None,
            output_join: None,
        }),
        batches_sent: AtomicU64::new(0),
    })
}

#[allow(clippy::too_many_arguments)]
fn open_writer_inner(
    path: *const c_char,
    mode: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    max_rows_per_file: u64,
    max_rows_per_group: u64,
    max_bytes_per_file: u64,
    data_storage_version: *const c_char,
    session: *mut c_void,
    schema: *const c_void,
) -> FfiResult<WriterHandle> {
    let path = unsafe { cstr_to_str(path, "path")? }.to_string();
    let mode = unsafe { cstr_to_str(mode, "mode")? };

    if schema.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "schema is null"));
    }

    if options_len > 0 && (option_keys.is_null() || option_values.is_null()) {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "option_keys/option_values is null with non-zero length",
        ));
    }

    let keys = if options_len == 0 {
        &[][..]
    } else {
        unsafe { slice_from_ptr(option_keys, options_len, "option_keys")? }
    };
    let values = if options_len == 0 {
        &[][..]
    } else {
        unsafe { slice_from_ptr(option_values, options_len, "option_values")? }
    };

    let mut storage_options = HashMap::<String, String>::new();
    for (idx, (&key_ptr, &val_ptr)) in keys.iter().zip(values.iter()).enumerate() {
        if key_ptr.is_null() || val_ptr.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                format!("option key/value is null at index {idx}"),
            ));
        }
        let key = unsafe { CStr::from_ptr(key_ptr) }.to_str().map_err(|err| {
            FfiError::new(ErrorCode::Utf8, format!("option_keys[{idx}] utf8: {err}"))
        })?;
        let value = unsafe { CStr::from_ptr(val_ptr) }.to_str().map_err(|err| {
            FfiError::new(ErrorCode::Utf8, format!("option_values[{idx}] utf8: {err}"))
        })?;
        storage_options.insert(key.to_string(), value.to_string());
    }

    let ffi_schema = unsafe { &*(schema as *const arrow_schema::ffi::FFI_ArrowSchema) };
    let data_type = DataType::try_from(ffi_schema).map_err(|err| {
        FfiError::new(ErrorCode::DatasetWriteOpen, format!("schema import: {err}"))
    })?;
    let DataType::Struct(fields) = &data_type else {
        return Err(FfiError::new(
            ErrorCode::DatasetWriteOpen,
            "schema must be a struct",
        ));
    };
    let schema: SchemaRef = std::sync::Arc::new(Schema::new(fields.clone()));

    let write_mode = WriteMode::try_from(mode).map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetWriteOpen,
            format!("invalid write mode '{mode}': {err}"),
        )
    })?;

    let max_rows_per_file = usize::try_from(max_rows_per_file).map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetWriteOpen,
            format!("invalid max_rows_per_file: {err}"),
        )
    })?;
    let max_rows_per_group = usize::try_from(max_rows_per_group).map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetWriteOpen,
            format!("invalid max_rows_per_group: {err}"),
        )
    })?;
    let max_bytes_per_file = usize::try_from(max_bytes_per_file).map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetWriteOpen,
            format!("invalid max_bytes_per_file: {err}"),
        )
    })?;
    let data_storage_version = parse_data_storage_version_arg(data_storage_version)?
        .map(|value| {
            value.parse().map_err(|err| {
                FfiError::new(
                    ErrorCode::DatasetWriteOpen,
                    format!("invalid data_storage_version '{value}': {err}"),
                )
            })
        })
        .transpose()?;
    let session = unsafe { optional_session_handle(session)? };

    let mut store_params = ObjectStoreParams::default();
    if !storage_options.is_empty() {
        store_params.storage_options_accessor = Some(Arc::new(
            StorageOptionsAccessor::with_static_options(storage_options),
        ));
    }

    let params = WriteParams {
        mode: write_mode,
        max_rows_per_file,
        max_rows_per_group,
        max_bytes_per_file,
        data_storage_version,
        session,
        store_params: Some(store_params),
        ..Default::default()
    };

    let mut vector_candidates = Vec::<VectorConversion>::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some((list_kind, element_type)) = is_variable_list_vector_type(field.data_type()) {
            vector_candidates.push(VectorConversion {
                col_idx: idx,
                dim: 0,
                list_kind,
                element_type,
            });
        }
    }

    Ok(WriterHandle {
        input_schema: schema.clone(),
        data_type,
        state: Mutex::new(WriterState {
            kind: WriterKind::Committed,
            path,
            #[cfg(feature = "vane-distributed")]
            dataset: None,
            #[cfg(feature = "vane-distributed")]
            frozen_target_schema: None,
            params,
            vector_candidates,
            buffered_batches: Vec::new(),
            output_schema: None,
            output_sender: None,
            output_join: None,
        }),
        batches_sent: AtomicU64::new(0),
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_writer_write_batch(writer: *mut c_void, array: *mut c_void) -> i32 {
    match writer_write_batch_inner(writer, array) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

fn writer_write_batch_inner(writer: *mut c_void, array: *mut c_void) -> FfiResult<()> {
    if writer.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "writer is null"));
    }
    if array.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "array is null"));
    }

    let handle = unsafe { &*(writer as *const WriterHandle) };

    let raw_array = unsafe { ptr::read(array as *mut RawArrowArray) };
    unsafe {
        (*(array as *mut RawArrowArray)).release = None;
    }

    let ffi_array: arrow::ffi::FFI_ArrowArray = unsafe { std::mem::transmute(raw_array) };

    let array_data =
        unsafe { arrow_array::ffi::from_ffi_and_data_type(ffi_array, handle.data_type.clone()) }
            .map_err(|err| {
                FfiError::new(ErrorCode::DatasetWriteBatch, format!("array import: {err}"))
            })?;
    let array = make_array(array_data);
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| FfiError::new(ErrorCode::DatasetWriteBatch, "array is not a struct"))?;

    let input_batch =
        RecordBatch::try_new(handle.input_schema.clone(), struct_array.columns().to_vec())
            .map_err(|err| {
                FfiError::new(ErrorCode::DatasetWriteBatch, format!("record batch: {err}"))
            })?;

    let (sender, to_send) = {
        let mut guard = handle
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        #[cfg(feature = "vane-distributed")]
        if guard.frozen_target_schema.is_some() {
            validate_frozen_target_values(&input_batch)?;
        }

        if guard.output_sender.is_none() {
            guard.buffered_batches.push(input_batch);

            if !guard.vector_candidates.is_empty() {
                let batches = guard.buffered_batches.clone();
                for cand in guard.vector_candidates.iter_mut() {
                    if cand.dim != 0 {
                        continue;
                    }
                    for batch in batches.iter() {
                        let arr = batch.column(cand.col_idx).as_ref();
                        match infer_vector_dim_from_array(arr, cand.list_kind) {
                            Some(Ok(dim)) => {
                                cand.dim = dim;
                                break;
                            }
                            Some(Err(e)) => {
                                return Err(FfiError::new(ErrorCode::DatasetWriteBatch, e));
                            }
                            None => {}
                        }
                    }
                }

                let batches = guard.buffered_batches.clone();
                for cand in guard.vector_candidates.iter() {
                    if cand.dim == 0 {
                        continue;
                    }
                    for batch in batches.iter() {
                        let arr = batch.column(cand.col_idx).as_ref();
                        if let Err(e) = validate_list_vector_dim(arr, cand.list_kind, cand.dim) {
                            return Err(FfiError::new(ErrorCode::DatasetWriteBatch, e));
                        }
                    }
                }
            }

            let can_start = guard.vector_candidates.iter().all(|c| c.dim != 0)
                || guard.buffered_batches.len() >= MAX_VECTOR_DIM_INFERENCE_BATCHES;
            if can_start {
                let conversions: Vec<VectorConversion> = guard
                    .vector_candidates
                    .iter()
                    .filter(|c| c.dim != 0)
                    .cloned()
                    .collect();

                let output_schema = build_output_schema(&handle.input_schema, &conversions)
                    .map_err(|e| FfiError::new(ErrorCode::DatasetWriteBatch, e))?;
                #[cfg(feature = "vane-distributed")]
                let output_schema = select_writer_output_schema(&guard, output_schema);
                let (sender, receiver) = sync_channel::<RecordBatch>(2);
                let join = spawn_writer_thread(
                    guard.kind,
                    guard.path.clone(),
                    #[cfg(feature = "vane-distributed")]
                    guard.dataset.clone(),
                    guard.params.clone(),
                    output_schema.clone(),
                    receiver,
                );

                let buffered = std::mem::take(&mut guard.buffered_batches);
                let mut out_batches = Vec::with_capacity(buffered.len());
                for b in buffered.iter() {
                    let out = convert_record_batch(b, &output_schema, &conversions)
                        .map_err(|e| FfiError::new(ErrorCode::DatasetWriteBatch, e))?;
                    out_batches.push(out);
                }

                guard.output_schema = Some(output_schema);
                guard.output_sender = Some(sender.clone());
                guard.output_join = Some(join);
                (Some(sender), out_batches)
            } else {
                (None, Vec::new())
            }
        } else {
            let sender = guard.output_sender.as_ref().cloned();
            let schema = guard
                .output_schema
                .as_ref()
                .ok_or_else(|| {
                    FfiError::new(ErrorCode::DatasetWriteBatch, "writer is not initialized")
                })?
                .clone();
            let conversions: Vec<VectorConversion> = guard
                .vector_candidates
                .iter()
                .filter(|c| c.dim != 0)
                .cloned()
                .collect();
            let out = convert_record_batch(&input_batch, &schema, &conversions)
                .map_err(|e| FfiError::new(ErrorCode::DatasetWriteBatch, e))?;
            (sender, vec![out])
        }
    };

    if let Some(sender) = sender {
        for batch in to_send {
            sender.send(batch).map_err(|_| {
                FfiError::new(
                    ErrorCode::DatasetWriteBatch,
                    "writer background task exited",
                )
            })?;
        }
    }

    handle.batches_sent.fetch_add(1, Ordering::Relaxed);

    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn lance_writer_finish(writer: *mut c_void) -> i32 {
    match writer_finish_inner(writer) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

fn writer_finish_inner(writer: *mut c_void) -> FfiResult<()> {
    if writer.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "writer is null"));
    }

    let handle = unsafe { &*(writer as *const WriterHandle) };
    let (sender, join, to_send) = {
        let mut guard = handle
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.output_sender.is_none() {
            let conversions: Vec<VectorConversion> = guard
                .vector_candidates
                .iter()
                .filter(|c| c.dim != 0)
                .cloned()
                .collect();
            let output_schema = build_output_schema(&handle.input_schema, &conversions)
                .map_err(|e| FfiError::new(ErrorCode::DatasetWriteFinish, e))?;
            #[cfg(feature = "vane-distributed")]
            let output_schema = select_writer_output_schema(&guard, output_schema);
            let (sender, receiver) = sync_channel::<RecordBatch>(2);
            let join = spawn_writer_thread(
                guard.kind,
                guard.path.clone(),
                #[cfg(feature = "vane-distributed")]
                guard.dataset.clone(),
                guard.params.clone(),
                output_schema.clone(),
                receiver,
            );
            let buffered = std::mem::take(&mut guard.buffered_batches);
            let mut out_batches = Vec::with_capacity(buffered.len() + 1);
            for b in buffered.iter() {
                let out = convert_record_batch(b, &output_schema, &conversions)
                    .map_err(|e| FfiError::new(ErrorCode::DatasetWriteFinish, e))?;
                out_batches.push(out);
            }
            if handle.batches_sent.load(Ordering::Acquire) == 0 {
                out_batches.push(RecordBatch::new_empty(output_schema.clone()));
            }
            guard.output_schema = Some(output_schema);
            guard.output_sender = Some(sender.clone());
            guard.output_join = Some(join);
            guard.buffered_batches = out_batches;
        }

        let sender = guard.output_sender.as_ref().cloned().ok_or_else(|| {
            FfiError::new(ErrorCode::DatasetWriteFinish, "writer is not initialized")
        })?;
        let join = guard.output_join.take().ok_or_else(|| {
            FfiError::new(ErrorCode::DatasetWriteFinish, "writer is already finished")
        })?;
        let to_send = std::mem::take(&mut guard.buffered_batches);
        guard.output_sender = None;
        (sender, join, to_send)
    };

    for b in to_send {
        sender.send(b).map_err(|_| {
            FfiError::new(
                ErrorCode::DatasetWriteFinish,
                "writer background task exited",
            )
        })?;
    }
    drop(sender);

    match join.join() {
        Ok(Ok(WriterResult::Committed)) => Ok(()),
        Ok(Ok(WriterResult::Uncommitted(_))) => Err(FfiError::new(
            ErrorCode::DatasetWriteFinish,
            "writer returned an uncommitted transaction",
        )),
        Ok(Err(message)) => Err(FfiError::new(ErrorCode::DatasetWriteFinish, message)),
        Err(_) => Err(FfiError::new(
            ErrorCode::DatasetWriteFinish,
            "writer thread panicked",
        )),
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_writer_finish_uncommitted(
    writer: *mut c_void,
    out_transaction: *mut *mut c_void,
) -> i32 {
    match writer_finish_uncommitted_inner(writer, out_transaction) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

fn writer_finish_uncommitted_inner(
    writer: *mut c_void,
    out_transaction: *mut *mut c_void,
) -> FfiResult<()> {
    if writer.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "writer is null"));
    }
    if out_transaction.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "out_transaction is null",
        ));
    }

    let handle = unsafe { &*(writer as *const WriterHandle) };
    let (sender, join, to_send) = {
        let mut guard = handle
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.output_sender.is_none() {
            let conversions: Vec<VectorConversion> = guard
                .vector_candidates
                .iter()
                .filter(|c| c.dim != 0)
                .cloned()
                .collect();
            let output_schema = build_output_schema(&handle.input_schema, &conversions)
                .map_err(|e| FfiError::new(ErrorCode::DatasetWriteFinishUncommitted, e))?;
            #[cfg(feature = "vane-distributed")]
            let output_schema = select_writer_output_schema(&guard, output_schema);
            let (sender, receiver) = sync_channel::<RecordBatch>(2);
            let join = spawn_writer_thread(
                guard.kind,
                guard.path.clone(),
                #[cfg(feature = "vane-distributed")]
                guard.dataset.clone(),
                guard.params.clone(),
                output_schema.clone(),
                receiver,
            );
            let buffered = std::mem::take(&mut guard.buffered_batches);
            let mut out_batches = Vec::with_capacity(buffered.len() + 1);
            for b in buffered.iter() {
                let out = convert_record_batch(b, &output_schema, &conversions)
                    .map_err(|e| FfiError::new(ErrorCode::DatasetWriteFinishUncommitted, e))?;
                out_batches.push(out);
            }
            if handle.batches_sent.load(Ordering::Acquire) == 0 {
                out_batches.push(RecordBatch::new_empty(output_schema.clone()));
            }
            guard.output_schema = Some(output_schema);
            guard.output_sender = Some(sender.clone());
            guard.output_join = Some(join);
            guard.buffered_batches = out_batches;
        }

        let sender = guard.output_sender.as_ref().cloned().ok_or_else(|| {
            FfiError::new(
                ErrorCode::DatasetWriteFinishUncommitted,
                "writer is not initialized",
            )
        })?;
        let join = guard.output_join.take().ok_or_else(|| {
            FfiError::new(
                ErrorCode::DatasetWriteFinishUncommitted,
                "writer is already finished",
            )
        })?;
        let to_send = std::mem::take(&mut guard.buffered_batches);
        guard.output_sender = None;
        (sender, join, to_send)
    };

    for b in to_send {
        sender.send(b).map_err(|_| {
            FfiError::new(
                ErrorCode::DatasetWriteFinishUncommitted,
                "writer background task exited",
            )
        })?;
    }
    drop(sender);

    let txn = match join.join() {
        Ok(Ok(WriterResult::Uncommitted(txn))) => txn,
        Ok(Ok(WriterResult::Committed)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetWriteFinishUncommitted,
                "writer did not return an uncommitted transaction",
            ))
        }
        Ok(Err(message)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetWriteFinishUncommitted,
                message,
            ))
        }
        Err(_) => {
            return Err(FfiError::new(
                ErrorCode::DatasetWriteFinishUncommitted,
                "writer thread panicked",
            ))
        }
    };

    unsafe {
        *out_transaction = Box::into_raw(txn) as *mut c_void;
    }

    Ok(())
}

#[cfg(feature = "vane-distributed")]
pub(super) fn finish_distributed_writer_uncommitted(
    writer: *mut c_void,
    out_transaction: *mut *mut c_void,
) -> FfiResult<()> {
    writer_finish_uncommitted_inner(writer, out_transaction)
}

#[no_mangle]
pub unsafe extern "C" fn lance_close_writer(writer: *mut c_void) {
    if writer.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(writer as *mut WriterHandle);
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_commit_transaction_with_storage_options(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
    transaction: *mut c_void,
) -> i32 {
    match commit_transaction_inner(
        path,
        option_keys,
        option_values,
        options_len,
        session,
        transaction,
    ) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

fn commit_transaction_inner(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
    transaction: *mut c_void,
) -> FfiResult<()> {
    let path = unsafe { cstr_to_str(path, "path")? }.to_string();
    if transaction.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "transaction is null",
        ));
    }

    if options_len > 0 && (option_keys.is_null() || option_values.is_null()) {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "option_keys/option_values is null with non-zero length",
        ));
    }

    let keys = if options_len == 0 {
        &[][..]
    } else {
        unsafe { slice_from_ptr(option_keys, options_len, "option_keys")? }
    };
    let values = if options_len == 0 {
        &[][..]
    } else {
        unsafe { slice_from_ptr(option_values, options_len, "option_values")? }
    };

    let mut storage_options = HashMap::<String, String>::new();
    for (idx, (&key_ptr, &val_ptr)) in keys.iter().zip(values.iter()).enumerate() {
        if key_ptr.is_null() || val_ptr.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                format!("option key/value is null at index {idx}"),
            ));
        }
        let key = unsafe { CStr::from_ptr(key_ptr) }.to_str().map_err(|err| {
            FfiError::new(ErrorCode::Utf8, format!("option_keys[{idx}] utf8: {err}"))
        })?;
        let value = unsafe { CStr::from_ptr(val_ptr) }.to_str().map_err(|err| {
            FfiError::new(ErrorCode::Utf8, format!("option_values[{idx}] utf8: {err}"))
        })?;
        storage_options.insert(key.to_string(), value.to_string());
    }

    let mut store_params = ObjectStoreParams::default();
    if !storage_options.is_empty() {
        store_params.storage_options_accessor = Some(Arc::new(
            StorageOptionsAccessor::with_static_options(storage_options),
        ));
    }
    let session = unsafe { optional_session_handle(session)? };

    let txn =
        unsafe { Box::from_raw(transaction as *mut lance::dataset::transaction::Transaction) };
    let mut builder = CommitBuilder::new(path.as_str()).with_store_params(store_params);
    if let Some(session) = session {
        builder = builder.with_session(session);
    }
    let fut = builder.execute(*txn);
    match runtime::block_on(fut) {
        Ok(Ok(_)) => {
            record_commit();
            Ok(())
        }
        Ok(Err(err)) => Err(FfiError::new(
            ErrorCode::DatasetCommitTransaction,
            err.to_string(),
        )),
        Err(err) => Err(FfiError::new(
            ErrorCode::DatasetCommitTransaction,
            format!("runtime: {err}"),
        )),
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_free_transaction(transaction: *mut c_void) {
    if transaction.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(transaction as *mut lance::dataset::transaction::Transaction);
    }
}

#[cfg(all(test, feature = "vane-distributed"))]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder};
    use arrow_array::{ArrayRef, Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use lance::dataset::progress::WriteFragmentProgress;
    use lance_table::format::Fragment;

    use super::*;

    #[derive(Debug)]
    struct FailCompletedFragment;

    #[async_trait::async_trait]
    impl WriteFragmentProgress for FailCompletedFragment {
        async fn begin(&self, _fragment: &Fragment) -> lance::Result<()> {
            Ok(())
        }

        async fn complete(&self, _fragment: &Fragment) -> lance::Result<()> {
            Err(lance::Error::invalid_input(
                "injected completed-fragment failure",
            ))
        }
    }

    fn data_objects(dataset: &Dataset) -> Vec<String> {
        let store = runtime::block_on(dataset.object_store(None))
            .unwrap()
            .unwrap();
        let mut objects = runtime::block_on(async {
            store
                .list(Some(dataset.data_dir()))
                .map_ok(|object| object.location.to_string())
                .try_collect::<Vec<_>>()
                .await
        })
        .unwrap()
        .unwrap();
        objects.sort();
        objects
    }

    #[test]
    fn frozen_target_schema_rejects_changes_and_configures_vectors() {
        let int_input = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let string_target = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);
        let mut candidates = Vec::new();
        let error = configure_frozen_target_schema(&int_input, &string_target, &mut candidates)
            .unwrap_err();
        assert!(error.message.contains("input field 'value' has type Int32"));

        // Float16 is intentionally widened only for reads. The SQL write path
        // rejects coerced columns, and the distributed worker must not narrow
        // Float32 input back to Float16 implicitly.
        let float_input = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float32,
            true,
        )]));
        let half_target = Schema::new(vec![Field::new("value", DataType::Float16, true)]);
        let error = configure_frozen_target_schema(&float_input, &half_target, &mut candidates)
            .unwrap_err();
        assert!(error.message.contains("frozen target has type Float16"));

        let nullable_input = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let required_target = Schema::new(vec![Field::new("value", DataType::Int32, false)]);
        let error =
            configure_frozen_target_schema(&nullable_input, &required_target, &mut candidates)
                .unwrap_err();
        assert!(error.message.contains(
            "input field 'value' is nullable, but the frozen target field is non-nullable"
        ));

        let nested_input = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Struct(vec![Arc::new(Field::new("child", DataType::Int32, true))].into()),
            true,
        )]));
        let nested_target = Schema::new(vec![Field::new(
            "value",
            DataType::Struct(vec![Arc::new(Field::new("child", DataType::Int32, false))].into()),
            true,
        )]);
        let error = configure_frozen_target_schema(&nested_input, &nested_target, &mut candidates)
            .unwrap_err();
        assert!(error.message.contains("frozen target has type Struct"));

        let string_input = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::LargeUtf8,
            true,
        )]));
        configure_frozen_target_schema(&string_input, &string_target, &mut candidates).unwrap();

        let duckdb_list_child = Arc::new(Field::new("l", DataType::Float32, true));
        let upstream_list_child = Arc::new(Field::new("item", DataType::Float32, true));
        let vector_input = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::List(duckdb_list_child),
            true,
        )]));
        let vector_target = Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(upstream_list_child.clone(), 3),
            true,
        )]);
        let mut candidates = vec![VectorConversion {
            col_idx: 0,
            dim: 0,
            list_kind: VectorListKind::List,
            element_type: VectorElementType::Float32,
        }];
        configure_frozen_target_schema(&vector_input, &vector_target, &mut candidates).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].dim, 3);

        let variable_target = Schema::new(vec![Field::new(
            "vector",
            DataType::List(upstream_list_child.clone()),
            true,
        )]);
        configure_frozen_target_schema(&vector_input, &variable_target, &mut candidates).unwrap();
        assert!(candidates.is_empty());

        let duckdb_fixed =
            DataType::FixedSizeList(Arc::new(Field::new("", DataType::Float32, true)), 3);
        let upstream_fixed = DataType::FixedSizeList(upstream_list_child, 3);
        assert!(frozen_target_type_compatible(
            &duckdb_fixed,
            &upstream_fixed
        ));

        let custom_list = DataType::List(Arc::new(Field::new(
            "custom_element",
            DataType::Float32,
            true,
        )));
        assert!(!frozen_target_type_compatible(
            &custom_list,
            vector_input.field(0).data_type()
        ));

        let renamed_nested_target = Schema::new(vec![Field::new(
            "value",
            DataType::Struct(vec![Arc::new(Field::new("renamed", DataType::Int32, true))].into()),
            true,
        )]);
        let error =
            configure_frozen_target_schema(&nested_input, &renamed_nested_target, &mut candidates)
                .unwrap_err();
        assert!(error.message.contains("frozen target has type Struct"));
    }

    #[test]
    fn frozen_target_values_enforce_nested_nullability() {
        let required_value = Arc::new(Field::new("value", DataType::Int32, false));
        let payload = Field::new(
            "payload",
            DataType::Struct(vec![required_value].into()),
            true,
        );
        let input_fields: arrow_schema::Fields =
            vec![Arc::new(Field::new("value", DataType::Int32, true))].into();
        let child = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;
        let active_struct = StructArray::new(input_fields.clone(), vec![child.clone()], None);
        let error = validate_frozen_target_field_values(&payload, &active_struct, payload.name())
            .unwrap_err();
        assert!(error.message.contains("'payload.value' contains a null"));

        let masked_struct = StructArray::new(
            input_fields,
            vec![child],
            Some(arrow::buffer::NullBuffer::from(vec![true, false])),
        );
        let error = validate_frozen_target_field_values(&payload, &masked_struct, payload.name())
            .unwrap_err();
        assert!(error.message.contains("'payload.value' contains a null"));

        let required_item = Arc::new(Field::new("item", DataType::Int32, false));
        let items = Field::new("items", DataType::List(required_item), true);
        let mut active_list_builder = ListBuilder::new(Int32Builder::new());
        active_list_builder.values().append_value(1);
        active_list_builder.values().append_null();
        active_list_builder.append(true);
        let active_list = active_list_builder.finish();
        let error =
            validate_frozen_target_field_values(&items, &active_list, items.name()).unwrap_err();
        assert!(error.message.contains("'items.item' contains a null"));

        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("keys", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Int32, false)),
                ]
                .into(),
            ),
            false,
        ));
        let attributes = Field::new("attributes", DataType::Map(entries, false), true);
        let mut active_map_builder =
            MapBuilder::new(None, Int32Builder::new(), Int32Builder::new());
        active_map_builder.keys().append_value(1);
        active_map_builder.values().append_null();
        active_map_builder.append(true).unwrap();
        let active_map = active_map_builder.finish();
        let error =
            validate_frozen_target_field_values(&attributes, &active_map, attributes.name())
                .unwrap_err();
        assert!(error
            .message
            .contains("'attributes.entries.values' contains a null"));
    }

    #[test]
    fn failed_uncommitted_write_cleans_completed_fragments() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        let uri = format!("memory://failed-write-cleanup-{}", rand::random::<u64>());
        let dataset = runtime::block_on(Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone()),
            uri.as_str(),
            None,
        ))
        .unwrap()
        .unwrap();
        let objects_before = data_objects(&dataset);

        let params = WriteParams {
            mode: WriteMode::Append,
            max_rows_per_file: 1,
            max_rows_per_group: 1,
            progress: Arc::new(FailCompletedFragment),
            // This skips post-commit version cleanup only. Lance must still
            // remove uncommitted data artifacts when the write itself fails.
            skip_auto_cleanup: true,
            ..Default::default()
        };
        let source = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let error = runtime::block_on(
            InsertBuilder::new(Arc::new(dataset.clone()))
                .with_params(&params)
                .execute_uncommitted_stream(source),
        )
        .unwrap()
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("injected completed-fragment failure"));
        assert_eq!(data_objects(&dataset), objects_before);
    }
}
