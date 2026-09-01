use std::cmp::Ordering;
use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::builder::Float32Builder;
use arrow_array::{Float32Array, UInt64Array};
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use datafusion_expr::Expr;
use datafusion_proto::bytes::Serializeable;
use lance::dataset::ProjectionRequest;
use lance::io::exec::fts::{FlatMatchFilterExec, FlatMatchQueryExec, MatchQueryExec};
use lance_datafusion::exec::{execute_plan, LanceExecutionOptions};
use lance_datafusion::planner::Planner;
use lance_index::scalar::FullTextSearchQuery;
use lance_table::format::pb;
use prost::Message;
use sha2::{Digest, Sha256};

use crate::constants::{DISTANCE_COLUMN, HYBRID_SCORE_COLUMN, ROW_ID_COLUMN, SCORE_COLUMN};
use crate::datafusion_stream::DataFusionStream;
use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;
use crate::scanner::LanceStream;

use super::dataset::{load_supported_raw_index_metadata, seed_frozen_index_metadata};
use super::projection;
use super::types::{DatasetHandle, StreamHandle};
use super::util::{
    cstr_to_str, dataset_handle, nonzero_u64_to_usize, optional_vane_session_handle,
    parse_optional_filter_ir, slice_from_ptr, FfiError, FfiResult,
};
use super::vane_search_plan::{build_search_index_plan, SearchIndexPlan, SearchKind};

const NAMESPACE_FILTER_MAGIC: &[u8; 4] = b"LNF1";
const NAMESPACE_FILTER_VERSION: u16 = 1;
const MAX_SERIALIZED_INDEX_SECTION_BYTES: usize = 256 * 1024 * 1024;

unsafe fn optional_cstr<'a>(
    value: *const c_char,
    what: &'static str,
) -> FfiResult<Option<&'a str>> {
    if value.is_null() {
        return Ok(None);
    }
    let value = unsafe { cstr_to_str(value, what)? };
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value))
    }
}

fn runtime_result<T, E>(value: Result<Result<T, E>, std::io::Error>, what: &str) -> FfiResult<T>
where
    E: std::fmt::Display,
{
    value
        .map_err(|err| FfiError::new(ErrorCode::Runtime, format!("{what} runtime: {err}")))?
        .map_err(|err| FfiError::new(ErrorCode::InvalidArgument, format!("{what}: {err}")))
}

#[no_mangle]
pub unsafe extern "C" fn lance_vane_sha256(
    input: *const u8,
    input_len: usize,
    output: *mut u8,
) -> i32 {
    if output.is_null() {
        set_last_error(ErrorCode::InvalidArgument, "sha256 output is null");
        return -1;
    }
    let input = if input_len == 0 {
        &[][..]
    } else {
        match unsafe { slice_from_ptr(input, input_len, "sha256 input") } {
            Ok(input) => input,
            Err(err) => {
                set_last_error(err.code, err.message);
                return -1;
            }
        }
    };
    let digest = Sha256::digest(input);
    unsafe { ptr::copy_nonoverlapping(digest.as_ptr(), output, digest.len()) };
    clear_last_error();
    0
}

#[no_mangle]
pub unsafe extern "C" fn lance_vane_free_bytes(data: *mut u8, len: usize) {
    if data.is_null() {
        return;
    }
    let slice = ptr::slice_from_raw_parts_mut(data, len);
    unsafe { drop(Box::from_raw(slice)) };
}

fn schema_fingerprint(schema: &Schema) -> FfiResult<[u8; 32]> {
    // Arrow's serde representation includes field order, nullability, nested
    // types, and metadata. Canonicalize every JSON object explicitly because
    // Arrow metadata is backed by HashMap and serde_json's map representation
    // can be feature-dependent.
    fn canonicalize(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(canonicalize).collect())
            }
            serde_json::Value::Object(values) => {
                let mut values = values.into_iter().collect::<Vec<_>>();
                values.sort_by(|left, right| left.0.cmp(&right.0));
                serde_json::Value::Object(
                    values
                        .into_iter()
                        .map(|(key, value)| (key, canonicalize(value)))
                        .collect(),
                )
            }
            value => value,
        }
    }

    let value = serde_json::to_value(schema)
        .map(canonicalize)
        .map_err(|err| {
            FfiError::new(
                ErrorCode::SchemaExport,
                format!("distributed schema fingerprint: {err}"),
            )
        })?;
    let bytes = serde_json::to_vec(&value).map_err(|err| {
        FfiError::new(
            ErrorCode::SchemaExport,
            format!("distributed schema fingerprint: {err}"),
        )
    })?;
    Ok(Sha256::digest(bytes).into())
}

#[no_mangle]
pub unsafe extern "C" fn lance_vane_dataset_schema_fingerprint(
    dataset: *mut c_void,
    output: *mut u8,
) -> i32 {
    let result = (|| -> FfiResult<[u8; 32]> {
        if output.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "schema fingerprint output is null",
            ));
        }
        let handle = unsafe { dataset_handle(dataset)? };
        schema_fingerprint(handle.arrow_schema.as_ref())
    })();
    match result {
        Ok(digest) => {
            unsafe { ptr::copy_nonoverlapping(digest.as_ptr(), output, digest.len()) };
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_vane_arrow_schema_fingerprint(
    schema: *const arrow_schema::ffi::FFI_ArrowSchema,
    output: *mut u8,
) -> i32 {
    let result = (|| -> FfiResult<[u8; 32]> {
        if schema.is_null() || output.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "Arrow schema fingerprint input is null",
            ));
        }
        let schema = Schema::try_from(unsafe { &*schema }).map_err(|err| {
            FfiError::new(
                ErrorCode::SchemaExport,
                format!("distributed Arrow schema import: {err}"),
            )
        })?;
        schema_fingerprint(&schema)
    })();
    match result {
        Ok(digest) => {
            unsafe { ptr::copy_nonoverlapping(digest.as_ptr(), output, digest.len()) };
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

fn encode_namespace_filter_plan(schema: Arc<Schema>, sql: &str) -> FfiResult<Vec<u8>> {
    let expr = Planner::new(schema).parse_filter(sql).map_err(|err| {
        FfiError::new(
            ErrorCode::InvalidArgument,
            format!("namespace filter planning: {err}"),
        )
    })?;
    let encoded = expr.to_bytes().map_err(|err| {
        FfiError::new(
            ErrorCode::InvalidArgument,
            format!("namespace filter serialization: {err}"),
        )
    })?;
    let mut result = Vec::with_capacity(6 + encoded.len());
    result.extend_from_slice(NAMESPACE_FILTER_MAGIC);
    result.extend_from_slice(&NAMESPACE_FILTER_VERSION.to_le_bytes());
    result.extend_from_slice(encoded.as_ref());
    Ok(result)
}

#[no_mangle]
pub unsafe extern "C" fn lance_vane_plan_namespace_filter(
    dataset: *mut c_void,
    sql: *const c_char,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if !out_data.is_null() {
        unsafe { ptr::write_unaligned(out_data, ptr::null_mut()) };
    }
    if !out_len.is_null() {
        unsafe { ptr::write_unaligned(out_len, 0) };
    }
    let result = (|| -> FfiResult<Vec<u8>> {
        if out_data.is_null() || out_len.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "namespace filter plan output pointers are null",
            ));
        }
        let handle = unsafe { dataset_handle(dataset)? };
        let sql = unsafe { cstr_to_str(sql, "namespace_filter")? };
        if sql.is_empty() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "namespace filter must not be empty",
            ));
        }
        encode_namespace_filter_plan(handle.arrow_schema.clone(), sql)
    })();
    match result {
        Ok(bytes) => {
            let mut bytes = bytes.into_boxed_slice();
            let len = bytes.len();
            let data = bytes.as_mut_ptr();
            std::mem::forget(bytes);
            unsafe {
                ptr::write_unaligned(out_data, data);
                ptr::write_unaligned(out_len, len);
            }
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_vane_serialize_dataset_index_section(
    dataset: *mut c_void,
    session: *mut c_void,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if !out_data.is_null() {
        unsafe { ptr::write_unaligned(out_data, ptr::null_mut()) };
    }
    if !out_len.is_null() {
        unsafe { ptr::write_unaligned(out_len, 0) };
    }
    let result = (|| -> FfiResult<Vec<u8>> {
        if out_data.is_null() || out_len.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "index section output pointers are null",
            ));
        }
        let handle = unsafe { dataset_handle(dataset)? };
        let session = unsafe { optional_vane_session_handle(session)? }.ok_or_else(|| {
            FfiError::new(
                ErrorCode::InvalidArgument,
                "coordinator-frozen index metadata requires a shared Lance session",
            )
        })?;
        if !Arc::ptr_eq(&handle.dataset.session(), &session.session) {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "coordinator dataset does not use the supplied shared Lance session",
            ));
        }
        let indices = match runtime::block_on(async {
            let indices = load_supported_raw_index_metadata(handle.dataset.as_ref()).await?;
            seed_frozen_index_metadata(handle.dataset.as_ref(), session, &indices).await?;
            Ok::<_, FfiError>(indices)
        }) {
            Ok(result) => result?,
            Err(err) => {
                return Err(FfiError::new(
                    ErrorCode::Runtime,
                    format!("freeze coordinator index section runtime: {err}"),
                ));
            }
        };
        let bytes = pb::IndexSection {
            indices: indices.iter().map(pb::IndexMetadata::from).collect(),
        }
        .encode_to_vec();
        if bytes.len() > MAX_SERIALIZED_INDEX_SECTION_BYTES {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "serialized index section exceeds {} bytes",
                    MAX_SERIALIZED_INDEX_SECTION_BYTES
                ),
            ));
        }
        Ok(bytes)
    })();

    match result {
        Ok(bytes) => {
            let mut bytes = bytes.into_boxed_slice();
            let len = bytes.len();
            let data = bytes.as_mut_ptr();
            std::mem::forget(bytes);
            unsafe {
                ptr::write_unaligned(out_data, data);
                ptr::write_unaligned(out_len, len);
            }
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_vane_build_search_index_plan(
    dataset: *mut c_void,
    generation: *const c_char,
    search_kind: u8,
    vector_column: *const c_char,
    text_column: *const c_char,
    use_vector_index: u8,
    out_data: *mut *mut u8,
    out_len: *mut usize,
) -> i32 {
    if !out_data.is_null() {
        unsafe { ptr::write_unaligned(out_data, ptr::null_mut()) };
    }
    if !out_len.is_null() {
        unsafe { ptr::write_unaligned(out_len, 0) };
    }
    let result = (|| -> FfiResult<Vec<u8>> {
        if out_data.is_null() || out_len.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "search index plan output pointers are null",
            ));
        }
        let handle = unsafe { dataset_handle(dataset)? };
        let generation = unsafe { cstr_to_str(generation, "generation")? };
        let kind = SearchKind::try_from(search_kind)
            .map_err(|err| FfiError::new(ErrorCode::InvalidArgument, err.to_string()))?;
        let vector_column = unsafe { optional_cstr(vector_column, "vector_column")? };
        let text_column = unsafe { optional_cstr(text_column, "text_column")? };
        runtime_result(
            runtime::block_on(build_search_index_plan(
                handle.dataset.as_ref(),
                generation,
                kind,
                vector_column,
                text_column,
                use_vector_index != 0,
            )),
            "build LSI1",
        )
    })();

    match result {
        Ok(bytes) => {
            let mut bytes = bytes.into_boxed_slice();
            let len = bytes.len();
            let data = bytes.as_mut_ptr();
            std::mem::forget(bytes);
            unsafe {
                ptr::write_unaligned(out_data, data);
                ptr::write_unaligned(out_len, len);
            }
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

unsafe fn parse_index_plan<'a>(data: *const u8, len: usize) -> FfiResult<&'a [u8]> {
    if len == 0 {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "LSI1 plan must not be empty",
        ));
    }
    unsafe { slice_from_ptr(data, len, "LSI1 plan") }
}

unsafe fn combined_filter(
    filter_ir: *const u8,
    filter_ir_len: usize,
    namespace_filter_plan: *const u8,
    namespace_filter_plan_len: usize,
    code: ErrorCode,
) -> FfiResult<Option<Expr>> {
    let filter = unsafe {
        parse_optional_filter_ir(
            filter_ir,
            filter_ir_len,
            code,
            "distributed search filter_ir",
        )?
    };
    let namespace_filter = if namespace_filter_plan_len == 0 {
        if !namespace_filter_plan.is_null() {
            return Err(FfiError::new(
                code,
                "namespace filter plan pointer is non-null for an empty plan",
            ));
        }
        None
    } else {
        let bytes = unsafe {
            slice_from_ptr(
                namespace_filter_plan,
                namespace_filter_plan_len,
                "namespace filter plan",
            )?
        };
        Some(decode_namespace_filter_plan(bytes, code)?)
    };
    Ok(match (filter, namespace_filter) {
        (Some(left), Some(right)) => Some(left.and(right)),
        (Some(filter), None) | (None, Some(filter)) => Some(filter),
        (None, None) => None,
    })
}

fn decode_namespace_filter_plan(bytes: &[u8], code: ErrorCode) -> FfiResult<Expr> {
    if bytes.len() <= 6 || &bytes[..4] != NAMESPACE_FILTER_MAGIC {
        return Err(FfiError::new(code, "namespace filter plan is malformed"));
    }
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != NAMESPACE_FILTER_VERSION {
        return Err(FfiError::new(
            code,
            format!("unsupported namespace filter plan version {version}"),
        ));
    }
    Expr::from_bytes(&bytes[6..])
        .map_err(|err| FfiError::new(code, format!("namespace filter plan decode: {err}")))
}

async fn validate_plan(
    handle: &DatasetHandle,
    plan_bytes: &[u8],
    generation: &str,
    kind: SearchKind,
    vector_column: Option<&str>,
    text_column: Option<&str>,
    use_vector_index: bool,
) -> anyhow::Result<super::vane_search_plan::ValidatedSearchIndexPlan> {
    SearchIndexPlan::decode(plan_bytes)?
        .validate(
            handle.dataset.as_ref(),
            generation,
            kind,
            vector_column,
            text_column,
            use_vector_index,
        )
        .await
}

#[allow(clippy::too_many_arguments)]
fn create_exact_vector_stream(
    handle: &DatasetHandle,
    vector_column: &str,
    query_values: &[f32],
    k: usize,
    nprobes: u64,
    refine_factor: u64,
    filter: Option<Expr>,
    prefilter: bool,
    use_index: bool,
    fragments: Vec<lance_table::format::Fragment>,
    index_segments: Vec<lance_table::format::IndexMetadata>,
    project_row_id_only: bool,
) -> FfiResult<LanceStream> {
    let mut scan = handle.dataset.scan();
    scan.prefilter(prefilter);
    if let Some(filter) = filter {
        scan.filter_expr(filter);
    }
    let query = Float32Array::from_iter_values(query_values.iter().copied());
    scan.nearest(vector_column, &query, k).map_err(|err| {
        FfiError::new(
            ErrorCode::KnnStreamCreate,
            format!("distributed vector nearest: {err}"),
        )
    })?;
    // Lance rejects `nearest` when a fragment restriction was installed
    // first for a post-filter query.  Install the already-validated frozen
    // fragment set after `nearest`; Lance then uses it both to restrict the
    // selected index segments and to flat-scan their uncovered fragments.
    scan.with_fragments(fragments);
    if nprobes != 0 {
        scan.nprobes(nonzero_u64_to_usize(nprobes, "nprobes")?);
    }
    if refine_factor != 0 {
        let refine: u32 = refine_factor.try_into().map_err(|_| {
            FfiError::new(ErrorCode::InvalidArgument, "refine_factor must fit in u32")
        })?;
        scan.refine(refine);
    }
    if use_index && !index_segments.is_empty() {
        scan.use_index(true);
        scan.with_index_segments(index_segments.iter().map(|item| item.uuid).collect())
            .map_err(|err| {
                FfiError::new(
                    ErrorCode::KnnStreamCreate,
                    format!("distributed vector segments: {err}"),
                )
            })?;
    } else {
        // This is intentional: a frozen flat decision must not start using an
        // index that appeared after coordinator admission.
        scan.use_index(false);
    }
    scan.disable_scoring_autoprojection();
    if project_row_id_only {
        scan.with_row_id();
        scan.project(&[ROW_ID_COLUMN, DISTANCE_COLUMN])
    } else {
        let projection = projection::build_knn_projection(&handle.base_projection);
        scan.project(projection.as_ref())
    }
    .map_err(|err| {
        FfiError::new(
            ErrorCode::KnnStreamCreate,
            format!("distributed vector projection: {err}"),
        )
    })?;
    scan.scan_in_order(false);
    LanceStream::from_scanner(scan).map_err(|err| {
        FfiError::new(
            ErrorCode::KnnStreamCreate,
            format!("distributed vector stream: {err}"),
        )
    })
}

fn rewrite_fts_plan(
    plan: Arc<dyn ExecutionPlan>,
    segments: &[lance_table::format::IndexMetadata],
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    let children = plan
        .children()
        .into_iter()
        .map(|child| rewrite_fts_plan(child.clone(), segments))
        .collect::<datafusion_common::Result<Vec<_>>>()?;
    let plan = with_new_children_if_necessary(plan, children)?;

    if let Some(exec) = plan.as_ref().downcast_ref::<MatchQueryExec>() {
        if segments.is_empty() {
            return Err(datafusion_common::DataFusionError::Plan(
                "indexed FTS plan has no frozen index segments".to_string(),
            ));
        }
        let mut replacement = MatchQueryExec::new_with_segments(
            exec.dataset().clone(),
            exec.query().clone(),
            exec.params().clone(),
            exec.prefilter_source().clone(),
            segments.to_vec(),
        );
        if let Some(scorer) = exec.base_scorer() {
            replacement = replacement.with_base_scorer(scorer.clone());
        }
        return Ok(Arc::new(replacement));
    }
    if let Some(exec) = plan.as_ref().downcast_ref::<FlatMatchQueryExec>() {
        if segments.is_empty() {
            return Ok(plan);
        }
        let input = plan.children()[0].clone();
        let mut replacement = FlatMatchQueryExec::new_with_segments(
            exec.dataset().clone(),
            exec.query().clone(),
            exec.params().clone(),
            input,
            segments.to_vec(),
        );
        if let Some(scorer) = exec.base_scorer() {
            replacement = replacement.with_base_scorer(scorer.clone());
        }
        return Ok(Arc::new(replacement));
    }
    if let Some(exec) = plan.as_ref().downcast_ref::<FlatMatchFilterExec>() {
        if segments.is_empty() {
            return Ok(plan);
        }
        let input = plan.children()[0].clone();
        return Ok(Arc::new(FlatMatchFilterExec::new_with_segments(
            input,
            exec.dataset().clone(),
            exec.query().clone(),
            exec.params().clone(),
            segments.to_vec(),
        )));
    }
    Ok(plan)
}

#[allow(clippy::too_many_arguments)]
fn create_exact_fts_stream(
    handle: &DatasetHandle,
    text_column: &str,
    query: &str,
    k: usize,
    filter: Option<Expr>,
    prefilter: bool,
    fragments: Vec<lance_table::format::Fragment>,
    index_segments: Vec<lance_table::format::IndexMetadata>,
    project_row_id_only: bool,
) -> FfiResult<StreamHandle> {
    let limit = i64::try_from(k)
        .map_err(|_| FfiError::new(ErrorCode::InvalidArgument, "k must fit in i64"))?;
    let query = FullTextSearchQuery::new(query.to_string())
        .with_column(text_column.to_string())
        .map_err(|err| {
            FfiError::new(
                ErrorCode::FtsStreamCreate,
                format!("distributed FTS query: {err}"),
            )
        })?
        .limit(Some(limit));
    let mut scan = handle.dataset.scan();
    scan.with_fragments(fragments);
    scan.prefilter(prefilter);
    if let Some(filter) = filter {
        scan.filter_expr(filter);
    }
    scan.full_text_search(query).map_err(|err| {
        FfiError::new(
            ErrorCode::FtsStreamCreate,
            format!("distributed FTS search: {err}"),
        )
    })?;
    if project_row_id_only {
        scan.with_row_id();
        scan.disable_scoring_autoprojection();
        scan.project(&[ROW_ID_COLUMN, SCORE_COLUMN])
    } else {
        scan.disable_scoring_autoprojection();
        scan.project(handle.fts_projection.as_ref())
    }
    .map_err(|err| {
        FfiError::new(
            ErrorCode::FtsStreamCreate,
            format!("distributed FTS projection: {err}"),
        )
    })?;
    scan.scan_in_order(false);

    // `execute_plan` starts DataFusion operators immediately.  Some of those
    // operators spawn Tokio tasks during `ExecutionPlan::execute`, so both
    // plan creation and stream construction must run while the Lance runtime
    // is entered.  `DataFusionStream` retains that runtime's handle for later
    // polling from the synchronous Arrow C stream callbacks.
    let stream = runtime::block_on(async {
        let plan = scan
            .create_plan()
            .await
            .map_err(|err| format!("create FTS plan: {err}"))?;
        let plan = rewrite_fts_plan(plan, &index_segments)
            .map_err(|err| format!("freeze FTS segments: {err}"))?;
        execute_plan(plan, LanceExecutionOptions::default())
            .map_err(|err| format!("execute distributed FTS plan: {err}"))
    })
    .map_err(|err| {
        FfiError::new(
            ErrorCode::Runtime,
            format!("distributed FTS runtime: {err}"),
        )
    })?
    .map_err(|err| FfiError::new(ErrorCode::FtsStreamCreate, err))?;
    Ok(StreamHandle::DataFusion(
        DataFusionStream::try_new(stream).map_err(|err| {
            FfiError::new(
                ErrorCode::FtsStreamCreate,
                format!("distributed FTS stream: {err}"),
            )
        })?,
    ))
}

#[no_mangle]
pub unsafe extern "C" fn lance_vane_create_knn_stream_ir(
    dataset: *mut c_void,
    generation: *const c_char,
    vector_column: *const c_char,
    query_values: *const f32,
    query_len: usize,
    k: u64,
    nprobes: u64,
    refine_factor: u64,
    filter_ir: *const u8,
    filter_ir_len: usize,
    namespace_filter_plan: *const u8,
    namespace_filter_plan_len: usize,
    prefilter: u8,
    use_index: u8,
    index_plan: *const u8,
    index_plan_len: usize,
) -> *mut c_void {
    let result = (|| -> FfiResult<StreamHandle> {
        let handle = unsafe { dataset_handle(dataset)? };
        let generation = unsafe { cstr_to_str(generation, "generation")? };
        let vector_column = unsafe { cstr_to_str(vector_column, "vector_column")? };
        let query_values = unsafe { slice_from_ptr(query_values, query_len, "query_values")? };
        let k = nonzero_u64_to_usize(k, "k")?;
        let bytes = unsafe { parse_index_plan(index_plan, index_plan_len)? };
        let validated = runtime_result(
            runtime::block_on(validate_plan(
                handle,
                bytes,
                generation,
                SearchKind::Vector,
                Some(vector_column),
                None,
                use_index != 0,
            )),
            "validate LSI1",
        )?;
        let filter = unsafe {
            combined_filter(
                filter_ir,
                filter_ir_len,
                namespace_filter_plan,
                namespace_filter_plan_len,
                ErrorCode::KnnStreamCreate,
            )?
        };
        Ok(StreamHandle::Lance(create_exact_vector_stream(
            handle,
            vector_column,
            query_values,
            k,
            nprobes,
            refine_factor,
            filter,
            prefilter != 0,
            use_index != 0,
            validated.fragments,
            validated.vector_segments,
            false,
        )?))
    })();
    stream_result(result)
}

#[no_mangle]
pub unsafe extern "C" fn lance_vane_create_fts_stream_ir(
    dataset: *mut c_void,
    generation: *const c_char,
    text_column: *const c_char,
    query: *const c_char,
    k: u64,
    filter_ir: *const u8,
    filter_ir_len: usize,
    namespace_filter_plan: *const u8,
    namespace_filter_plan_len: usize,
    prefilter: u8,
    index_plan: *const u8,
    index_plan_len: usize,
) -> *mut c_void {
    let result = (|| -> FfiResult<StreamHandle> {
        let handle = unsafe { dataset_handle(dataset)? };
        let generation = unsafe { cstr_to_str(generation, "generation")? };
        let text_column = unsafe { cstr_to_str(text_column, "text_column")? };
        let query = unsafe { cstr_to_str(query, "query")? };
        let k = nonzero_u64_to_usize(k, "k")?;
        let bytes = unsafe { parse_index_plan(index_plan, index_plan_len)? };
        let validated = runtime_result(
            runtime::block_on(validate_plan(
                handle,
                bytes,
                generation,
                SearchKind::Fts,
                None,
                Some(text_column),
                false,
            )),
            "validate LSI1",
        )?;
        let filter = unsafe {
            combined_filter(
                filter_ir,
                filter_ir_len,
                namespace_filter_plan,
                namespace_filter_plan_len,
                ErrorCode::FtsStreamCreate,
            )?
        };
        create_exact_fts_stream(
            handle,
            text_column,
            query,
            k,
            filter,
            prefilter != 0,
            validated.fragments,
            validated.fts_segments,
            false,
        )
    })();
    stream_result(result)
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn lance_vane_create_hybrid_stream_ir(
    dataset: *mut c_void,
    generation: *const c_char,
    vector_column: *const c_char,
    query_values: *const f32,
    query_len: usize,
    text_column: *const c_char,
    text_query: *const c_char,
    k: u64,
    nprobes: u64,
    refine_factor: u64,
    filter_ir: *const u8,
    filter_ir_len: usize,
    namespace_filter_plan: *const u8,
    namespace_filter_plan_len: usize,
    prefilter: u8,
    use_index: u8,
    alpha: f32,
    oversample_factor: u32,
    index_plan: *const u8,
    index_plan_len: usize,
) -> *mut c_void {
    let result = (|| -> FfiResult<StreamHandle> {
        let handle = unsafe { dataset_handle(dataset)? };
        let generation = unsafe { cstr_to_str(generation, "generation")? };
        let vector_column = unsafe { cstr_to_str(vector_column, "vector_column")? };
        let text_column = unsafe { cstr_to_str(text_column, "text_column")? };
        let text_query = unsafe { cstr_to_str(text_query, "text_query")? };
        let query_values = unsafe { slice_from_ptr(query_values, query_len, "query_values")? };
        let k = nonzero_u64_to_usize(k, "k")?;
        let bytes = unsafe { parse_index_plan(index_plan, index_plan_len)? };
        let validated = runtime_result(
            runtime::block_on(validate_plan(
                handle,
                bytes,
                generation,
                SearchKind::Hybrid,
                Some(vector_column),
                Some(text_column),
                use_index != 0,
            )),
            "validate LSI1",
        )?;
        let filter = unsafe {
            combined_filter(
                filter_ir,
                filter_ir_len,
                namespace_filter_plan,
                namespace_filter_plan_len,
                ErrorCode::HybridStreamCreate,
            )?
        };
        let batch = create_exact_hybrid_batch(
            handle,
            vector_column,
            query_values,
            text_column,
            text_query,
            k,
            nprobes,
            refine_factor,
            filter,
            prefilter != 0,
            use_index != 0,
            alpha,
            oversample_factor,
            validated,
        )?;
        Ok(StreamHandle::Batches(vec![batch].into_iter()))
    })();
    stream_result(result)
}

fn stream_result(result: FfiResult<StreamHandle>) -> *mut c_void {
    match result {
        Ok(stream) => {
            clear_last_error();
            Box::into_raw(Box::new(stream)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn create_exact_hybrid_batch(
    handle: &DatasetHandle,
    vector_column: &str,
    query_values: &[f32],
    text_column: &str,
    text_query: &str,
    k: usize,
    nprobes: u64,
    refine_factor: u64,
    filter: Option<Expr>,
    prefilter: bool,
    use_index: bool,
    alpha: f32,
    oversample_factor: u32,
    validated: super::vane_search_plan::ValidatedSearchIndexPlan,
) -> FfiResult<RecordBatch> {
    let oversample = k.saturating_mul(oversample_factor.max(1) as usize).max(k);
    let mut vector_stream = StreamHandle::Lance(create_exact_vector_stream(
        handle,
        vector_column,
        query_values,
        oversample,
        nprobes,
        refine_factor,
        filter.clone(),
        prefilter,
        use_index,
        validated.fragments.clone(),
        validated.vector_segments,
        true,
    )?);
    let vector_rows = collect_row_f32_pairs(&mut vector_stream, ROW_ID_COLUMN, DISTANCE_COLUMN)?;

    let mut fts_stream = create_exact_fts_stream(
        handle,
        text_column,
        text_query,
        oversample,
        filter,
        prefilter,
        validated.fragments,
        validated.fts_segments,
        true,
    )?;
    let fts_rows = collect_row_f32_pairs(&mut fts_stream, ROW_ID_COLUMN, SCORE_COLUMN)?;

    let alpha = alpha.clamp(0.0, 1.0);
    let (dist_min, dist_max) = finite_min_max(vector_rows.iter().map(|(_, value)| *value));
    let (score_min, score_max) = finite_min_max(fts_rows.iter().map(|(_, value)| *value));
    let mut merged = std::collections::HashMap::<u64, (Option<f32>, Option<f32>)>::new();
    for (row_id, distance) in vector_rows {
        merged
            .entry(row_id)
            .and_modify(|entry| {
                entry.0 = Some(entry.0.map_or(distance, |old| old.min(distance)));
            })
            .or_insert((Some(distance), None));
    }
    for (row_id, score) in fts_rows {
        merged
            .entry(row_id)
            .and_modify(|entry| {
                entry.1 = Some(entry.1.map_or(score, |old| old.max(score)));
            })
            .or_insert((None, Some(score)));
    }

    let mut ranked = merged
        .into_iter()
        .map(|(row_id, (distance, score))| {
            let vector_score = distance
                .map(|value| distance_similarity(value, dist_min, dist_max))
                .unwrap_or(0.0);
            let text_score = score
                .map(|value| normalize_value(value, score_min, score_max))
                .unwrap_or(0.0);
            let hybrid = alpha * vector_score + (1.0 - alpha) * text_score;
            (row_id, distance, score, hybrid)
        })
        .collect::<Vec<_>>();
    // Keep the native hybrid implementation's ordering contract exactly.  In
    // particular, equal scores remain unordered unless SQL adds a stable key.
    ranked.sort_by(|left, right| cmp_desc_f32(right.3, left.3));
    ranked.truncate(k);

    let row_ids = ranked.iter().map(|item| item.0).collect::<Vec<_>>();
    let projection =
        ProjectionRequest::from_columns(handle.base_projection.as_ref(), handle.dataset.schema());
    let rows = runtime_result(
        runtime::block_on(handle.dataset.take_rows(&row_ids, projection)),
        "hybrid take_rows",
    )?;

    let mut distance_builder = Float32Builder::with_capacity(rows.num_rows());
    let mut score_builder = Float32Builder::with_capacity(rows.num_rows());
    let mut hybrid_builder = Float32Builder::with_capacity(rows.num_rows());
    for (_, distance, score, hybrid) in &ranked {
        append_finite(&mut distance_builder, *distance);
        append_finite(&mut score_builder, *score);
        append_finite(&mut hybrid_builder, Some(*hybrid));
    }
    let mut columns = rows.columns().to_vec();
    columns.push(Arc::new(distance_builder.finish()) as Arc<dyn Array>);
    columns.push(Arc::new(score_builder.finish()) as Arc<dyn Array>);
    columns.push(Arc::new(hybrid_builder.finish()) as Arc<dyn Array>);
    let mut fields = rows.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        DISTANCE_COLUMN,
        DataType::Float32,
        true,
    )));
    fields.push(Arc::new(Field::new(SCORE_COLUMN, DataType::Float32, true)));
    fields.push(Arc::new(Field::new(
        HYBRID_SCORE_COLUMN,
        DataType::Float32,
        true,
    )));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|err| {
        FfiError::new(
            ErrorCode::HybridStreamCreate,
            format!("distributed hybrid batch: {err}"),
        )
    })
}

fn append_finite(builder: &mut Float32Builder, value: Option<f32>) {
    match value {
        Some(value) if value.is_finite() => builder.append_value(value),
        _ => builder.append_null(),
    }
}

fn collect_row_f32_pairs(
    stream: &mut StreamHandle,
    row_id_column: &str,
    value_column: &str,
) -> FfiResult<Vec<(u64, f32)>> {
    let mut result = Vec::new();
    loop {
        let batch = stream.next_batch().map_err(|err| {
            FfiError::new(
                ErrorCode::HybridStreamCreate,
                format!("distributed hybrid stream: {err}"),
            )
        })?;
        let Some(batch) = batch else {
            break;
        };
        let row_id_index = batch.schema().index_of(row_id_column).map_err(|_| {
            FfiError::new(
                ErrorCode::HybridStreamCreate,
                format!("batch missing {row_id_column}"),
            )
        })?;
        let value_index = batch.schema().index_of(value_column).map_err(|_| {
            FfiError::new(
                ErrorCode::HybridStreamCreate,
                format!("batch missing {value_column}"),
            )
        })?;
        let row_ids = batch
            .column(row_id_index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                FfiError::new(ErrorCode::HybridStreamCreate, "invalid row id column type")
            })?;
        let values = batch
            .column(value_index)
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| {
                FfiError::new(ErrorCode::HybridStreamCreate, "invalid score column type")
            })?;
        for index in 0..batch.num_rows() {
            if !row_ids.is_null(index) && !values.is_null(index) {
                result.push((row_ids.value(index), values.value(index)));
            }
        }
    }
    Ok(result)
}

fn finite_min_max(values: impl Iterator<Item = f32>) -> (f32, f32) {
    let mut min = f32::INFINITY;
    let mut max = f32::NEG_INFINITY;
    for value in values.filter(|value| value.is_finite()) {
        min = min.min(value);
        max = max.max(value);
    }
    if min.is_finite() {
        (min, max)
    } else {
        (0.0, 0.0)
    }
}

fn normalize_value(value: f32, min: f32, max: f32) -> f32 {
    if !value.is_finite() {
        return 0.0;
    }
    if (max - min).abs() < f32::EPSILON {
        return 0.5;
    }
    ((value - min) / (max - min)).clamp(0.0, 1.0)
}

fn distance_similarity(value: f32, min: f32, max: f32) -> f32 {
    if !value.is_finite() {
        return 0.0;
    }
    1.0 - normalize_value(value, min, max)
}

fn cmp_desc_f32(left: f32, right: f32) -> Ordering {
    match left.partial_cmp(&right) {
        Some(ordering) => ordering,
        None if left.is_nan() && right.is_nan() => Ordering::Equal,
        None if left.is_nan() => Ordering::Less,
        None => Ordering::Greater,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn hybrid_normalization_matches_native_constant_range_rule() {
        assert_eq!(normalize_value(3.0, 3.0, 3.0), 0.5);
        assert_eq!(distance_similarity(3.0, 3.0, 3.0), 0.5);
        assert_eq!(normalize_value(f32::NAN, 0.0, 1.0), 0.0);
        assert_eq!(distance_similarity(f32::NAN, 0.0, 1.0), 0.0);
    }

    #[test]
    fn sha256_matches_known_vector() {
        let digest = Sha256::digest(b"abc");
        assert_eq!(
            format!("{digest:x}"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn namespace_filter_is_planned_once_and_round_trips_as_an_expr() {
        let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int64, false)]));
        let expected = Planner::new(Arc::clone(&schema))
            .parse_filter("age >= 18")
            .expect("coordinator plan");
        let bytes = encode_namespace_filter_plan(schema, "age >= 18").expect("encode");
        assert_eq!(&bytes[..4], NAMESPACE_FILTER_MAGIC);
        assert_eq!(
            decode_namespace_filter_plan(&bytes, ErrorCode::InvalidArgument).expect("decode"),
            expected
        );
    }

    #[test]
    fn schema_fingerprint_covers_nullability_and_canonicalizes_metadata() {
        let mut first_metadata = HashMap::new();
        first_metadata.insert("lance:field_id".to_string(), "7".to_string());
        first_metadata.insert("custom".to_string(), "value".to_string());
        let mut second_metadata = HashMap::new();
        second_metadata.insert("custom".to_string(), "value".to_string());
        second_metadata.insert("lance:field_id".to_string(), "7".to_string());

        let first = Schema::new(vec![
            Field::new("value", DataType::Utf8, false).with_metadata(first_metadata)
        ]);
        let reordered = Schema::new(vec![
            Field::new("value", DataType::Utf8, false).with_metadata(second_metadata)
        ]);
        let nullable = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);

        assert_eq!(
            schema_fingerprint(&first).expect("first"),
            schema_fingerprint(&reordered).expect("reordered")
        );
        assert_ne!(
            schema_fingerprint(&first).expect("first"),
            schema_fingerprint(&nullable).expect("nullable")
        );
    }
}
