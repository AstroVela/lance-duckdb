use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr};
use std::ptr;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use datafusion::logical_expr::Expr;
#[cfg(feature = "vane-distributed")]
use datafusion::object_store::ObjectStoreExt;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::scalar::ScalarValue;
use datafusion_sql::unparser::expr_to_sql;
use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::statistics::DatasetStatisticsExt;
use lance::dataset::transaction::{Operation, Transaction};
#[cfg(feature = "vane-distributed")]
use lance::index::scalar::IndexDetails;
#[cfg(feature = "vane-distributed")]
use lance::session::index_caches::IndexMetadataKey;
#[cfg(feature = "vane-distributed")]
use lance::session::Session;
#[cfg(feature = "vane-distributed")]
use lance_table::format::{pb, IndexMetadata};
#[cfg(feature = "vane-distributed")]
use lance_table::io::manifest::read_manifest_indexes;
#[cfg(feature = "vane-distributed")]
use prost::Message;
use roaring::RoaringTreemap;
#[cfg(feature = "vane-distributed")]
use sha2::{Digest, Sha256};

use crate::constants::ROW_ID_COLUMN;
use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;

use super::session::record_dataset_open;
use super::types::DatasetHandle;
#[cfg(feature = "vane-distributed")]
use super::types::SessionHandle;
use super::update::{apply_deletions, build_row_id_index, CapturedRowIds};
use super::util::{
    cstr_to_str, optional_session_handle, parse_optional_filter_ir, slice_from_ptr, FfiError,
    FfiResult,
};
#[cfg(feature = "vane-distributed")]
use super::util::{
    optional_vane_session_handle, vane_object_store_params, with_explicit_aws_credentials,
};
#[cfg(feature = "vane-distributed")]
use super::vane_index_cache::VaneIndexCacheLease;

#[cfg(feature = "vane-distributed")]
// Keep this limit in sync with LANCE_VANE_MAX_SERIALIZED_MANIFEST_BYTES in
// src/include/lance_vane_snapshot.hpp.
const MAX_SERIALIZED_MANIFEST_BYTES: usize = 256 * 1024 * 1024;

#[cfg(feature = "vane-distributed")]
// Keep this limit in sync with LANCE_VANE_MAX_SERIALIZED_INDEX_SECTION_BYTES in
// src/include/lance_vane_search.hpp.
pub(super) const MAX_SERIALIZED_INDEX_SECTION_BYTES: usize = 256 * 1024 * 1024;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LanceFieldStats {
    pub field_id: u32,
    pub bytes_on_disk: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LanceFragmentStats {
    pub fragment_id: u64,
    /// Number of rows in the fragment. `-1` means unknown.
    pub num_rows: i64,
    /// Sum of known data file sizes in bytes. Missing/unknown sizes are treated as 0.
    pub bytes_on_disk: u64,
}

#[no_mangle]
pub unsafe extern "C" fn lance_open_dataset(path: *const c_char) -> *mut c_void {
    match open_dataset_inner(path, ptr::null_mut()) {
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
pub unsafe extern "C" fn lance_open_dataset_with_session(
    path: *const c_char,
    session: *mut c_void,
) -> *mut c_void {
    match open_dataset_inner(path, session) {
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

fn open_dataset_inner(path: *const c_char, session: *mut c_void) -> FfiResult<DatasetHandle> {
    let path_str = unsafe { cstr_to_str(path, "path")? };
    let session = unsafe { optional_session_handle(session)? };
    let dataset = match runtime::block_on(async {
        let mut builder = DatasetBuilder::from_uri(path_str);
        if let Some(session) = session {
            builder = builder.with_session(session);
        }
        builder.load().await
    }) {
        Ok(Ok(ds)) => Arc::new(ds),
        Ok(Err(err)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetOpen,
                format!("dataset open '{path_str}': {err}"),
            ))
        }
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };
    record_dataset_open();
    Ok(DatasetHandle::new(dataset))
}

#[no_mangle]
pub unsafe extern "C" fn lance_open_dataset_with_storage_options(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
) -> *mut c_void {
    match open_dataset_with_storage_options_inner(
        path,
        option_keys,
        option_values,
        options_len,
        ptr::null_mut(),
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
pub unsafe extern "C" fn lance_open_dataset_with_storage_options_and_session(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
) -> *mut c_void {
    match open_dataset_with_storage_options_inner(
        path,
        option_keys,
        option_values,
        options_len,
        session,
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

fn open_dataset_with_storage_options_inner(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
) -> FfiResult<DatasetHandle> {
    let path_str = unsafe { cstr_to_str(path, "path")? };
    let session = unsafe { optional_session_handle(session)? };

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

    let dataset = match runtime::block_on(async {
        #[cfg(feature = "vane-distributed")]
        let mut builder = DatasetBuilder::from_uri(path_str);
        #[cfg(feature = "vane-distributed")]
        {
            builder = with_explicit_aws_credentials(builder, &storage_options);
            builder = builder.with_storage_options(storage_options);
        }
        #[cfg(not(feature = "vane-distributed"))]
        let mut builder = DatasetBuilder::from_uri(path_str).with_storage_options(storage_options);
        if let Some(session) = session {
            builder = builder.with_session(session);
        }
        builder.load().await
    }) {
        Ok(Ok(ds)) => Arc::new(ds),
        Ok(Err(err)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetOpen,
                format!("dataset open '{path_str}': {err}"),
            ))
        }
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };

    record_dataset_open();
    Ok(DatasetHandle::new(dataset))
}

#[cfg(feature = "vane-distributed")]
#[no_mangle]
pub unsafe extern "C" fn lance_vane_open_dataset_version_with_session(
    path: *const c_char,
    version: u64,
    session: *mut c_void,
) -> *mut c_void {
    match vane_open_dataset_version_inner(path, version, None, session) {
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
#[no_mangle]
pub unsafe extern "C" fn lance_vane_open_dataset_version_with_storage_options_and_session(
    path: *const c_char,
    version: u64,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
) -> *mut c_void {
    let storage_options = match unsafe {
        // SAFETY: The caller supplies arrays containing `options_len` C string pointers.
        vane_storage_options_from_ffi(option_keys, option_values, options_len)
    } {
        Ok(storage_options) => storage_options,
        Err(err) => {
            set_last_error(err.code, err.message);
            return ptr::null_mut();
        }
    };
    match vane_open_dataset_version_inner(path, version, Some(storage_options), session) {
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
#[no_mangle]
pub unsafe extern "C" fn lance_vane_serialize_dataset_manifest(
    dataset: *mut c_void,
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
                "serialized manifest output pointers are null",
            ));
        }
        // SAFETY: dataset_handle validates the opaque pointer before dereferencing it.
        let handle = unsafe { super::util::dataset_handle(dataset)? };
        let manifest = pb::Manifest::from(handle.dataset.manifest()).encode_to_vec();
        if manifest.is_empty() {
            return Err(FfiError::new(
                ErrorCode::DatasetOpen,
                "serialized dataset manifest is empty",
            ));
        }
        if manifest.len() > MAX_SERIALIZED_MANIFEST_BYTES {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                format!(
                    "serialized dataset manifest is {} bytes; limit is {} bytes",
                    manifest.len(),
                    MAX_SERIALIZED_MANIFEST_BYTES
                ),
            ));
        }
        Ok(manifest)
    })();

    match result {
        Ok(manifest) => {
            let mut manifest = manifest.into_boxed_slice();
            let len = manifest.len();
            let data = manifest.as_mut_ptr();
            std::mem::forget(manifest);
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

#[cfg(feature = "vane-distributed")]
struct VaneFrozenDatasetPayload {
    manifest: *const u8,
    manifest_len: usize,
    index_section: Option<(*const u8, usize)>,
}

#[cfg(feature = "vane-distributed")]
#[no_mangle]
pub unsafe extern "C" fn lance_vane_open_dataset_version_from_manifest_with_session(
    path: *const c_char,
    version: u64,
    manifest: *const u8,
    manifest_len: usize,
    expected_generation: *const c_char,
    session: *mut c_void,
) -> *mut c_void {
    match unsafe {
        vane_open_dataset_version_from_manifest_inner(
            path,
            version,
            VaneFrozenDatasetPayload {
                manifest,
                manifest_len,
                index_section: None,
            },
            expected_generation,
            None,
            session,
        )
    } {
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
#[no_mangle]
pub unsafe extern "C" fn lance_vane_open_dataset_version_from_manifest_with_storage_options_and_session(
    path: *const c_char,
    version: u64,
    manifest: *const u8,
    manifest_len: usize,
    expected_generation: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
) -> *mut c_void {
    let storage_options = match unsafe {
        // SAFETY: The caller supplies arrays containing `options_len` C string pointers.
        vane_storage_options_from_ffi(option_keys, option_values, options_len)
    } {
        Ok(storage_options) => storage_options,
        Err(err) => {
            set_last_error(err.code, err.message);
            return ptr::null_mut();
        }
    };
    match unsafe {
        vane_open_dataset_version_from_manifest_inner(
            path,
            version,
            VaneFrozenDatasetPayload {
                manifest,
                manifest_len,
                index_section: None,
            },
            expected_generation,
            Some(storage_options),
            session,
        )
    } {
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
#[no_mangle]
pub unsafe extern "C" fn lance_vane_open_dataset_version_from_manifest_and_index_section_with_session(
    path: *const c_char,
    version: u64,
    manifest: *const u8,
    manifest_len: usize,
    index_section: *const u8,
    index_section_len: usize,
    expected_generation: *const c_char,
    session: *mut c_void,
) -> *mut c_void {
    match unsafe {
        vane_open_dataset_version_from_manifest_inner(
            path,
            version,
            VaneFrozenDatasetPayload {
                manifest,
                manifest_len,
                index_section: Some((index_section, index_section_len)),
            },
            expected_generation,
            None,
            session,
        )
    } {
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
#[no_mangle]
pub unsafe extern "C" fn lance_vane_open_dataset_version_from_manifest_and_index_section_with_storage_options_and_session(
    path: *const c_char,
    version: u64,
    manifest: *const u8,
    manifest_len: usize,
    index_section: *const u8,
    index_section_len: usize,
    expected_generation: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
) -> *mut c_void {
    let storage_options = match unsafe {
        // SAFETY: The caller supplies arrays containing `options_len` C string pointers.
        vane_storage_options_from_ffi(option_keys, option_values, options_len)
    } {
        Ok(storage_options) => storage_options,
        Err(err) => {
            set_last_error(err.code, err.message);
            return ptr::null_mut();
        }
    };
    match unsafe {
        vane_open_dataset_version_from_manifest_inner(
            path,
            version,
            VaneFrozenDatasetPayload {
                manifest,
                manifest_len,
                index_section: Some((index_section, index_section_len)),
            },
            expected_generation,
            Some(storage_options),
            session,
        )
    } {
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
unsafe fn vane_storage_options_from_ffi(
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
    if options_len == 0 {
        return Ok(HashMap::new());
    }

    // SAFETY: The caller guarantees both pointer arrays contain `options_len` entries.
    let keys = unsafe { slice_from_ptr(option_keys, options_len, "option_keys")? };
    // SAFETY: The caller guarantees both pointer arrays contain `options_len` entries.
    let values = unsafe { slice_from_ptr(option_values, options_len, "option_values")? };
    let mut storage_options = HashMap::with_capacity(options_len);
    for (idx, (&key_ptr, &value_ptr)) in keys.iter().zip(values.iter()).enumerate() {
        if key_ptr.is_null() || value_ptr.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                format!("option key/value is null at index {idx}"),
            ));
        }
        // SAFETY: Each non-null pointer is required to reference a NUL-terminated C string.
        let key = unsafe { CStr::from_ptr(key_ptr) }.to_str().map_err(|err| {
            FfiError::new(ErrorCode::Utf8, format!("option_keys[{idx}] utf8: {err}"))
        })?;
        // SAFETY: Each non-null pointer is required to reference a NUL-terminated C string.
        let value = unsafe { CStr::from_ptr(value_ptr) }
            .to_str()
            .map_err(|err| {
                FfiError::new(ErrorCode::Utf8, format!("option_values[{idx}] utf8: {err}"))
            })?;
        storage_options.insert(key.to_string(), value.to_string());
    }
    Ok(storage_options)
}

#[cfg(feature = "vane-distributed")]
fn vane_versioned_dataset_builder(
    path: &str,
    version: u64,
    storage_options: Option<&HashMap<String, String>>,
) -> DatasetBuilder {
    let mut builder = DatasetBuilder::from_uri(path).with_version(version);
    if let Some(storage_options) = storage_options {
        builder = with_explicit_aws_credentials(builder, storage_options);
        builder = builder.with_storage_options(storage_options.clone());
    }
    builder
}

#[cfg(feature = "vane-distributed")]
fn decode_frozen_index_section(bytes: &[u8]) -> FfiResult<Vec<IndexMetadata>> {
    if bytes.len() > MAX_SERIALIZED_INDEX_SECTION_BYTES {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!(
                "serialized index section length must not exceed {} bytes",
                MAX_SERIALIZED_INDEX_SECTION_BYTES
            ),
        ));
    }
    let section = pb::IndexSection::decode(bytes).map_err(|err| {
        FfiError::new(
            ErrorCode::InvalidArgument,
            format!("decode coordinator-frozen index section: {err}"),
        )
    })?;
    section
        .indices
        .into_iter()
        .map(|metadata| {
            if let Some(created_at) = metadata.created_at {
                let created_at = i64::try_from(created_at).map_err(|_| {
                    FfiError::new(
                        ErrorCode::InvalidArgument,
                        "coordinator-frozen index timestamp is out of range",
                    )
                })?;
                if chrono::DateTime::from_timestamp_millis(created_at).is_none() {
                    return Err(FfiError::new(
                        ErrorCode::InvalidArgument,
                        "coordinator-frozen index timestamp is invalid",
                    ));
                }
            }
            IndexMetadata::try_from(metadata).map_err(|err| {
                FfiError::new(
                    ErrorCode::InvalidArgument,
                    format!("decode coordinator-frozen index metadata: {err}"),
                )
            })
        })
        .collect()
}

#[cfg(feature = "vane-distributed")]
fn retain_supported_frozen_indices(indices: &mut Vec<IndexMetadata>) {
    // Lance applies this compatibility filter inside Dataset::load_indices,
    // after reading the physical IndexSection. The helper is crate-private
    // upstream, so mirror the pinned Lance rule before seeding that native
    // cache with coordinator-frozen raw metadata.
    indices.retain(|index| {
        let max_supported_version = index
            .index_details
            .as_ref()
            .map(|details| {
                IndexDetails(details.clone())
                    .index_version()
                    .unwrap_or(i32::MAX as u32)
            })
            .unwrap_or_default();
        index.index_version <= max_supported_version as i32
    });
}

#[cfg(feature = "vane-distributed")]
pub(super) async fn load_supported_raw_index_metadata(
    dataset: &lance::Dataset,
) -> FfiResult<Vec<IndexMetadata>> {
    let object_store = dataset.object_store(None).await.map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetOpen,
            format!("resolve coordinator-frozen index object store: {err}"),
        )
    })?;
    let mut indices = read_manifest_indexes(
        object_store.as_ref(),
        dataset.manifest_location(),
        dataset.manifest(),
    )
    .await
    .map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetOpen,
            format!("read coordinator-frozen index section: {err}"),
        )
    })?;
    retain_supported_frozen_indices(&mut indices);
    Ok(indices)
}

#[cfg(feature = "vane-distributed")]
pub(super) async fn seed_frozen_index_metadata(
    dataset: &lance::Dataset,
    session: &SessionHandle,
    indices: &[IndexMetadata],
) -> FfiResult<VaneIndexCacheLease> {
    let store_identity = dataset
        .object_store(None)
        .await
        .map_err(|err| {
            FfiError::new(
                ErrorCode::DatasetOpen,
                format!("resolve frozen index metadata object store: {err}"),
            )
        })?
        .store_prefix
        .clone();
    let cache_prefix = format!("{}/", dataset.uri());
    let key = IndexMetadataKey {
        version: dataset.version_id(),
        store_identity: &store_identity,
    };

    let _guard = session.index_metadata_seed_lock.lock().await;
    if session
        .vane_index_cache
        .get_pinned_with_key(&cache_prefix, &key)
        .is_some_and(|current| current.as_ref() != indices)
    {
        return Err(FfiError::new(
            ErrorCode::DatasetOpen,
            "conflicting coordinator-frozen index metadata is already active for this snapshot",
        ));
    }
    let frozen_indices = Arc::new(indices.to_vec());
    Ok(session
        .vane_index_cache
        .pin_with_key(&cache_prefix, &key, frozen_indices))
}

#[cfg(feature = "vane-distributed")]
async fn vane_load_dataset_version(
    path: &str,
    version: u64,
    storage_options: Option<&HashMap<String, String>>,
    shared_session: Option<Arc<Session>>,
) -> lance::Result<lance::Dataset> {
    // A persistent worker Session can contain a manifest for an earlier dataset
    // that occupied the same URI and version. Resolve and load the current
    // manifest through a cache-free validation Session before accepting it.
    let mut validation_builder = vane_versioned_dataset_builder(path, version, storage_options);
    if let Some(shared_session) = shared_session.as_ref() {
        validation_builder = validation_builder.with_session(Arc::new(Session::new(
            0,
            0,
            shared_session.store_registry(),
        )));
    }
    let current = validation_builder.load().await?;

    let Some(shared_session) = shared_session else {
        return Ok(current);
    };

    // Rebuild from the independently loaded manifest so the returned Dataset
    // uses the worker's shared index/metadata caches without consulting a stale
    // manifest entry in that Session.
    let manifest = pb::Manifest::from(current.manifest()).encode_to_vec();
    vane_versioned_dataset_builder(path, version, storage_options)
        .with_serialized_manifest(&manifest)?
        .with_session(shared_session)
        .load()
        .await
}

#[cfg(feature = "vane-distributed")]
async fn vane_load_dataset_version_from_manifest(
    path: &str,
    version: u64,
    storage_options: Option<&HashMap<String, String>>,
    shared_session: Option<Arc<Session>>,
    serialized_manifest: &[u8],
    expected_generation: &str,
    frozen_indices: Option<&[IndexMetadata]>,
) -> FfiResult<lance::Dataset> {
    // This is Lance's native IPC fast path. It resolves the immutable manifest
    // location (and object metadata) but does not download manifest contents.
    // DatasetBuilder also validates and decodes the protobuf, so avoid decoding
    // the same potentially large payload once before handing it to Lance.
    let frozen = if frozen_indices.is_some() {
        let shared_session = shared_session.as_ref().ok_or_else(|| {
            FfiError::new(
                ErrorCode::InvalidArgument,
                "coordinator-frozen index metadata requires a shared Lance session",
            )
        })?;
        let object_store_builder = vane_versioned_dataset_builder(path, version, storage_options)
            .with_session(shared_session.clone());
        let (object_store, base_path, commit_handler) = object_store_builder
            .build_object_store()
            .await
            .map_err(|err| {
                FfiError::new(
                    ErrorCode::DatasetOpen,
                    format!("resolve coordinator-frozen dataset object store: {err}"),
                )
            })?;
        let mut generation_store = object_store.as_ref().clone();
        let generation_digest = Sha256::digest(expected_generation.as_bytes());
        generation_store.store_prefix = format!(
            "{}|vane-frozen:{generation_digest:x}",
            generation_store.store_prefix
        );
        let manifest = pb::Manifest::decode(serialized_manifest)
            .map_err(|err| {
                FfiError::new(
                    ErrorCode::InvalidArgument,
                    format!("decode coordinator-frozen dataset manifest: {err}"),
                )
            })?
            .try_into()
            .map_err(|err| {
                FfiError::new(
                    ErrorCode::InvalidArgument,
                    format!("load coordinator-frozen dataset manifest: {err}"),
                )
            })?;
        DatasetBuilder::load_by_uri(
            shared_session.clone(),
            Some(manifest),
            None,
            path.to_string(),
            Some(version),
            Arc::new(generation_store),
            base_path,
            commit_handler,
            Some(vane_object_store_params(storage_options)),
            None,
        )
        .await
        .map_err(|err| {
            FfiError::new(
                ErrorCode::DatasetOpen,
                format!("open coordinator-frozen dataset version {version} at '{path}': {err}"),
            )
        })?
    } else {
        let mut frozen_builder = vane_versioned_dataset_builder(path, version, storage_options)
            .with_serialized_manifest(serialized_manifest)
            .map_err(|err| {
                FfiError::new(
                    ErrorCode::InvalidArgument,
                    format!("load coordinator-frozen dataset manifest: {err}"),
                )
            })?;
        if let Some(shared_session) = shared_session.as_ref() {
            frozen_builder = frozen_builder.with_session(shared_session.clone());
        }
        frozen_builder.load().await.map_err(|err| {
            FfiError::new(
                ErrorCode::DatasetOpen,
                format!("open coordinator-frozen dataset version {version} at '{path}': {err}"),
            )
        })?
    };

    let frozen_generation = format!("snapshot|{}", dataset_snapshot_identity(&frozen).await?);
    if frozen_generation != expected_generation {
        return Err(FfiError::new(
            ErrorCode::DatasetOpen,
            "coordinator-frozen dataset snapshot generation changed; generation does not match current object metadata",
        ));
    }

    let has_reliable_object_identity = frozen
        .manifest_location()
        .e_tag
        .as_deref()
        .is_some_and(|etag| !etag.is_empty());
    if !has_reliable_object_identity {
        // Filesystems and object stores without an immutable ETag cannot prove
        // snapshot identity from metadata alone. Preserve fail-closed behavior
        // by loading the current manifest through a cache-free Session and
        // comparing the decoded protobuf value, not its map encoding order.
        let mut validation_builder = vane_versioned_dataset_builder(path, version, storage_options);
        if let Some(shared_session) = shared_session.as_ref() {
            validation_builder = validation_builder.with_session(Arc::new(Session::new(
                0,
                0,
                shared_session.store_registry(),
            )));
        }
        let current = validation_builder.load().await.map_err(|err| {
            FfiError::new(
                ErrorCode::DatasetOpen,
                format!("validate current dataset version {version} at '{path}': {err}"),
            )
        })?;
        let supplied_manifest = pb::Manifest::from(frozen.manifest());
        let current_manifest = pb::Manifest::from(current.manifest());
        if current_manifest != supplied_manifest {
            return Err(FfiError::new(
                ErrorCode::DatasetOpen,
                "dataset manifest changed on a backend without reliable immutable object identity",
            ));
        }
        let current_generation = format!("snapshot|{}", dataset_snapshot_identity(&current).await?);
        if current_generation != expected_generation {
            return Err(FfiError::new(
                ErrorCode::DatasetOpen,
                "validated dataset generation does not match the coordinator snapshot",
            ));
        }
        if let Some(frozen_indices) = frozen_indices {
            let current_indices = load_supported_raw_index_metadata(&current).await?;
            if current_indices != frozen_indices {
                return Err(FfiError::new(
                    ErrorCode::DatasetOpen,
                    "dataset index section changed on a backend without reliable immutable object identity",
                ));
            }
        }
    }
    Ok(frozen)
}

#[cfg(feature = "vane-distributed")]
fn vane_open_dataset_version_inner(
    path: *const c_char,
    version: u64,
    storage_options: Option<HashMap<String, String>>,
    session: *mut c_void,
) -> FfiResult<DatasetHandle> {
    if version == 0 {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "dataset version must be greater than zero",
        ));
    }
    // SAFETY: The FFI caller supplies a NUL-terminated path string.
    let path_str = unsafe { cstr_to_str(path, "path")? };
    // SAFETY: A non-null pointer is owned by this library and points to a SessionHandle.
    let session = unsafe { optional_session_handle(session)? };
    let dataset = match runtime::block_on(vane_load_dataset_version(
        path_str,
        version,
        storage_options.as_ref(),
        session,
    )) {
        Ok(Ok(dataset)) => Arc::new(dataset),
        Ok(Err(err)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetOpen,
                format!("dataset version {version} open '{path_str}': {err}"),
            ));
        }
        Err(err) => {
            return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}")));
        }
    };
    record_dataset_open();
    Ok(DatasetHandle::new(dataset))
}

#[cfg(feature = "vane-distributed")]
unsafe fn vane_open_dataset_version_from_manifest_inner(
    path: *const c_char,
    version: u64,
    payload: VaneFrozenDatasetPayload,
    expected_generation: *const c_char,
    storage_options: Option<HashMap<String, String>>,
    session: *mut c_void,
) -> FfiResult<DatasetHandle> {
    if version == 0 {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "dataset version must be greater than zero",
        ));
    }
    if payload.manifest_len == 0 || payload.manifest_len > MAX_SERIALIZED_MANIFEST_BYTES {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!(
                "serialized manifest length must be between 1 and {} bytes",
                MAX_SERIALIZED_MANIFEST_BYTES
            ),
        ));
    }
    // SAFETY: The FFI caller supplies a NUL-terminated path string.
    let path_str = unsafe { cstr_to_str(path, "path")? };
    // SAFETY: The FFI caller supplies a NUL-terminated generation string.
    let expected_generation = unsafe { cstr_to_str(expected_generation, "expected_generation")? };
    if !expected_generation.starts_with("snapshot|") {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "expected dataset generation is invalid",
        ));
    }
    // SAFETY: The caller guarantees `manifest` references `manifest_len` bytes.
    let manifest = unsafe {
        slice_from_ptr(
            payload.manifest,
            payload.manifest_len,
            "serialized_manifest",
        )?
    };
    let frozen_indices = match payload.index_section {
        Some((data, len)) => {
            if len > MAX_SERIALIZED_INDEX_SECTION_BYTES {
                return Err(FfiError::new(
                    ErrorCode::InvalidArgument,
                    format!(
                        "serialized index section length must not exceed {} bytes",
                        MAX_SERIALIZED_INDEX_SECTION_BYTES
                    ),
                ));
            }
            let bytes = if len == 0 {
                &[][..]
            } else {
                // SAFETY: The caller guarantees `data` references `len` bytes.
                unsafe { slice_from_ptr(data, len, "serialized_index_section")? }
            };
            Some(decode_frozen_index_section(bytes)?)
        }
        None => None,
    };
    // SAFETY: A non-null pointer is owned by this library and points to a SessionHandle.
    let session_handle = unsafe { optional_vane_session_handle(session)? };
    if frozen_indices.is_some() && session_handle.is_none() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "coordinator-frozen index metadata requires a shared Lance session",
        ));
    }
    // Keep Lance's database-wide metadata cache while isolating frozen index
    // keys through the generation-scoped object-store identity constructed by
    // `vane_load_dataset_version_from_manifest`.
    let dataset_session = session_handle.map(|handle| handle.session.clone());
    let dataset = match runtime::block_on(vane_load_dataset_version_from_manifest(
        path_str,
        version,
        storage_options.as_ref(),
        dataset_session,
        manifest,
        expected_generation,
        frozen_indices.as_deref(),
    )) {
        Ok(Ok(dataset)) => Arc::new(dataset),
        Ok(Err(err)) => return Err(err),
        Err(err) => {
            return Err(FfiError::new(
                ErrorCode::Runtime,
                format!("frozen dataset open runtime: {err}"),
            ));
        }
    };
    let frozen_index_metadata_lease = if let Some(frozen_indices) = frozen_indices.as_deref() {
        let Some(session_handle) = session_handle else {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "coordinator-frozen index metadata requires a shared Lance session",
            ));
        };
        match runtime::block_on(seed_frozen_index_metadata(
            dataset.as_ref(),
            session_handle,
            frozen_indices,
        )) {
            Ok(Ok(lease)) => Some(lease),
            Ok(Err(err)) => return Err(err),
            Err(err) => {
                return Err(FfiError::new(
                    ErrorCode::Runtime,
                    format!("frozen index metadata seed runtime: {err}"),
                ));
            }
        }
    } else {
        None
    };
    record_dataset_open();
    let handle = DatasetHandle::new(dataset);
    if let Some(lease) = frozen_index_metadata_lease {
        handle.retain_frozen_index_metadata(lease);
    }
    Ok(handle)
}

#[no_mangle]
pub unsafe extern "C" fn lance_close_dataset(dataset: *mut c_void) {
    if !dataset.is_null() {
        unsafe {
            let _ = Box::from_raw(dataset as *mut DatasetHandle);
        }
    }
}

#[cfg(feature = "vane-distributed")]
#[no_mangle]
pub unsafe extern "C" fn lance_dataset_version(dataset: *mut c_void) -> u64 {
    match dataset_version_inner(dataset) {
        Ok(version) => {
            clear_last_error();
            version
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            0
        }
    }
}

#[cfg(feature = "vane-distributed")]
fn dataset_version_inner(dataset: *mut c_void) -> FfiResult<u64> {
    // SAFETY: dataset_handle validates the opaque pointer before dereferencing it.
    let handle = unsafe { super::util::dataset_handle(dataset)? };
    Ok(handle.dataset.version_id())
}

#[cfg(feature = "vane-distributed")]
pub(super) async fn dataset_snapshot_identity(dataset: &lance::Dataset) -> FfiResult<String> {
    let manifest = dataset.manifest();
    let location = dataset.manifest_location();
    let mut size = location.size;
    let mut e_tag = location.e_tag.clone();

    // checkout_version may reconstruct the manifest location without its
    // object metadata. Fill only missing fields: metadata retained by an old
    // handle is what distinguishes it from a same-version replacement.
    if size.is_none() || e_tag.is_none() {
        let store = dataset.object_store(None).await.map_err(|err| {
            FfiError::new(
                ErrorCode::DatasetOpen,
                format!(
                    "resolve object store for dataset snapshot identity at '{}': {err}",
                    location.path
                ),
            )
        })?;
        let metadata = manifest_snapshot_metadata(store.inner.as_ref(), &location.path).await?;
        size.get_or_insert(metadata.size);
        if e_tag.is_none() {
            e_tag = metadata.e_tag;
        }
    }

    serde_json::to_string(&(
        manifest.version,
        manifest.timestamp_nanos,
        manifest.transaction_file.as_deref().unwrap_or_default(),
        size.unwrap_or_default(),
        e_tag.as_deref().unwrap_or_default(),
    ))
    .map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetOpen,
            format!("serialize dataset snapshot identity: {err}"),
        )
    })
}

#[cfg(feature = "vane-distributed")]
async fn manifest_snapshot_metadata(
    store: &dyn object_store::ObjectStore,
    path: &object_store::path::Path,
) -> FfiResult<object_store::ObjectMeta> {
    store.head(path).await.map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetOpen,
            format!("inspect manifest '{path}' for dataset snapshot identity: {err}"),
        )
    })
}

#[cfg(feature = "vane-distributed")]
#[no_mangle]
pub unsafe extern "C" fn lance_dataset_generation_id(dataset: *mut c_void) -> *const c_char {
    match dataset_generation_id_inner(dataset) {
        Ok(identity) => {
            clear_last_error();
            identity.into_raw()
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null()
        }
    }
}

#[cfg(feature = "vane-distributed")]
fn dataset_generation_id_inner(dataset: *mut c_void) -> FfiResult<std::ffi::CString> {
    // SAFETY: dataset_handle validates the opaque pointer before dereferencing it.
    let handle = unsafe { super::util::dataset_handle(dataset)? };
    let identity = match runtime::block_on(dataset_snapshot_identity(&handle.dataset)) {
        Ok(Ok(identity)) => identity,
        Ok(Err(err)) => return Err(err),
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };
    std::ffi::CString::new(format!("snapshot|{identity}")).map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetOpen,
            format!("dataset snapshot identity contains NUL: {err}"),
        )
    })
}

#[cfg(feature = "vane-distributed")]
#[no_mangle]
pub unsafe extern "C" fn lance_dataset_checkout_version(
    dataset: *mut c_void,
    version: u64,
) -> *mut c_void {
    match dataset_checkout_version_inner(dataset, version) {
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
fn dataset_checkout_version_inner(dataset: *mut c_void, version: u64) -> FfiResult<DatasetHandle> {
    // SAFETY: dataset_handle validates the opaque pointer before dereferencing it.
    let handle = unsafe { super::util::dataset_handle(dataset)? };
    if version == 0 {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "dataset version must be greater than zero",
        ));
    }
    let dataset = match runtime::block_on(handle.dataset.checkout_version(version)) {
        Ok(Ok(dataset)) => dataset,
        Ok(Err(err)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetOpen,
                format!("dataset checkout version {version}: {err}"),
            ))
        }
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };
    record_dataset_open();
    Ok(DatasetHandle::new(Arc::new(dataset)))
}

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_count_rows(dataset: *mut c_void) -> i64 {
    match dataset_count_rows_inner(dataset) {
        Ok(v) => {
            clear_last_error();
            v
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

fn dataset_count_rows_inner(dataset: *mut c_void) -> FfiResult<i64> {
    let handle = unsafe { super::util::dataset_handle(dataset)? };

    let rows = match runtime::block_on(handle.dataset.count_rows(None)) {
        Ok(Ok(rows)) => rows,
        Ok(Err(err)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetCountRows,
                format!("dataset count_rows: {err}"),
            ))
        }
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };

    i64::try_from(rows)
        .map_err(|_| FfiError::new(ErrorCode::DatasetCountRows, "row count overflow"))
}

#[no_mangle]
pub unsafe extern "C" fn lance_get_schema(dataset: *mut c_void) -> *mut c_void {
    match get_schema_inner(dataset) {
        Ok(schema) => {
            clear_last_error();
            Box::into_raw(Box::new(schema)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

fn get_schema_inner(dataset: *mut c_void) -> FfiResult<super::types::SchemaHandle> {
    let handle = unsafe { super::util::dataset_handle(dataset)? };
    Ok(handle.arrow_schema.clone())
}

#[no_mangle]
pub unsafe extern "C" fn lance_get_schema_for_scan(dataset: *mut c_void) -> *mut c_void {
    match get_schema_for_scan_inner(dataset) {
        Ok(schema) => {
            clear_last_error();
            Box::into_raw(Box::new(schema)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

fn get_schema_for_scan_inner(dataset: *mut c_void) -> FfiResult<super::types::SchemaHandle> {
    let handle = unsafe { super::util::dataset_handle(dataset)? };

    let mut schema: Schema = (*handle.arrow_schema).clone();
    let has_row_id = schema.fields.iter().any(|f| f.name() == ROW_ID_COLUMN);
    if !has_row_id {
        let mut fields = schema.fields.iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(ROW_ID_COLUMN, DataType::UInt64, false)));
        schema.fields = fields.into();
    }

    Ok(Arc::new(schema))
}

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_list_fragments(
    dataset: *mut c_void,
    out_len: *mut usize,
) -> *mut u64 {
    match dataset_list_fragments_inner(dataset, out_len) {
        Ok(ptr) => {
            clear_last_error();
            ptr
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

fn dataset_list_fragments_inner(dataset: *mut c_void, out_len: *mut usize) -> FfiResult<*mut u64> {
    if out_len.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "out_len is null"));
    }

    let handle = unsafe { super::util::dataset_handle(dataset)? };
    let ids: Vec<u64> = handle.dataset.fragments().iter().map(|f| f.id).collect();

    let mut boxed = ids.into_boxed_slice();
    let len = boxed.len();
    let data = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    unsafe {
        std::ptr::write_unaligned(out_len, len);
    }
    Ok(data)
}

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_list_fragment_stats(
    dataset: *mut c_void,
    out_len: *mut usize,
) -> *mut LanceFragmentStats {
    match dataset_list_fragment_stats_inner(dataset, out_len) {
        Ok(ptr) => {
            clear_last_error();
            ptr
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

fn dataset_list_fragment_stats_inner(
    dataset: *mut c_void,
    out_len: *mut usize,
) -> FfiResult<*mut LanceFragmentStats> {
    if out_len.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "out_len is null"));
    }
    let handle = unsafe { super::util::dataset_handle(dataset)? };

    let mut out: Vec<LanceFragmentStats> = Vec::with_capacity(handle.dataset.fragments().len());
    for frag in handle.dataset.fragments().iter() {
        let mut bytes_on_disk = 0u64;
        for file in frag.files.iter() {
            if let Some(sz) = file.file_size_bytes.get() {
                bytes_on_disk = bytes_on_disk.saturating_add(sz.get());
            }
        }
        let num_rows = match frag.num_rows() {
            Some(v) => i64::try_from(v).unwrap_or(-1),
            None => -1,
        };
        out.push(LanceFragmentStats {
            fragment_id: frag.id,
            num_rows,
            bytes_on_disk,
        });
    }

    let mut boxed = out.into_boxed_slice();
    let len = boxed.len();
    let data = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    unsafe {
        std::ptr::write_unaligned(out_len, len);
    }
    Ok(data)
}

#[cfg(feature = "vane-distributed")]
#[no_mangle]
pub unsafe extern "C" fn lance_dataset_list_distributed_fragment_stats(
    dataset: *mut c_void,
    out_len: *mut usize,
) -> *mut LanceFragmentStats {
    match dataset_list_distributed_fragment_stats_inner(dataset, out_len) {
        Ok(ptr) => {
            clear_last_error();
            ptr
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

#[cfg(feature = "vane-distributed")]
fn dataset_list_distributed_fragment_stats_inner(
    dataset: *mut c_void,
    out_len: *mut usize,
) -> FfiResult<*mut LanceFragmentStats> {
    if out_len.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "out_len is null"));
    }
    // SAFETY: the FFI caller supplies a live dataset handle returned by this
    // module; dataset_handle rejects a null pointer before dereferencing it.
    let handle = unsafe { super::util::dataset_handle(dataset)? };

    let mut out: Vec<LanceFragmentStats> = Vec::with_capacity(handle.dataset.fragments().len());
    for frag in handle.dataset.fragments().iter() {
        let mut bytes_on_disk = 0u64;
        let mut all_file_sizes_known = true;
        for file in frag.files.iter() {
            match file.file_size_bytes.get() {
                Some(size) => {
                    bytes_on_disk = bytes_on_disk.saturating_add(size.get());
                }
                None => {
                    all_file_sizes_known = false;
                }
            }
        }
        if !all_file_sizes_known {
            bytes_on_disk = 0;
        }
        let num_rows = match frag.num_rows() {
            Some(v) => i64::try_from(v).unwrap_or(-1),
            None => -1,
        };
        out.push(LanceFragmentStats {
            fragment_id: frag.id,
            num_rows,
            bytes_on_disk,
        });
    }

    let mut boxed = out.into_boxed_slice();
    let len = boxed.len();
    let data = boxed.as_mut_ptr();
    std::mem::forget(boxed);

    // SAFETY: out_len was checked non-null above and the FFI caller provides
    // writable storage for one usize value.
    unsafe {
        std::ptr::write_unaligned(out_len, len);
    }
    Ok(data)
}

#[no_mangle]
pub unsafe extern "C" fn lance_free_fragment_list(ptr: *mut u64, len: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let slice = std::ptr::slice_from_raw_parts_mut(ptr, len);
        let _ = Box::<[u64]>::from_raw(slice);
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_free_fragment_stats_list(ptr: *mut LanceFragmentStats, len: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let slice = std::ptr::slice_from_raw_parts_mut(ptr, len);
        let _ = Box::<[LanceFragmentStats]>::from_raw(slice);
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_list_field_stats(
    dataset: *mut c_void,
    out_len: *mut usize,
) -> *mut LanceFieldStats {
    match dataset_list_field_stats_inner(dataset, out_len) {
        Ok(ptr) => {
            clear_last_error();
            ptr
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

/// Transfer a boxed slice to C, writing its length to `out_len`.
unsafe fn boxed_slice_to_c<T>(items: Vec<T>, out_len: *mut usize) -> *mut T {
    let mut boxed = items.into_boxed_slice();
    let len = boxed.len();
    let data = boxed.as_mut_ptr();
    std::mem::forget(boxed);
    std::ptr::write_unaligned(out_len, len);
    data
}

macro_rules! fetch_data_stats {
    ($handle:expr) => {
        match runtime::block_on($handle.dataset.calculate_data_stats()) {
            Ok(Ok(stats)) => stats,
            Ok(Err(err)) => {
                return Err(FfiError::new(
                    ErrorCode::DatasetCalculateDataStats,
                    format!("dataset calculate_data_stats: {err}"),
                ))
            }
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        }
    };
}

fn dataset_list_field_stats_inner(
    dataset: *mut c_void,
    out_len: *mut usize,
) -> FfiResult<*mut LanceFieldStats> {
    if out_len.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "out_len is null"));
    }

    let handle = unsafe { super::util::dataset_handle(dataset)? };
    let stats = fetch_data_stats!(handle);

    let out: Vec<LanceFieldStats> = stats
        .fields
        .into_iter()
        .map(|field| LanceFieldStats {
            field_id: field.id,
            bytes_on_disk: field.bytes_on_disk,
        })
        .collect();

    Ok(unsafe { boxed_slice_to_c(out, out_len) })
}

#[no_mangle]
pub unsafe extern "C" fn lance_free_field_stats_list(ptr: *mut LanceFieldStats, len: usize) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let slice = std::ptr::slice_from_raw_parts_mut(ptr, len);
        let _ = Box::<[LanceFieldStats]>::from_raw(slice);
    }
}

#[repr(C)]
pub struct LanceNamedFieldStats {
    pub name: *const c_char,
    pub bytes_on_disk: u64,
}

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_list_named_field_stats(
    dataset: *mut c_void,
    out_len: *mut usize,
) -> *mut LanceNamedFieldStats {
    match dataset_list_named_field_stats_inner(dataset, out_len) {
        Ok(ptr) => {
            clear_last_error();
            ptr
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

fn dataset_list_named_field_stats_inner(
    dataset: *mut c_void,
    out_len: *mut usize,
) -> FfiResult<*mut LanceNamedFieldStats> {
    if out_len.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "out_len is null"));
    }

    let handle = unsafe { super::util::dataset_handle(dataset)? };
    let stats = fetch_data_stats!(handle);

    // Build field_id → name map from lance schema.
    let lance_schema = handle.dataset.schema();
    let mut id_to_name: HashMap<i32, String> = HashMap::new();
    fn collect_field_names(
        fields: &[lance_core::datatypes::Field],
        map: &mut HashMap<i32, String>,
    ) {
        for f in fields {
            map.insert(f.id, f.name.clone());
            collect_field_names(&f.children, map);
        }
    }
    collect_field_names(&lance_schema.fields, &mut id_to_name);

    let out: Vec<LanceNamedFieldStats> = stats
        .fields
        .into_iter()
        .filter_map(|field| {
            let name = id_to_name.get(&(field.id as i32))?;
            Some(LanceNamedFieldStats {
                name: super::util::to_c_string(name).into_raw(),
                bytes_on_disk: field.bytes_on_disk,
            })
        })
        .collect();

    Ok(unsafe { boxed_slice_to_c(out, out_len) })
}

#[no_mangle]
pub unsafe extern "C" fn lance_free_named_field_stats_list(
    ptr: *mut LanceNamedFieldStats,
    len: usize,
) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let slice = std::slice::from_raw_parts_mut(ptr, len);
        for item in slice.iter() {
            if !item.name.is_null() {
                let _ = std::ffi::CString::from_raw(item.name as *mut c_char);
            }
        }
        let boxed = Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len));
        drop(boxed);
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_delete(
    dataset: *mut c_void,
    filter_ir: *const u8,
    filter_ir_len: usize,
    out_deleted_rows: *mut i64,
) -> i32 {
    match dataset_delete_inner(dataset, filter_ir, filter_ir_len, out_deleted_rows) {
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

#[no_mangle]
pub unsafe extern "C" fn lance_delete_transaction_with_storage_options(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    filter_ir: *const u8,
    filter_ir_len: usize,
    session: *mut c_void,
    out_transaction: *mut *mut c_void,
    out_deleted_rows: *mut i64,
) -> i32 {
    match delete_transaction_with_storage_options_inner(
        path,
        option_keys,
        option_values,
        options_len,
        filter_ir,
        filter_ir_len,
        session,
        out_transaction,
        out_deleted_rows,
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

#[allow(clippy::too_many_arguments)]
fn delete_transaction_with_storage_options_inner(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    filter_ir: *const u8,
    filter_ir_len: usize,
    session: *mut c_void,
    out_transaction: *mut *mut c_void,
    out_deleted_rows: *mut i64,
) -> FfiResult<()> {
    if out_transaction.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "out_transaction is null",
        ));
    }
    if out_deleted_rows.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "out_deleted_rows is null",
        ));
    }

    let path_str = unsafe { cstr_to_str(path, "path")? };
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

    let filter = unsafe {
        parse_optional_filter_ir(
            filter_ir,
            filter_ir_len,
            ErrorCode::DatasetDelete,
            "delete filter_ir",
        )?
    };
    let predicate = match filter {
        Some(expr) => expr_to_sql(&expr)
            .map_err(|err| {
                FfiError::new(ErrorCode::DatasetDelete, format!("predicate sql: {err}"))
            })?
            .to_string(),
        None => "true".to_string(),
    };
    let session = unsafe { optional_session_handle(session)? };

    let (maybe_txn, deleted_rows) = match runtime::block_on(async {
        let mut builder = DatasetBuilder::from_uri(path_str).with_storage_options(storage_options);
        if let Some(session) = session.clone() {
            builder = builder.with_session(session);
        }
        let dataset = builder.load().await.map_err(|e| e.to_string())?;
        let dataset = Arc::new(dataset);

        let mut scanner = dataset.scan();
        scanner
            .with_row_id()
            .project(&[lance_core::ROW_ID])
            .map_err(|e| e.to_string())?
            .filter(&predicate)
            .map_err(|e| e.to_string())?;

        let Some(filter_expr) = scanner.get_expr_filter().map_err(|e| e.to_string())? else {
            return Ok::<_, String>((None, 0_i64));
        };

        if matches!(
            filter_expr,
            Expr::Literal(ScalarValue::Boolean(Some(false)), _)
        ) {
            return Ok::<_, String>((None, 0_i64));
        }

        let (updated_fragments, deleted_fragment_ids, deleted_rows) = if matches!(
            filter_expr,
            Expr::Literal(ScalarValue::Boolean(Some(true)), _)
        ) {
            let deleted_fragment_ids = dataset
                .get_fragments()
                .iter()
                .map(|f| f.id() as u64)
                .collect::<Vec<_>>();
            let deleted_rows = dataset.count_rows(None).await.map_err(|e| e.to_string())?;
            let deleted_rows = i64::try_from(deleted_rows)
                .map_err(|_| "deleted row count overflow".to_string())?;
            (Vec::new(), deleted_fragment_ids, deleted_rows)
        } else {
            let stable_row_ids = dataset.manifest.uses_stable_row_ids();
            let mut captured_row_ids = CapturedRowIds::new(stable_row_ids);

            let stream: SendableRecordBatchStream = scanner
                .try_into_stream()
                .await
                .map_err(|e| e.to_string())?
                .into();

            futures::pin_mut!(stream);
            while let Some(batch) = stream.try_next().await.map_err(|e| e.to_string())? {
                let row_ids = batch
                    .column_by_name(lance_core::ROW_ID)
                    .ok_or_else(|| "missing _rowid column".to_string())?
                    .as_primitive::<UInt64Type>()
                    .values();
                captured_row_ids
                    .capture(row_ids)
                    .map_err(|e| e.to_string())?;
            }

            let deleted_rows = captured_row_ids.len();
            let deleted_rows_i64 = i64::try_from(deleted_rows)
                .map_err(|_| "deleted row count overflow".to_string())?;

            let row_addrs = match &captured_row_ids {
                CapturedRowIds::AddressStyle(addrs) => addrs.clone(),
                CapturedRowIds::SequenceStyle(sequence) => {
                    let row_id_index = build_row_id_index(dataset.as_ref()).await?;
                    let mut addrs = RoaringTreemap::new();
                    for row_id in sequence.iter() {
                        let addr = row_id_index
                            .get(row_id)
                            .ok_or_else(|| format!("row id missing from row id index: {row_id}"))?;
                        addrs.insert(u64::from(addr));
                    }
                    addrs
                }
            };

            let (fragments, deleted_ids) = apply_deletions(dataset.as_ref(), &row_addrs).await?;
            (fragments, deleted_ids, deleted_rows_i64)
        };

        if updated_fragments.is_empty() && deleted_fragment_ids.is_empty() {
            return Ok::<_, String>((None, deleted_rows));
        }

        let operation = Operation::Delete {
            updated_fragments,
            deleted_fragment_ids,
            predicate,
        };
        let txn = Transaction::new(dataset.manifest.version, operation, None);
        Ok::<_, String>((Some(txn), deleted_rows))
    }) {
        Ok(Ok(v)) => v,
        Ok(Err(message)) => return Err(FfiError::new(ErrorCode::DatasetDelete, message)),
        Err(err) => {
            return Err(FfiError::new(
                ErrorCode::DatasetDelete,
                format!("runtime: {err}"),
            ))
        }
    };

    unsafe {
        std::ptr::write_unaligned(out_deleted_rows, deleted_rows);
        std::ptr::write_unaligned(out_transaction, std::ptr::null_mut());
    }

    if let Some(txn) = maybe_txn {
        let boxed = Box::new(txn);
        unsafe {
            std::ptr::write_unaligned(out_transaction, Box::into_raw(boxed) as *mut c_void);
        }
    }

    Ok(())
}

fn dataset_delete_inner(
    dataset: *mut c_void,
    filter_ir: *const u8,
    filter_ir_len: usize,
    out_deleted_rows: *mut i64,
) -> FfiResult<()> {
    if out_deleted_rows.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "out_deleted_rows is null",
        ));
    }

    let handle = unsafe { super::util::dataset_handle(dataset)? };

    let filter = unsafe {
        parse_optional_filter_ir(
            filter_ir,
            filter_ir_len,
            ErrorCode::DatasetDelete,
            "delete filter_ir",
        )?
    };
    let predicate = match filter {
        Some(expr) => expr_to_sql(&expr)
            .map_err(|err| {
                FfiError::new(ErrorCode::DatasetDelete, format!("predicate sql: {err}"))
            })?
            .to_string(),
        None => "true".to_string(),
    };

    let mut ds = (*handle.dataset).clone();

    let deleted_rows = match runtime::block_on(ds.delete(&predicate)) {
        Ok(Ok(result)) => result.num_deleted_rows,
        Ok(Err(err)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetDelete,
                format!("dataset delete: {err}"),
            ))
        }
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };
    let deleted_rows_i64 = i64::try_from(deleted_rows)
        .map_err(|_| FfiError::new(ErrorCode::DatasetDelete, "deleted row count overflow"))?;

    unsafe {
        std::ptr::write_unaligned(out_deleted_rows, deleted_rows_i64);
    }
    Ok(())
}

#[cfg(all(test, feature = "vane-distributed"))]
mod tests {
    use std::ffi::CString;
    use std::fs;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::WriteParams;
    use lance::index::DatasetIndexExt;
    use lance::session::Session;
    use lance::Dataset;
    use lance_core::cache::{CacheBackend, MokaCacheBackend};
    use lance_index::scalar::ScalarIndexParams;
    use lance_index::IndexType;
    use lance_io::object_store::providers::memory::MemoryStoreProvider;
    use lance_io::object_store::{
        ObjectStore as LanceObjectStore, ObjectStoreParams, ObjectStoreProvider,
        ObjectStoreRegistry, WrappingObjectStore,
    };
    use lance_io::utils::tracking_store::IOTracker;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use prost::Message;
    use url::Url;

    use super::*;
    use crate::runtime;

    #[derive(Debug)]
    struct TrackingMemoryStoreProvider {
        backend: Arc<InMemory>,
        tracker: IOTracker,
    }

    #[async_trait::async_trait]
    impl ObjectStoreProvider for TrackingMemoryStoreProvider {
        async fn new_store(
            &self,
            base_path: Url,
            params: &ObjectStoreParams,
        ) -> lance_core::Result<LanceObjectStore> {
            let mut store = MemoryStoreProvider.new_store(base_path, params).await?;
            store.inner = self.tracker.wrap("tracked-memory", self.backend.clone());
            Ok(store)
        }

        fn extract_path(&self, url: &Url) -> lance_core::Result<Path> {
            Ok(Path::from(url.path().trim_start_matches('/')))
        }

        fn calculate_object_store_prefix(
            &self,
            _url: &Url,
            _storage_options: Option<&std::collections::HashMap<String, String>>,
        ) -> lance_core::Result<String> {
            Ok("memory".to_string())
        }
    }

    fn tracked_memory_session(backend: Arc<InMemory>, tracker: IOTracker) -> Arc<Session> {
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert(
            "tracked-memory",
            Arc::new(TrackingMemoryStoreProvider { backend, tracker }),
        );
        Arc::new(Session::new(0, 0, registry))
    }

    fn tracked_memory_session_handle(backend: Arc<InMemory>, tracker: IOTracker) -> SessionHandle {
        let registry = Arc::new(ObjectStoreRegistry::default());
        registry.insert(
            "tracked-memory",
            Arc::new(TrackingMemoryStoreProvider { backend, tracker }),
        );
        // The frozen index catalog is deliberately larger than this cache.
        // Its snapshot-owned pin must keep the query valid without changing
        // the user's bounded-cache configuration.
        let bounded_index_cache: Arc<dyn CacheBackend> =
            Arc::new(MokaCacheBackend::with_capacity(1));
        let vane_index_cache = Arc::new(
            super::super::vane_index_cache::VaneIndexCacheBackend::new(bounded_index_cache),
        );
        let index_cache: Arc<dyn CacheBackend> = vane_index_cache.clone();
        SessionHandle {
            session: Arc::new(Session::with_index_cache_backend(
                index_cache.clone(),
                0,
                registry,
            )),
            index_cache,
            index_metadata_seed_lock: Arc::new(tokio::sync::Mutex::new(())),
            vane_index_cache,
        }
    }

    fn write_test_dataset(uri: &str, values: Vec<i32>) -> Dataset {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        runtime::block_on(Dataset::write(reader, uri, Some(WriteParams::default())))
            .unwrap()
            .unwrap()
    }

    #[test]
    fn snapshot_manifest_head_error_preserves_path_and_cause() {
        let path = Path::from("_versions/42.manifest");
        let store = InMemory::new();

        let error = runtime::block_on(manifest_snapshot_metadata(&store, &path))
            .unwrap()
            .unwrap_err();

        assert!(error.message.contains(path.as_ref()));
        assert!(error.message.to_ascii_lowercase().contains("not found"));
    }

    #[test]
    fn coordinator_frozen_manifest_round_trips_through_ffi() {
        let dataset_dir =
            std::env::temp_dir().join(format!("ffi-frozen-manifest-{}", rand::random::<u64>()));
        let uri = dataset_dir.to_string_lossy().to_string();
        let dataset = write_test_dataset(&uri, vec![1, 2, 3]);
        let version = dataset.version_id();
        let generation = format!(
            "snapshot|{}",
            runtime::block_on(dataset_snapshot_identity(&dataset))
                .unwrap()
                .unwrap()
        );

        let dataset_handle =
            Box::into_raw(Box::new(DatasetHandle::new(Arc::new(dataset)))) as *mut c_void;
        let mut manifest_ptr = ptr::null_mut();
        let mut manifest_len = 0;
        assert_eq!(
            unsafe {
                lance_vane_serialize_dataset_manifest(
                    dataset_handle,
                    &mut manifest_ptr,
                    &mut manifest_len,
                )
            },
            0
        );
        assert!(!manifest_ptr.is_null());
        assert!(manifest_len > 0);
        let manifest = unsafe { std::slice::from_raw_parts(manifest_ptr, manifest_len) }.to_vec();
        unsafe {
            super::super::vane_distributed_search::lance_vane_free_bytes(
                manifest_ptr,
                manifest_len,
            );
            lance_close_dataset(dataset_handle);
        }

        let session = Arc::new(Session::default());
        let opened = runtime::block_on(vane_load_dataset_version_from_manifest(
            &uri,
            version,
            None,
            Some(session.clone()),
            &manifest,
            &generation,
            None,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(opened.version_id(), version);
        assert_eq!(
            pb::Manifest::from(opened.manifest()).encode_to_vec(),
            manifest
        );
        assert!(Arc::ptr_eq(&opened.session(), &session));

        let _ = fs::remove_dir_all(dataset_dir);
    }

    #[test]
    fn coordinator_frozen_manifest_fails_closed_without_reliable_etag() {
        let dataset_dir = std::env::temp_dir().join(format!(
            "ffi-frozen-manifest-replacement-{}",
            rand::random::<u64>()
        ));
        let uri = dataset_dir.to_string_lossy().to_string();
        let first = write_test_dataset(&uri, vec![1, 2, 3]);
        let version = first.version_id();
        let manifest = pb::Manifest::from(first.manifest()).encode_to_vec();
        let generation = format!(
            "snapshot|{}",
            runtime::block_on(dataset_snapshot_identity(&first))
                .unwrap()
                .unwrap()
        );
        drop(first);

        fs::remove_dir_all(&dataset_dir).unwrap();
        let replacement = write_test_dataset(&uri, vec![4, 5, 6]);
        assert_eq!(replacement.version_id(), version);
        drop(replacement);

        let error = runtime::block_on(vane_load_dataset_version_from_manifest(
            &uri,
            version,
            None,
            Some(Arc::new(Session::default())),
            &manifest,
            &generation,
            None,
        ))
        .unwrap()
        .unwrap_err();
        assert!(
            error.message.contains("generation") || error.message.contains("manifest changed"),
            "unexpected error: {}",
            error.message
        );

        let _ = fs::remove_dir_all(dataset_dir);
    }

    #[test]
    fn coordinator_frozen_manifest_uses_only_metadata_io_with_reliable_etag() {
        let backend = Arc::new(InMemory::new());
        let tracker = IOTracker::default();
        let coordinator_session = tracked_memory_session(backend.clone(), tracker.clone());
        let worker_session = tracked_memory_session(backend, tracker.clone());
        let uri = format!(
            "tracked-memory://snapshot-{}/dataset.lance",
            rand::random::<u64>()
        );

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        let write_params = WriteParams {
            session: Some(coordinator_session),
            ..WriteParams::default()
        };
        let dataset = runtime::block_on(Dataset::write(reader, &uri, Some(write_params)))
            .unwrap()
            .unwrap();
        let version = dataset.version_id();
        let manifest = pb::Manifest::from(dataset.manifest()).encode_to_vec();
        let generation = format!(
            "snapshot|{}",
            runtime::block_on(dataset_snapshot_identity(&dataset))
                .unwrap()
                .unwrap()
        );
        assert!(dataset.manifest_location().e_tag.is_some());
        let _ = tracker.incremental_stats();

        let opened = runtime::block_on(vane_load_dataset_version_from_manifest(
            &uri,
            version,
            None,
            Some(worker_session),
            &manifest,
            &generation,
            None,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(opened.version_id(), version);
        assert!(opened.manifest_location().e_tag.is_some());

        let worker_io = tracker.incremental_stats();
        // IOTracker observes ObjectStore::head through the trait's get_opts
        // default and attributes the object's metadata size to read_bytes. The
        // request count is authoritative here: one request is the required
        // version-location HEAD; a manifest content GET or fallback validation
        // would add another request.
        assert_eq!(
            worker_io.read_iops, 1,
            "worker performed manifest content I/O or repeated metadata checks: {worker_io}"
        );
    }

    #[test]
    fn frozen_index_section_uses_generation_scoped_pin_above_cache_capacity() {
        let _counter_guard = super::super::session::DEBUG_COUNTER_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let backend = Arc::new(InMemory::new());
        let tracker = IOTracker::default();
        let coordinator_session = tracked_memory_session_handle(backend.clone(), tracker.clone());
        let mut worker_session = tracked_memory_session_handle(backend, tracker.clone());
        let uri = format!(
            "tracked-memory://search-snapshot-{}/dataset.lance",
            rand::random::<u64>()
        );

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        let write_params = WriteParams {
            session: Some(coordinator_session.session.clone()),
            ..WriteParams::default()
        };
        let mut dataset = runtime::block_on(Dataset::write(reader, &uri, Some(write_params)))
            .unwrap()
            .unwrap();
        runtime::block_on(dataset.create_index(
            &["id"],
            IndexType::Scalar,
            Some("id_idx".to_string()),
            &ScalarIndexParams::default(),
            true,
        ))
        .unwrap()
        .unwrap();
        let dataset = Arc::new(dataset);

        let version = dataset.version_id();
        let manifest = pb::Manifest::from(dataset.manifest()).encode_to_vec();
        let generation = format!(
            "snapshot|{}",
            runtime::block_on(dataset_snapshot_identity(dataset.as_ref()))
                .unwrap()
                .unwrap()
        );
        assert!(dataset.manifest_location().e_tag.is_some());

        let dataset_handle =
            Box::into_raw(Box::new(DatasetHandle::new(dataset.clone()))) as *mut c_void;
        let mut index_section_ptr = ptr::null_mut();
        let mut index_section_len = 0;
        assert_eq!(
            unsafe {
                super::super::vane_distributed_search::lance_vane_serialize_dataset_index_section(
                    dataset_handle,
                    &mut index_section_ptr,
                    &mut index_section_len,
                )
            },
            0
        );
        assert!(!index_section_ptr.is_null());
        assert!(index_section_len > 0);
        assert_eq!(coordinator_session.vane_index_cache.pinned_entry_count(), 0);
        let index_section =
            unsafe { std::slice::from_raw_parts(index_section_ptr, index_section_len) }.to_vec();
        let expected_frozen_indices = decode_frozen_index_section(&index_section).unwrap();
        assert_eq!(expected_frozen_indices.len(), 1);
        unsafe {
            super::super::vane_distributed_search::lance_vane_free_bytes(
                index_section_ptr,
                index_section_len,
            );
            lance_close_dataset(dataset_handle);
        }
        let _ = tracker.incremental_stats();

        let path = CString::new(uri).unwrap();
        let expected_generation = CString::new(generation).unwrap();
        let session_ptr = (&mut worker_session as *mut SessionHandle).cast::<c_void>();
        let opened = unsafe {
            vane_open_dataset_version_from_manifest_inner(
                path.as_ptr(),
                version,
                VaneFrozenDatasetPayload {
                    manifest: manifest.as_ptr(),
                    manifest_len: manifest.len(),
                    index_section: Some((index_section.as_ptr(), index_section.len())),
                },
                expected_generation.as_ptr(),
                None,
                session_ptr,
            )
        }
        .unwrap();
        let actual_indices = runtime::block_on(opened.dataset.load_indices())
            .unwrap()
            .unwrap();
        assert_eq!(actual_indices.as_ref(), &expected_frozen_indices);
        assert!(Arc::ptr_eq(
            &opened.dataset.session(),
            &worker_session.session
        ));

        let worker_io = tracker.incremental_stats();
        assert_eq!(
            worker_io.read_iops, 1,
            "worker reopened the manifest IndexSection instead of using frozen metadata: {worker_io}"
        );
        assert_eq!(worker_session.vane_index_cache.pinned_entry_count(), 1);
        drop(opened);
        assert_eq!(worker_session.vane_index_cache.pinned_entry_count(), 0);
    }

    #[test]
    fn frozen_index_pin_isolated_from_replacement_at_same_uri_and_version() {
        let _counter_guard = super::super::session::DEBUG_COUNTER_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let dataset_dir = std::env::temp_dir().join(format!(
            "ffi-frozen-index-generation-isolation-{}",
            rand::random::<u64>()
        ));
        let uri = dataset_dir.to_string_lossy().to_string();
        let mut first = write_test_dataset(&uri, vec![1, 2, 3]);
        runtime::block_on(first.create_index(
            &["id"],
            IndexType::Scalar,
            Some("first_idx".to_string()),
            &ScalarIndexParams::default(),
            true,
        ))
        .unwrap()
        .unwrap();
        let version = first.version_id();
        let manifest = pb::Manifest::from(first.manifest()).encode_to_vec();
        let generation = format!(
            "snapshot|{}",
            runtime::block_on(dataset_snapshot_identity(&first))
                .unwrap()
                .unwrap()
        );
        let first_indices = runtime::block_on(load_supported_raw_index_metadata(&first))
            .unwrap()
            .unwrap();
        let index_section = pb::IndexSection {
            indices: first_indices.iter().map(pb::IndexMetadata::from).collect(),
        }
        .encode_to_vec();
        drop(first);

        let mut shared_session =
            tracked_memory_session_handle(Arc::new(InMemory::new()), IOTracker::default());
        let session_ptr = (&mut shared_session as *mut SessionHandle).cast::<c_void>();
        let path = CString::new(uri.clone()).unwrap();
        let expected_generation = CString::new(generation).unwrap();
        let frozen = unsafe {
            vane_open_dataset_version_from_manifest_inner(
                path.as_ptr(),
                version,
                VaneFrozenDatasetPayload {
                    manifest: manifest.as_ptr(),
                    manifest_len: manifest.len(),
                    index_section: Some((index_section.as_ptr(), index_section.len())),
                },
                expected_generation.as_ptr(),
                None,
                session_ptr,
            )
        }
        .unwrap();
        assert!(Arc::ptr_eq(
            &frozen.dataset.session(),
            &shared_session.session
        ));
        assert_eq!(shared_session.vane_index_cache.pinned_entry_count(), 1);

        fs::remove_dir_all(&dataset_dir).unwrap();
        let mut replacement = write_test_dataset(&uri, vec![4, 5, 6]);
        runtime::block_on(replacement.create_index(
            &["id"],
            IndexType::Scalar,
            Some("replacement_idx".to_string()),
            &ScalarIndexParams::default(),
            true,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(replacement.version_id(), version);
        drop(replacement);

        let current =
            vane_open_dataset_version_inner(path.as_ptr(), version, None, session_ptr).unwrap();
        assert!(Arc::ptr_eq(
            &current.dataset.session(),
            &shared_session.session
        ));
        let frozen_store_identity = runtime::block_on(frozen.dataset.object_store(None))
            .unwrap()
            .unwrap()
            .store_prefix
            .clone();
        let current_store_identity = runtime::block_on(current.dataset.object_store(None))
            .unwrap()
            .unwrap()
            .store_prefix
            .clone();
        assert_ne!(frozen_store_identity, current_store_identity);
        let current_indices = runtime::block_on(current.dataset.load_indices())
            .unwrap()
            .unwrap();
        assert!(current_indices
            .iter()
            .any(|index| index.name == "replacement_idx"));
        assert!(current_indices
            .iter()
            .all(|index| index.name != "first_idx"));

        let still_frozen = runtime::block_on(frozen.dataset.load_indices())
            .unwrap()
            .unwrap();
        assert!(still_frozen.iter().any(|index| index.name == "first_idx"));
        assert!(still_frozen
            .iter()
            .all(|index| index.name != "replacement_idx"));
        assert_eq!(shared_session.vane_index_cache.pinned_entry_count(), 1);

        drop(current);
        drop(frozen);
        assert_eq!(shared_session.vane_index_cache.pinned_entry_count(), 0);
        let _ = fs::remove_dir_all(dataset_dir);
    }

    #[test]
    fn frozen_index_section_rejects_malformed_and_oversized_payloads() {
        let malformed = decode_frozen_index_section(&[0xff]).unwrap_err();
        assert!(malformed
            .message
            .contains("decode coordinator-frozen index section"));

        let path = CString::new("unused.lance").unwrap();
        let generation = CString::new("snapshot|unused").unwrap();
        let manifest = [0_u8];
        let oversized = std::ptr::NonNull::<u8>::dangling().as_ptr();
        let result = unsafe {
            vane_open_dataset_version_from_manifest_inner(
                path.as_ptr(),
                1,
                VaneFrozenDatasetPayload {
                    manifest: manifest.as_ptr(),
                    manifest_len: manifest.len(),
                    index_section: Some((oversized, MAX_SERIALIZED_INDEX_SECTION_BYTES + 1)),
                },
                generation.as_ptr(),
                None,
                ptr::null_mut(),
            )
        };
        let error = match result {
            Ok(_) => panic!("oversized frozen index section was accepted"),
            Err(error) => error,
        };
        assert!(error.message.contains("serialized index section length"));
    }

    #[test]
    fn frozen_manifest_ffi_rejects_oversized_payload_before_reading_it() {
        let path = CString::new("unused.lance").unwrap();
        let generation = CString::new("snapshot|unused").unwrap();
        let manifest = std::ptr::NonNull::<u8>::dangling().as_ptr();
        let result = unsafe {
            vane_open_dataset_version_from_manifest_inner(
                path.as_ptr(),
                1,
                VaneFrozenDatasetPayload {
                    manifest,
                    manifest_len: MAX_SERIALIZED_MANIFEST_BYTES + 1,
                    index_section: None,
                },
                generation.as_ptr(),
                None,
                ptr::null_mut(),
            )
        };
        let error = match result {
            Ok(_) => panic!("oversized frozen manifest was accepted"),
            Err(error) => error,
        };
        assert!(error.message.contains("serialized manifest length"));
    }
}
