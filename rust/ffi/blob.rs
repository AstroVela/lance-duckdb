//! Snapshot-bound Blob v2 byte ranges. Cursor and read-ahead belong to the
//! eventual DuckDB FileHandle; this internal primitive reads exact ranges only.

use std::ffi::{c_char, c_void, CString};
use std::ops::Range;
use std::ptr;
use std::sync::Arc;

use lance::dataset::builder::DatasetBuilder;
use lance::dataset::rowids::get_row_id_index;
use lance::dataset::BlobFile;
use lance::Dataset;
use lance_core::utils::address::RowAddress;
use lance_io::object_store::{ObjectStore, ObjectStoreParams};
use object_store::ObjectStoreExt;
use sha2::{Digest, Sha256};

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;

use super::blob_store::{
    blob_open_error, clone_access, io_error, normalize_uri, session, BlobAccessContext,
};
use super::dataset::{exact_version_builder, format_snapshot_identity};
use super::session::record_dataset_open;
use super::types::DatasetHandle;
use super::util::{cstr_to_str, dataset_handle, FfiError, FfiResult};

#[repr(C)]
pub struct LanceBlobOpenOptions {
    pub dataset_uri: *const c_char,
    pub version: u64,
    pub snapshot_token: *const c_char,
    pub column_path: *const c_char,
    pub row_id: u64,
    pub access_context: *mut c_void,
}

struct BlobHandle {
    file: BlobFile,
    // The file/store/scheduler also hold access Arcs. Keep the captured Dataset
    // and callback owner alive explicitly, independently of the binding handle.
    _dataset: Arc<Dataset>,
    _access: Arc<BlobAccessContext>,
    size: u64,
}

fn run<F: std::future::Future<Output = FfiResult<T>>, T>(future: F) -> FfiResult<T> {
    runtime::block_on(future)
        .map_err(|_| FfiError::new(ErrorCode::Runtime, "Blob runtime unavailable"))?
}

fn token(identity: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!("snapshot|{identity}").as_bytes())
    )
}

async fn snapshot_token(
    dataset: &Dataset,
    access: Arc<BlobAccessContext>,
    refresh: bool,
) -> FfiResult<String> {
    let location = dataset.manifest_location();
    let mut size = location.size;
    let mut e_tag = location.e_tag.clone();
    if refresh || size.is_none() || e_tag.is_none() {
        // Never use a captured Dataset's unmediated store to fill missing
        // metadata. Its manifest fields and retained metadata remain pinned.
        let uri = normalize_uri(dataset.uri())?;
        let (store, _) = ObjectStore::from_uri_and_params(
            session(access).store_registry(),
            uri.as_str(),
            &ObjectStoreParams::default(),
        )
        .await
        .map_err(|error| io_error(ErrorCode::BlobSnapshot, &error, "resolve snapshot store"))?;
        let metadata = store.inner.head(&location.path).await.map_err(|error| {
            io_error(ErrorCode::BlobSnapshot, &error.into(), "inspect snapshot")
        })?;
        if refresh {
            size = Some(metadata.size);
            e_tag = metadata.e_tag;
        } else {
            size.get_or_insert(metadata.size);
            if e_tag.is_none() {
                e_tag = metadata.e_tag;
            }
        }
    }
    Ok(token(&format_snapshot_identity(
        dataset,
        size.unwrap_or_default(),
        e_tag.as_deref(),
    )?))
}

#[no_mangle]
pub unsafe extern "C" fn lance_blob_open_binding_dataset(
    dataset_uri: *const c_char,
    access: *mut c_void,
) -> *mut c_void {
    match open_binding_inner(dataset_uri, access) {
        Ok(dataset) => {
            clear_last_error();
            Box::into_raw(Box::new(dataset)).cast()
        }
        Err(error) => {
            set_last_error(error.code, error.message);
            ptr::null_mut()
        }
    }
}

fn open_binding_inner(dataset_uri: *const c_char, access: *mut c_void) -> FfiResult<DatasetHandle> {
    // SAFETY: Caller supplies live C strings and a live owned access handle.
    let (uri, access) = unsafe {
        (
            cstr_to_str(dataset_uri, "dataset_uri")?,
            clone_access(access)?,
        )
    };
    let uri = normalize_uri(uri)?;
    let dataset = run(async {
        DatasetBuilder::from_uri(uri.as_str())
            .with_session(session(access))
            .load()
            .await
            .map_err(|error| io_error(ErrorCode::BlobOpen, &error, "open binding dataset"))
    })?;
    record_dataset_open();
    Ok(DatasetHandle::new(Arc::new(dataset)))
}

#[no_mangle]
pub unsafe extern "C" fn lance_blob_snapshot_token(
    dataset: *mut c_void,
    access: *mut c_void,
) -> *const c_char {
    match snapshot_token_inner(dataset, access) {
        Ok(token) => {
            clear_last_error();
            token.into_raw()
        }
        Err(error) => {
            set_last_error(error.code, error.message);
            ptr::null()
        }
    }
}

fn snapshot_token_inner(dataset: *mut c_void, access: *mut c_void) -> FfiResult<CString> {
    // SAFETY: Caller supplies live handles; nulls are rejected by these helpers.
    let (dataset, access) = unsafe { (dataset_handle(dataset)?, clone_access(access)?) };
    let token = run(snapshot_token(&dataset.dataset, access, false))?;
    CString::new(token)
        .map_err(|_| FfiError::new(ErrorCode::BlobSnapshot, "invalid snapshot token"))
}

/// Normalize with the same provider rules used at open. `local_path` requests
/// the OS path for native DuckDB policy matching, without filesystem I/O.
#[no_mangle]
pub unsafe extern "C" fn lance_blob_normalize_uri(
    uri: *const c_char,
    local_path: bool,
) -> *const c_char {
    let result = (|| {
        // SAFETY: Caller supplies a live NUL-terminated string.
        let uri = normalize_uri(unsafe { cstr_to_str(uri, "uri")? })?;
        let normalized = if local_path && uri.scheme() == "file" {
            uri.to_file_path()
                .ok()
                .and_then(|path| path.into_os_string().into_string().ok())
                .ok_or_else(|| FfiError::new(ErrorCode::BlobAccess, "invalid local Blob target"))?
        } else {
            uri.to_string()
        };
        CString::new(normalized)
            .map_err(|_| FfiError::new(ErrorCode::BlobAccess, "invalid Blob target"))
    })();
    match result {
        Ok(value) => {
            clear_last_error();
            value.into_raw()
        }
        Err(error) => {
            set_last_error(error.code, error.message);
            ptr::null()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_blob_validate_column(
    dataset: *mut c_void,
    column: *const c_char,
) -> i32 {
    let result = (|| {
        // SAFETY: Caller supplies a live dataset and NUL-terminated column.
        let (dataset, column) =
            unsafe { (dataset_handle(dataset)?, cstr_to_str(column, "column")?) };
        validate_column(&dataset.dataset, column)
    })();
    match result {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(error) => {
            set_last_error(error.code, error.message);
            -1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_blob_open(
    options: *const LanceBlobOpenOptions,
    out_blob: *mut *mut c_void,
    out_size: *mut u64,
) -> i32 {
    match open_inner(options, out_blob, out_size) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(error) => {
            set_last_error(error.code, error.message);
            -1
        }
    }
}

fn open_inner(
    options: *const LanceBlobOpenOptions,
    out_blob: *mut *mut c_void,
    out_size: *mut u64,
) -> FfiResult<()> {
    // Initialize every valid output even when its companion pointer is missing.
    // SAFETY: Non-null outputs are valid writable, non-overlapping pointers.
    unsafe {
        if !out_blob.is_null() {
            *out_blob = ptr::null_mut();
        }
        if !out_size.is_null() {
            *out_size = 0;
        }
    }
    if out_blob.is_null() || out_size.is_null() || options.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "Blob options and outputs are required",
        ));
    }
    // SAFETY: The caller supplies a live options structure and live C strings.
    let (options, uri, expected_token, column, access) = unsafe {
        let options = &*options;
        (
            options,
            cstr_to_str(options.dataset_uri, "dataset_uri")?,
            cstr_to_str(options.snapshot_token, "snapshot_token")?,
            cstr_to_str(options.column_path, "column_path")?,
            clone_access(options.access_context)?,
        )
    };
    if options.version == 0
        || expected_token.len() != 64
        || !expected_token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || column.is_empty()
    {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "invalid Blob version, token or column",
        ));
    }
    let uri = normalize_uri(uri)?;
    let blob = run(async {
        let dataset = exact_version_builder(uri.as_str(), options.version)
            .with_session(session(access.clone()))
            .load()
            .await
            .map_err(|error| {
                io_error(ErrorCode::BlobSnapshot, &error, "open exact Blob snapshot")
            })?;
        record_dataset_open();
        if snapshot_token(&dataset, access.clone(), true).await? != expected_token {
            return Err(FfiError::new(
                ErrorCode::BlobSnapshot,
                "Blob snapshot identity mismatch",
            ));
        }
        validate_column(&dataset, column)?;
        let row_address = resolve_row_address(&dataset, options.row_id).await?;
        let dataset = Arc::new(dataset);
        let mut files = dataset
            .take_blobs_by_addresses(&[row_address.into()], column)
            .await
            .map_err(|error| blob_open_error(&error))?;
        if files.is_empty() {
            return Err(missing_row());
        }
        if files.len() != 1 {
            return Err(FfiError::new(
                ErrorCode::BlobOpen,
                "Blob row did not resolve to one value",
            ));
        }
        let file = files.remove(0);
        let size = file.size();
        // Lance opens packed/dedicated payload sources lazily. Initialize the
        // source with an empty range while the opener's transaction is live,
        // so its target-specific credentials are owned by this handle before
        // returning to DuckDB. This probes metadata without fetching payload.
        file.read_range(0..0)
            .await
            .map_err(|error| blob_open_error(&error))?;
        Ok(BlobHandle {
            file,
            _dataset: dataset,
            _access: access,
            size,
        })
    })?;
    // SAFETY: Publish only a fully initialized owner, after all fallible work.
    unsafe {
        *out_size = blob.size;
        *out_blob = Box::into_raw(Box::new(blob)).cast();
    }
    Ok(())
}

fn missing_row() -> FfiError {
    FfiError::new(
        ErrorCode::BlobNotFound,
        "Blob row is missing, deleted or null",
    )
}

async fn resolve_row_address(dataset: &Dataset, row_id: u64) -> FfiResult<RowAddress> {
    // Lance's Blob take path can report invalid input for absent physical row
    // addresses. Establish absence using its row metadata instead of reclassifying
    // generic take errors, which could also indicate corrupt or inaccessible data.
    let address = match get_row_id_index(dataset)
        .await
        .map_err(|error| io_error(ErrorCode::BlobOpen, &error, "resolve Blob row ID"))?
    {
        Some(index) => index.get(row_id).ok_or_else(missing_row)?,
        None => RowAddress::from(row_id),
    };
    let fragment = dataset
        .get_fragment(address.fragment_id() as usize)
        .ok_or_else(missing_row)?;
    let physical_rows = fragment
        .physical_rows()
        .await
        .map_err(|error| io_error(ErrorCode::BlobOpen, &error, "inspect Blob fragment"))?;
    if address.row_offset() as usize >= physical_rows {
        return Err(missing_row());
    }
    let deletions = fragment
        .get_deletion_vector()
        .await
        .map_err(|error| io_error(ErrorCode::BlobOpen, &error, "inspect Blob deletions"))?;
    if deletions.is_some_and(|deletions| deletions.contains(address.row_offset())) {
        return Err(missing_row());
    }
    Ok(address)
}

fn validate_column(dataset: &Dataset, column: &str) -> FfiResult<()> {
    let fields = dataset
        .schema()
        .resolve(column)
        .filter(|fields| !fields.is_empty())
        .ok_or_else(|| FfiError::new(ErrorCode::BlobOpen, "Blob field path does not exist"))?;
    if !fields.last().is_some_and(|field| field.is_blob_v2())
        || fields[..fields.len() - 1]
            .iter()
            .any(|field| !matches!(field.data_type(), arrow_schema::DataType::Struct(_)))
    {
        return Err(FfiError::new(
            ErrorCode::BlobOpen,
            "expected a Blob v2 field reached through structs",
        ));
    }
    Ok(())
}

fn checked_range(size: u64, offset: u64, length: usize) -> FfiResult<Range<u64>> {
    let length = u64::try_from(length)
        .map_err(|_| FfiError::new(ErrorCode::BlobRange, "Blob read length overflow"))?;
    if offset > size || length > size - offset || length > isize::MAX as u64 {
        return Err(FfiError::new(
            ErrorCode::BlobRange,
            "Blob range exceeds its size or addressable buffer",
        ));
    }
    Ok(offset..offset + length)
}

#[no_mangle]
pub unsafe extern "C" fn lance_blob_read_at(
    blob: *mut c_void,
    offset: u64,
    out_buffer: *mut u8,
    length: usize,
    out_read: *mut usize,
) -> i32 {
    match read_at_inner(blob, offset, out_buffer, length, out_read) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(error) => {
            set_last_error(error.code, error.message);
            -1
        }
    }
}

fn read_at_inner(
    blob: *mut c_void,
    offset: u64,
    out_buffer: *mut u8,
    length: usize,
    out_read: *mut usize,
) -> FfiResult<()> {
    if out_read.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "out_read is null",
        ));
    }
    // SAFETY: The caller supplies an aligned writable output pointer.
    unsafe { *out_read = 0 };
    if blob.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "Blob handle is null",
        ));
    }
    // SAFETY: The caller keeps this Blob handle live until this call completes.
    let blob = unsafe { &*blob.cast::<BlobHandle>() };
    let range = checked_range(blob.size, offset, length)?;
    if length == 0 {
        return Ok(());
    }
    if out_buffer.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "Blob output buffer is null",
        ));
    }
    let bytes = run(async {
        blob.file
            .read_range(range)
            .await
            .map_err(|error| io_error(ErrorCode::BlobRead, &error, "read Blob range"))
    })?;
    if bytes.len() != length {
        return Err(FfiError::new(
            ErrorCode::BlobRead,
            "short backing read for exact Blob range",
        ));
    }
    // SAFETY: The caller provides length writable bytes, disjoint from Rust's
    // owned response. No bytes or success count are published on failure.
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), out_buffer, length);
        *out_read = length;
    }
    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn lance_blob_free(blob: *mut c_void) {
    if !blob.is_null() {
        // SAFETY: The caller consumes this handle once, with no concurrent reads.
        let blob = unsafe { Box::from_raw(blob.cast::<BlobHandle>()) };
        let _ = runtime::block_on(blob.file.close());
        // Upstream scheduled work retains its store and access Arc until done.
        // Dropping this owner never invalidates a pending callback lease.
    }
    // Do not clear or overwrite a preceding read/open error during destruction.
}

#[cfg(test)]
#[path = "blob_tests.rs"]
mod tests;
