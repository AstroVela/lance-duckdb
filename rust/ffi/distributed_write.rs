#![cfg(feature = "vane-distributed")]

use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, c_void, CString};
use std::ptr;
use std::sync::Arc;

use lance::dataset::builder::DatasetBuilder;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, Dataset};
use lance::io::{ObjectStoreParams, StorageOptionsAccessor};
use lance_table::format::pb;
use prost::Message;

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;

use super::dataset::dataset_snapshot_identity;
use super::session::{record_commit, record_dataset_open};
use super::util::{
    cstr_to_str, optional_session_handle, slice_from_ptr, with_explicit_aws_credentials, FfiError,
    FfiResult,
};
use super::write::distributed_storage_options;

const OPERATION_PROPERTY: &str = "vane.operation_id";
const QUERY_PROPERTY: &str = "vane.query_id";
const TASK_ATTEMPT_PROPERTY: &str = "vane.task_attempt_id";

struct DistributedArtifact {
    path: CString,
    size_bytes: u64,
}

struct DistributedAppendTransaction {
    bytes: Vec<u8>,
    artifacts: Vec<DistributedArtifact>,
    byte_count: u64,
}

fn distributed_error(message: impl Into<String>) -> FfiError {
    FfiError::new(ErrorCode::DistributedWrite, message)
}

fn transaction_to_bytes(transaction: &Transaction) -> Vec<u8> {
    let transaction: lance_table::format::Transaction = transaction.into();
    transaction.inner.encode_to_vec()
}

fn transaction_from_bytes(bytes: &[u8]) -> FfiResult<Transaction> {
    if bytes.is_empty() {
        return Err(distributed_error(
            "distributed Lance append transaction payload is empty",
        ));
    }
    let transaction = pb::Transaction::decode(bytes).map_err(|err| {
        distributed_error(format!(
            "decode distributed Lance append transaction: {err}"
        ))
    })?;
    Transaction::try_from(transaction).map_err(|err| {
        distributed_error(format!(
            "validate distributed Lance append transaction: {err}"
        ))
    })
}

fn validate_property<'a>(
    transaction: &'a Transaction,
    name: &str,
    expected: Option<&str>,
) -> FfiResult<&'a str> {
    let value = transaction
        .transaction_properties
        .as_deref()
        .and_then(|properties| properties.get(name))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            distributed_error(format!(
                "distributed Lance append transaction is missing property '{name}'"
            ))
        })?;
    if expected.is_some_and(|expected| expected != value) {
        return Err(distributed_error(format!(
            "distributed Lance append transaction property '{name}' does not match its worker envelope"
        )));
    }
    Ok(value.as_str())
}

fn validate_relative_data_path(path: &str) -> FfiResult<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.starts_with('\\')
        || path.contains("\\")
        || path.contains("://")
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(distributed_error(
            "distributed Lance append transaction contains an unsafe data-file path",
        ));
    }
    Ok(())
}

fn inspect_append_transaction(
    transaction: &Transaction,
    expected_version: u64,
    expected_operation_id: &str,
    expected_query_id: Option<&str>,
    expected_task_attempt_id: Option<&str>,
    expected_row_count: Option<u64>,
) -> FfiResult<(Vec<DistributedArtifact>, u64, u64)> {
    if expected_version == 0 || transaction.read_version != expected_version {
        return Err(distributed_error(
            "distributed Lance append transaction read version does not match the frozen target",
        ));
    }
    validate_property(transaction, OPERATION_PROPERTY, Some(expected_operation_id))?;
    validate_property(transaction, QUERY_PROPERTY, expected_query_id)?;
    validate_property(transaction, TASK_ATTEMPT_PROPERTY, expected_task_attempt_id)?;

    let Operation::Append { fragments } = &transaction.operation else {
        return Err(distributed_error(
            "distributed Lance worker returned a non-append transaction",
        ));
    };
    if fragments.is_empty() {
        return Err(distributed_error(
            "distributed Lance worker returned an empty transaction for non-empty input",
        ));
    }

    let mut artifact_paths = HashSet::new();
    let mut artifacts = Vec::new();
    let mut row_count = 0u64;
    let mut byte_count = 0u64;
    for fragment in fragments {
        if fragment.deletion_file.is_some()
            || !fragment.overlays.is_empty()
            || fragment.files.is_empty()
        {
            return Err(distributed_error(
                "distributed Lance append transaction contains non-append fragment state",
            ));
        }
        let fragment_rows = fragment.physical_rows.ok_or_else(|| {
            distributed_error(
                "distributed Lance append transaction has an unknown fragment row count",
            )
        })?;
        row_count = row_count
            .checked_add(fragment_rows as u64)
            .ok_or_else(|| distributed_error("distributed Lance append row count overflow"))?;
        for file in &fragment.files {
            if file.base_id.is_some() {
                return Err(distributed_error(
                    "distributed Lance writes do not support routed data-file bases",
                ));
            }
            validate_relative_data_path(file.path.as_str())?;
            let path = format!("data/{}", file.path);
            if !artifact_paths.insert(path.clone()) {
                return Err(distributed_error(
                    "distributed Lance append transaction contains a duplicate data-file path",
                ));
            }
            let size_bytes = file.file_size_bytes.get().map_or(0, |size| size.get());
            byte_count = byte_count
                .checked_add(size_bytes)
                .ok_or_else(|| distributed_error("distributed Lance append byte count overflow"))?;
            let path = CString::new(path).map_err(|err| {
                distributed_error(format!(
                    "distributed Lance data-file path contains NUL: {err}"
                ))
            })?;
            artifacts.push(DistributedArtifact { path, size_bytes });
        }
    }
    if row_count == 0 || expected_row_count.is_some_and(|expected| expected != row_count) {
        return Err(distributed_error(
            "distributed Lance append transaction row count does not match its worker envelope",
        ));
    }
    Ok((artifacts, row_count, byte_count))
}

fn build_encoded_transaction(
    transaction: &Transaction,
    bytes: Vec<u8>,
    expected_version: u64,
    operation_id: &str,
    query_id: &str,
    task_attempt_id: &str,
    row_count: u64,
) -> FfiResult<DistributedAppendTransaction> {
    let (artifacts, _, byte_count) = inspect_append_transaction(
        transaction,
        expected_version,
        operation_id,
        Some(query_id),
        Some(task_attempt_id),
        Some(row_count),
    )?;
    Ok(DistributedAppendTransaction {
        bytes,
        artifacts,
        byte_count,
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_encode_append_transaction(
    transaction: *mut c_void,
    expected_version: u64,
    operation_id: *const c_char,
    query_id: *const c_char,
    task_attempt_id: *const c_char,
    row_count: u64,
) -> *mut c_void {
    let result = (|| {
        if transaction.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "transaction is null",
            ));
        }
        // SAFETY: the caller retains ownership of a live Transaction pointer for this call.
        let transaction = unsafe { &*(transaction as *const Transaction) };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let query_id = unsafe { cstr_to_str(query_id, "query_id")? };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let task_attempt_id = unsafe { cstr_to_str(task_attempt_id, "task_attempt_id")? };
        build_encoded_transaction(
            transaction,
            transaction_to_bytes(transaction),
            expected_version,
            operation_id,
            query_id,
            task_attempt_id,
            row_count,
        )
    })();
    match result {
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
pub unsafe extern "C" fn lance_distributed_decode_append_transaction(
    bytes: *const u8,
    bytes_len: usize,
    expected_version: u64,
    operation_id: *const c_char,
    query_id: *const c_char,
    task_attempt_id: *const c_char,
    row_count: u64,
) -> *mut c_void {
    let result = (|| {
        // SAFETY: slice_from_ptr checks the pointer and length before constructing a slice.
        let bytes = unsafe { slice_from_ptr(bytes, bytes_len, "transaction bytes")? };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let query_id = unsafe { cstr_to_str(query_id, "query_id")? };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let task_attempt_id = unsafe { cstr_to_str(task_attempt_id, "task_attempt_id")? };
        let transaction = transaction_from_bytes(bytes)?;
        build_encoded_transaction(
            &transaction,
            bytes.to_vec(),
            expected_version,
            operation_id,
            query_id,
            task_attempt_id,
            row_count,
        )
    })();
    match result {
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
pub unsafe extern "C" fn lance_distributed_transaction_bytes(
    transaction: *mut c_void,
    out_len: *mut usize,
) -> *const u8 {
    if transaction.is_null() || out_len.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "distributed transaction handle or output length is null",
        );
        return ptr::null();
    }
    // SAFETY: the caller supplies a live handle returned by this module.
    let transaction = unsafe { &*(transaction as *const DistributedAppendTransaction) };
    // SAFETY: out_len is non-null and points to caller-owned writable storage.
    unsafe { ptr::write_unaligned(out_len, transaction.bytes.len()) };
    clear_last_error();
    transaction.bytes.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_transaction_artifact_count(
    transaction: *mut c_void,
) -> usize {
    if transaction.is_null() {
        return 0;
    }
    // SAFETY: the caller supplies a live handle returned by this module.
    let transaction = unsafe { &*(transaction as *const DistributedAppendTransaction) };
    transaction.artifacts.len()
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_transaction_artifact_path(
    transaction: *mut c_void,
    index: usize,
) -> *const c_char {
    if transaction.is_null() {
        return ptr::null();
    }
    // SAFETY: the caller supplies a live handle returned by this module.
    let transaction = unsafe { &*(transaction as *const DistributedAppendTransaction) };
    transaction
        .artifacts
        .get(index)
        .map_or(ptr::null(), |artifact| artifact.path.as_ptr())
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_transaction_artifact_size(
    transaction: *mut c_void,
    index: usize,
) -> u64 {
    if transaction.is_null() {
        return 0;
    }
    // SAFETY: the caller supplies a live handle returned by this module.
    let transaction = unsafe { &*(transaction as *const DistributedAppendTransaction) };
    transaction
        .artifacts
        .get(index)
        .map_or(0, |artifact| artifact.size_bytes)
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_transaction_byte_count(transaction: *mut c_void) -> u64 {
    if transaction.is_null() {
        return 0;
    }
    // SAFETY: the caller supplies a live handle returned by this module.
    let transaction = unsafe { &*(transaction as *const DistributedAppendTransaction) };
    transaction.byte_count
}

#[no_mangle]
pub unsafe extern "C" fn lance_free_distributed_transaction(transaction: *mut c_void) {
    if transaction.is_null() {
        return;
    }
    // SAFETY: ownership of the handle is transferred back exactly once by the caller.
    unsafe {
        let _ = Box::from_raw(transaction as *mut DistributedAppendTransaction);
    }
}

async fn open_dataset(
    path: &str,
    storage_options: HashMap<String, String>,
    session: Option<Arc<lance::session::Session>>,
) -> FfiResult<Dataset> {
    let mut builder = DatasetBuilder::from_uri(path);
    builder = with_explicit_aws_credentials(builder, &storage_options);
    builder = builder.with_storage_options(storage_options);
    if let Some(session) = session {
        builder = builder.with_session(session);
    }
    let dataset = builder.load().await.map_err(|err| {
        distributed_error(format!("open distributed Lance coordinator target: {err}"))
    })?;
    record_dataset_open();
    Ok(dataset)
}

fn validate_generation(
    dataset: &Dataset,
    expected_version: u64,
    generation: &str,
) -> FfiResult<()> {
    if expected_version == 0 || dataset.version_id() != expected_version {
        return Err(distributed_error(
            "distributed Lance coordinator target version changed",
        ));
    }
    let identity = match runtime::block_on(dataset_snapshot_identity(dataset)) {
        Ok(Ok(identity)) => format!("snapshot|{identity}"),
        Ok(Err(err)) => return Err(err),
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };
    if identity != generation {
        return Err(distributed_error(
            "distributed Lance coordinator target generation changed",
        ));
    }
    Ok(())
}

fn store_params(storage_options: HashMap<String, String>) -> ObjectStoreParams {
    let mut params = ObjectStoreParams::default();
    if !storage_options.is_empty() {
        params.storage_options_accessor = Some(Arc::new(
            StorageOptionsAccessor::with_static_options(storage_options),
        ));
    }
    params
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub unsafe extern "C" fn lance_distributed_commit_empty_create(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
    operation_id: *const c_char,
    transaction: *mut c_void,
) -> i32 {
    let result = (|| {
        if transaction.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "transaction is null",
            ));
        }
        // SAFETY: ownership of the uncommitted transaction is transferred exactly once.
        let transaction = unsafe { Box::from_raw(transaction as *mut Transaction) };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
        if operation_id.is_empty() || transaction.read_version != 0 {
            return Err(distributed_error(
                "distributed Lance CTAS create transaction has invalid identity",
            ));
        }
        let Operation::Overwrite { fragments, .. } = &transaction.operation else {
            return Err(distributed_error(
                "distributed Lance CTAS preparation requires an overwrite transaction",
            ));
        };
        if !fragments.is_empty() {
            return Err(distributed_error(
                "distributed Lance CTAS preparation produced data fragments",
            ));
        }
        // SAFETY: option arrays are validated before constructing Rust slices.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
        // SAFETY: the optional session handle is validated before dereferencing.
        let session = unsafe { optional_session_handle(session)? };

        let mut transaction = *transaction;
        transaction.uuid = operation_id.clone();
        let mut properties = HashMap::new();
        properties.insert(OPERATION_PROPERTY.to_string(), operation_id.clone());
        transaction.transaction_properties = Some(Arc::new(properties));
        let mut builder = CommitBuilder::new(path.as_str())
            .with_store_params(store_params(storage_options))
            .with_max_retries(0)
            .with_skip_auto_cleanup(true);
        if let Some(session) = session {
            builder = builder.with_session(session);
        }
        let dataset = match runtime::block_on(builder.execute(transaction)) {
            Ok(Ok(dataset)) => dataset,
            Ok(Err(err)) => {
                return Err(distributed_error(format!(
                    "commit prepared distributed Lance CTAS target: {err}"
                )))
            }
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        let expected_suffix = format!("-{operation_id}.txn");
        if dataset.version_id() != 1
            || !dataset
                .manifest()
                .transaction_file
                .as_deref()
                .is_some_and(|path| path.ends_with(expected_suffix.as_str()))
        {
            return Err(distributed_error(
                "prepared distributed Lance CTAS target has an unexpected generation",
            ));
        }
        record_commit();
        Ok(())
    })();
    match result {
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
#[no_mangle]
pub unsafe extern "C" fn lance_distributed_abort_empty_create(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
    operation_id: *const c_char,
) -> i32 {
    let result = (|| {
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
        if operation_id.is_empty() {
            return Err(distributed_error(
                "distributed Lance CTAS abort has an empty operation identity",
            ));
        }
        // SAFETY: option arrays are validated before constructing Rust slices.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
        // SAFETY: the optional session handle is validated before dereferencing.
        let session = unsafe { optional_session_handle(session)? };
        let dataset = match runtime::block_on(open_dataset(
            path.as_str(),
            storage_options.clone(),
            session.clone(),
        )) {
            Ok(Ok(dataset)) => dataset,
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        let expected_suffix = format!("-{operation_id}.txn");
        if dataset.version_id() != 1
            || !dataset.manifest().fragments.is_empty()
            || !dataset
                .manifest()
                .transaction_file
                .as_deref()
                .is_some_and(|path| path.ends_with(expected_suffix.as_str()))
        {
            return Err(distributed_error(
                "refusing to remove a distributed Lance CTAS target that no longer matches its empty prepared generation",
            ));
        }

        let mut builder = DatasetBuilder::from_uri(path.as_str());
        builder = with_explicit_aws_credentials(builder, &storage_options);
        builder = builder.with_storage_options(storage_options);
        if let Some(session) = session {
            builder = builder.with_session(session);
        }
        let (store, base, _) = match runtime::block_on(builder.build_object_store()) {
            Ok(Ok(store)) => store,
            Ok(Err(err)) => {
                return Err(distributed_error(format!(
                    "resolve prepared distributed Lance CTAS store: {err}"
                )))
            }
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        match runtime::block_on(store.remove_dir_all(base)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(distributed_error(format!(
                "remove prepared distributed Lance CTAS target: {err}"
            ))),
            Err(err) => Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        }
    })();
    match result {
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
#[no_mangle]
pub unsafe extern "C" fn lance_distributed_commit_append_transactions(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
    expected_version: u64,
    expected_generation: *const c_char,
    operation_id: *const c_char,
    transaction_bytes: *const *const u8,
    transaction_lengths: *const usize,
    transaction_count: usize,
) -> i32 {
    let result = (|| {
        if transaction_count == 0 {
            return Err(distributed_error(
                "distributed Lance batch commit requires transactions",
            ));
        }
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let expected_generation =
            unsafe { cstr_to_str(expected_generation, "expected_generation")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
        // SAFETY: option arrays are validated before constructing Rust slices.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
        // SAFETY: the optional session handle is validated before dereferencing.
        let session = unsafe { optional_session_handle(session)? };
        // SAFETY: transaction pointer arrays are validated against transaction_count.
        let transaction_bytes =
            unsafe { slice_from_ptr(transaction_bytes, transaction_count, "transaction_bytes")? };
        // SAFETY: transaction length arrays are validated against transaction_count.
        let transaction_lengths = unsafe {
            slice_from_ptr(
                transaction_lengths,
                transaction_count,
                "transaction_lengths",
            )?
        };

        let dataset = match runtime::block_on(open_dataset(path.as_str(), storage_options, session))
        {
            Ok(Ok(dataset)) => dataset,
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        validate_generation(&dataset, expected_version, expected_generation.as_str())?;

        let mut transactions = Vec::with_capacity(transaction_count);
        let mut artifact_paths = HashSet::new();
        for (bytes, length) in transaction_bytes.iter().zip(transaction_lengths) {
            // SAFETY: each payload pointer is validated with its paired length.
            let bytes = unsafe { slice_from_ptr(*bytes, *length, "transaction payload")? };
            let transaction = transaction_from_bytes(bytes)?;
            let (artifacts, _, _) = inspect_append_transaction(
                &transaction,
                expected_version,
                operation_id.as_str(),
                None,
                None,
                None,
            )?;
            for artifact in artifacts {
                let path = artifact.path.to_string_lossy().into_owned();
                if !artifact_paths.insert(path) {
                    return Err(distributed_error(
                        "distributed Lance batch commit selected a duplicate data-file path",
                    ));
                }
            }
            transactions.push(transaction);
        }

        let builder = CommitBuilder::new(Arc::new(dataset))
            .with_max_retries(0)
            .with_skip_auto_cleanup(true);
        let result = match runtime::block_on(builder.execute_batch(transactions)) {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                return Err(distributed_error(format!(
                    "distributed Lance coordinator commit outcome is unknown: {err}"
                )))
            }
            Err(err) => {
                return Err(FfiError::new(
                    ErrorCode::Runtime,
                    format!("distributed Lance coordinator commit outcome is unknown: {err}"),
                ))
            }
        };
        if result.dataset.version_id() != expected_version.saturating_add(1) {
            return Err(distributed_error(
                "distributed Lance coordinator commit returned an unexpected version",
            ));
        }
        record_commit();
        Ok(())
    })();
    match result {
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
#[no_mangle]
pub unsafe extern "C" fn lance_distributed_cleanup_append_transaction(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    operation_id: *const c_char,
    bytes: *const u8,
    bytes_len: usize,
) -> i32 {
    let result = (|| {
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
        // SAFETY: the payload pointer is validated with its paired length.
        let bytes = unsafe { slice_from_ptr(bytes, bytes_len, "transaction payload")? };
        // SAFETY: option arrays are validated before constructing Rust slices.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
        let transaction = transaction_from_bytes(bytes)?;
        let (artifacts, _, _) = inspect_append_transaction(
            &transaction,
            transaction.read_version,
            operation_id.as_str(),
            None,
            None,
            None,
        )?;
        let dataset = match runtime::block_on(open_dataset(path.as_str(), storage_options, None)) {
            Ok(Ok(dataset)) => dataset,
            Ok(Err(err)) => return Err(err),
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        let live_paths: HashSet<String> = dataset
            .manifest()
            .fragments
            .iter()
            .flat_map(|fragment| fragment.files.iter())
            .map(|file| format!("data/{}", file.path))
            .collect();
        let store = match runtime::block_on(dataset.object_store(None)) {
            Ok(Ok(store)) => store,
            Ok(Err(err)) => {
                return Err(distributed_error(format!(
                    "resolve distributed Lance cleanup store: {err}"
                )))
            }
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        let data_dir = dataset.data_dir();
        for artifact in artifacts {
            let relative = artifact.path.to_string_lossy();
            if live_paths.contains(relative.as_ref()) {
                continue;
            }
            let Some(file_name) = relative.strip_prefix("data/") else {
                continue;
            };
            let file_path = data_dir.clone().join(file_name);
            let _ = runtime::block_on(store.delete(&file_path));
            if let Some(stem) = std::path::Path::new(file_name)
                .file_stem()
                .and_then(|stem| stem.to_str())
            {
                let _ = runtime::block_on(store.remove_dir_all(data_dir.clone().join(stem)));
            }
        }
        Ok(())
    })();
    match result {
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
