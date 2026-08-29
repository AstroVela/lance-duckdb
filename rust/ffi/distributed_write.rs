#![cfg(feature = "vane-distributed")]

use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, c_void, CString};
use std::ptr;
use std::sync::Arc;

use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, Dataset};
use lance::io::{ObjectStoreParams, StorageOptionsAccessor};
use lance_table::format::pb;
use object_store::{Error as ObjectStoreError, ObjectStore as _, ObjectStoreExt as _, PutMode};
use prost::Message;

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;

use super::dataset::dataset_snapshot_identity;
use super::session::{record_commit, record_dataset_open};
use super::util::{
    cstr_to_str, optional_cstr_array, optional_session_handle, slice_from_ptr,
    with_explicit_aws_credentials, FfiError, FfiResult,
};
use super::write::{distributed_storage_options, finish_distributed_writer_uncommitted};

const OPERATION_PROPERTY: &str = "vane.operation_id";
const QUERY_PROPERTY: &str = "vane.query_id";
const TASK_ATTEMPT_PROPERTY: &str = "vane.task_attempt_id";
const ATTEMPT_MANIFEST_MAGIC: &[u8; 4] = b"LAM1";
const ATTEMPT_MANIFEST_DIRECTORY: &str = "_vane_distributed_write_attempts";
const ATTEMPT_MANIFEST_SUFFIX: &str = ".manifest";

#[derive(Clone, PartialEq, Message)]
struct DistributedAttemptManifest {
    #[prost(string, tag = "1")]
    operation_id: String,
    #[prost(string, tag = "2")]
    query_id: String,
    #[prost(string, tag = "3")]
    task_attempt_id: String,
    #[prost(uint64, tag = "4")]
    expected_version: u64,
    #[prost(bytes = "vec", repeated, tag = "5")]
    transactions: Vec<Vec<u8>>,
}

struct LoadedAttemptManifest {
    path: object_store::path::Path,
    task_attempt_id: String,
    artifacts: Vec<DistributedArtifact>,
}

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

fn validate_vane_task_attempt_identity(query_id: &str, task_attempt_id: &str) -> FfiResult<()> {
    let Some(suffix) = task_attempt_id.strip_prefix(query_id) else {
        return Err(distributed_error(
            "distributed Lance attempt manifest task does not match its query",
        ));
    };
    let Some(suffix) = suffix.strip_prefix('.') else {
        return Err(distributed_error(
            "distributed Lance attempt manifest task does not match its query",
        ));
    };
    let components: Vec<_> = suffix.split('.').collect();
    if query_id.is_empty()
        || components.len() != 3
        || components.iter().any(|component| {
            component.is_empty()
                || (component.len() > 1 && component.starts_with('0'))
                || !component.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(distributed_error(
            "distributed Lance attempt manifest has an invalid Vane task identity",
        ));
    }
    Ok(())
}

fn is_canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

fn attempt_manifest_bytes(manifest: &DistributedAttemptManifest) -> FfiResult<Vec<u8>> {
    let mut bytes = Vec::with_capacity(ATTEMPT_MANIFEST_MAGIC.len() + manifest.encoded_len());
    bytes.extend_from_slice(ATTEMPT_MANIFEST_MAGIC);
    manifest.encode(&mut bytes).map_err(|err| {
        distributed_error(format!(
            "encode distributed Lance attempt cleanup manifest: {err}"
        ))
    })?;
    Ok(bytes)
}

fn attempt_manifest_from_bytes(bytes: &[u8]) -> FfiResult<DistributedAttemptManifest> {
    let Some(payload) = bytes.strip_prefix(ATTEMPT_MANIFEST_MAGIC) else {
        return Err(distributed_error(
            "distributed Lance attempt cleanup manifest has invalid magic",
        ));
    };
    DistributedAttemptManifest::decode(payload).map_err(|err| {
        distributed_error(format!(
            "decode distributed Lance attempt cleanup manifest: {err}"
        ))
    })
}

fn validate_attempt_manifest(
    manifest: &DistributedAttemptManifest,
    expected_operation_id: &str,
) -> FfiResult<(Vec<DistributedArtifact>, String)> {
    if manifest.operation_id != expected_operation_id
        || !is_canonical_uuid(&manifest.operation_id)
        || manifest.expected_version == 0
        || manifest.transactions.is_empty()
    {
        return Err(distributed_error(
            "distributed Lance attempt cleanup manifest has invalid operation state",
        ));
    }
    validate_vane_task_attempt_identity(&manifest.query_id, &manifest.task_attempt_id)?;

    let mut artifact_paths = HashSet::new();
    let mut artifacts = Vec::new();
    let mut transaction_ids = HashSet::new();
    let mut manifest_id = None;
    for bytes in &manifest.transactions {
        let transaction = transaction_from_bytes(bytes)?;
        if !is_canonical_uuid(&transaction.uuid)
            || !transaction_ids.insert(transaction.uuid.clone())
        {
            return Err(distributed_error(
                "distributed Lance attempt cleanup manifest has an invalid transaction identity",
            ));
        }
        if manifest_id.is_none() {
            manifest_id = Some(transaction.uuid.clone());
        }
        let (transaction_artifacts, _, _) = inspect_append_transaction(
            &transaction,
            manifest.expected_version,
            manifest.operation_id.as_str(),
            Some(manifest.query_id.as_str()),
            Some(manifest.task_attempt_id.as_str()),
            None,
        )?;
        for artifact in transaction_artifacts {
            let path = artifact.path.to_string_lossy().into_owned();
            if !artifact_paths.insert(path) {
                return Err(distributed_error(
                    "distributed Lance attempt cleanup manifest contains a duplicate data-file path",
                ));
            }
            artifacts.push(artifact);
        }
    }
    if artifacts.is_empty() {
        return Err(distributed_error(
            "distributed Lance attempt cleanup manifest has no data artifacts",
        ));
    }
    Ok((
        artifacts,
        manifest_id.expect("non-empty transactions establish a manifest identity"),
    ))
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

fn attempt_manifest_directory(
    dataset: &Dataset,
    operation_id: &str,
) -> FfiResult<object_store::path::Path> {
    if !is_canonical_uuid(operation_id) {
        return Err(distributed_error(
            "distributed Lance attempt cleanup operation identity is invalid",
        ));
    }
    let dataset_root = dataset.data_dir().parent().ok_or_else(|| {
        distributed_error("distributed Lance target has no object-store root directory")
    })?;
    Ok(dataset_root
        .join(ATTEMPT_MANIFEST_DIRECTORY)
        .join(operation_id))
}

fn attempt_manifest_path(
    dataset: &Dataset,
    operation_id: &str,
    manifest_id: &str,
) -> FfiResult<object_store::path::Path> {
    if !is_canonical_uuid(manifest_id) {
        return Err(distributed_error(
            "distributed Lance attempt cleanup manifest identity is invalid",
        ));
    }
    Ok(attempt_manifest_directory(dataset, operation_id)?
        .join(format!("{}{}", manifest_id, ATTEMPT_MANIFEST_SUFFIX)))
}

async fn dataset_object_store(
    dataset: &Dataset,
) -> FfiResult<Arc<lance_io::object_store::ObjectStore>> {
    dataset.object_store(None).await.map_err(|err| {
        distributed_error(format!(
            "resolve distributed Lance attempt cleanup store: {err}"
        ))
    })
}

async fn publish_attempt_manifest(
    dataset: &Dataset,
    manifest: &DistributedAttemptManifest,
) -> FfiResult<()> {
    let (_, manifest_id) = validate_attempt_manifest(manifest, manifest.operation_id.as_str())?;
    let bytes = attempt_manifest_bytes(manifest)?;
    let manifest_path = attempt_manifest_path(
        dataset,
        manifest.operation_id.as_str(),
        manifest_id.as_str(),
    )?;
    let store = dataset_object_store(dataset).await?;
    match store
        .inner
        .put_opts(&manifest_path, bytes.clone().into(), PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::AlreadyExists { .. }) => {
            let existing = store.read_one_all(&manifest_path).await.map_err(|err| {
                distributed_error(format!(
                    "read existing distributed Lance attempt cleanup manifest: {err}"
                ))
            })?;
            if existing.as_ref() != bytes.as_slice() {
                return Err(distributed_error(
                    "distributed Lance task attempt reused a cleanup manifest with different contents",
                ));
            }
            Ok(())
        }
        Err(err) => Err(distributed_error(format!(
            "publish distributed Lance attempt cleanup manifest: {err}"
        ))),
    }
}

fn live_artifact_paths(dataset: &Dataset) -> HashSet<String> {
    dataset
        .manifest()
        .fragments
        .iter()
        .flat_map(|fragment| fragment.files.iter())
        .map(|file| format!("data/{}", file.path))
        .collect()
}

async fn delete_object_if_present(
    store: &lance_io::object_store::ObjectStore,
    path: &object_store::path::Path,
    description: &str,
) -> FfiResult<()> {
    match store.inner.delete(path).await {
        Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
        Err(err) => Err(distributed_error(format!("{description}: {err}"))),
    }
}

async fn cleanup_append_artifacts_in_dataset(
    dataset: &Dataset,
    store: &lance_io::object_store::ObjectStore,
    live_paths: &HashSet<String>,
    artifacts: &[DistributedArtifact],
) -> FfiResult<()> {
    let data_dir = dataset.data_dir();
    for artifact in artifacts {
        let relative = artifact.path.to_string_lossy();
        if live_paths.contains(relative.as_ref()) {
            continue;
        }
        let Some(file_name) = relative.strip_prefix("data/") else {
            return Err(distributed_error(
                "distributed Lance cleanup artifact is outside the data directory",
            ));
        };
        let file_path = data_dir.clone().join(file_name);
        delete_object_if_present(store, &file_path, "delete distributed Lance data artifact")
            .await?;
        if let Some(stem) = std::path::Path::new(file_name)
            .file_stem()
            .and_then(|stem| stem.to_str())
        {
            let sidecar_path = data_dir.clone().join(stem);
            if let Err(err) = store.remove_dir_all(sidecar_path).await {
                if !err.is_not_found() {
                    return Err(distributed_error(format!(
                        "delete distributed Lance data artifact sidecars: {err}"
                    )));
                }
            }
        }
    }
    Ok(())
}

async fn load_attempt_manifests(
    dataset: &Dataset,
    operation_id: &str,
) -> FfiResult<(
    Arc<lance_io::object_store::ObjectStore>,
    Vec<LoadedAttemptManifest>,
)> {
    let store = dataset_object_store(dataset).await?;
    let directory = attempt_manifest_directory(dataset, operation_id)?;
    let objects = store
        .list(Some(directory.clone()))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|err| {
            distributed_error(format!(
                "list distributed Lance attempt cleanup manifests: {err}"
            ))
        })?;

    let mut loaded = Vec::with_capacity(objects.len());
    let mut task_attempt_ids = HashSet::new();
    let mut artifact_owners = HashMap::new();
    for object in objects {
        if object.location.parent().as_ref() != Some(&directory)
            || !object
                .location
                .filename()
                .is_some_and(|name| name.ends_with(ATTEMPT_MANIFEST_SUFFIX))
        {
            return Err(distributed_error(
                "distributed Lance attempt cleanup directory contains an unexpected object",
            ));
        }
        let bytes = store.read_one_all(&object.location).await.map_err(|err| {
            distributed_error(format!(
                "read distributed Lance attempt cleanup manifest: {err}"
            ))
        })?;
        let manifest = attempt_manifest_from_bytes(bytes.as_ref())?;
        let (artifacts, manifest_id) = validate_attempt_manifest(&manifest, operation_id)?;
        if object.location != attempt_manifest_path(dataset, operation_id, manifest_id.as_str())?
            || !task_attempt_ids.insert(manifest.task_attempt_id.clone())
        {
            return Err(distributed_error(
                "distributed Lance attempt cleanup manifest path or task identity is duplicated",
            ));
        }
        for artifact in &artifacts {
            let path = artifact.path.to_string_lossy().into_owned();
            if artifact_owners
                .insert(path, manifest.task_attempt_id.clone())
                .is_some()
            {
                return Err(distributed_error(
                    "distributed Lance attempt cleanup manifests share a data-file path",
                ));
            }
        }
        loaded.push(LoadedAttemptManifest {
            path: object.location,
            task_attempt_id: manifest.task_attempt_id,
            artifacts,
        });
    }
    Ok((store, loaded))
}

async fn cleanup_attempt_manifests(
    dataset: &Dataset,
    operation_id: &str,
    retained_task_attempt_ids: &HashSet<String>,
) -> FfiResult<()> {
    let (store, manifests) = load_attempt_manifests(dataset, operation_id).await?;
    let present_task_attempt_ids: HashSet<_> = manifests
        .iter()
        .map(|manifest| manifest.task_attempt_id.as_str())
        .collect();
    if retained_task_attempt_ids
        .iter()
        .any(|task_attempt_id| !present_task_attempt_ids.contains(task_attempt_id.as_str()))
    {
        return Err(distributed_error(
            "selected distributed Lance task attempt has no cleanup manifest",
        ));
    }

    let live_paths = live_artifact_paths(dataset);
    for manifest in manifests {
        if retained_task_attempt_ids.contains(&manifest.task_attempt_id) {
            continue;
        }
        cleanup_append_artifacts_in_dataset(
            dataset,
            store.as_ref(),
            &live_paths,
            &manifest.artifacts,
        )
        .await?;
        delete_object_if_present(
            store.as_ref(),
            &manifest.path,
            "delete distributed Lance attempt cleanup manifest",
        )
        .await?;
    }
    Ok(())
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
    out_commit_started: *mut u8,
) -> i32 {
    let result = (|| {
        if out_commit_started.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "out_commit_started is null",
            ));
        }
        // SAFETY: the caller provides writable storage for the commit phase marker.
        unsafe { ptr::write_unaligned(out_commit_started, 0) };
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
        // No catalog mutation is attempted before this point. Once the marker
        // is set, every error is conservatively treated as an unknown outcome.
        // SAFETY: out_commit_started was validated above.
        unsafe { ptr::write_unaligned(out_commit_started, 1) };
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
pub unsafe extern "C" fn lance_distributed_publish_attempt_manifest(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    expected_version: u64,
    operation_id: *const c_char,
    query_id: *const c_char,
    task_attempt_id: *const c_char,
    transaction_bytes: *const *const u8,
    transaction_lengths: *const usize,
    transaction_count: usize,
) -> i32 {
    let result = (|| {
        if transaction_count == 0 {
            return Err(distributed_error(
                "distributed Lance attempt cleanup manifest requires transactions",
            ));
        }
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let query_id = unsafe { cstr_to_str(query_id, "query_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let task_attempt_id =
            unsafe { cstr_to_str(task_attempt_id, "task_attempt_id")? }.to_string();
        // SAFETY: option arrays are validated before constructing Rust slices.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
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
        let mut transactions = Vec::with_capacity(transaction_count);
        for (bytes, length) in transaction_bytes.iter().zip(transaction_lengths) {
            // SAFETY: each payload pointer is validated with its paired length.
            let bytes = unsafe { slice_from_ptr(*bytes, *length, "transaction payload")? };
            transactions.push(bytes.to_vec());
        }
        let manifest = DistributedAttemptManifest {
            operation_id,
            query_id,
            task_attempt_id,
            expected_version,
            transactions,
        };
        validate_attempt_manifest(&manifest, manifest.operation_id.as_str())?;

        match runtime::block_on(async {
            let dataset = open_dataset(path.as_str(), storage_options, None).await?;
            publish_attempt_manifest(&dataset, &manifest).await
        }) {
            Ok(result) => result,
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
pub unsafe extern "C" fn lance_distributed_cleanup_attempt_manifests(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    operation_id: *const c_char,
    retained_task_attempt_ids: *const *const c_char,
    retained_task_attempt_count: usize,
) -> i32 {
    let result = (|| {
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
        if operation_id.is_empty() {
            return Err(distributed_error(
                "distributed Lance attempt cleanup operation identity is empty",
            ));
        }
        // SAFETY: option arrays are validated before constructing Rust slices.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
        // SAFETY: the optional string array validates every non-null C string.
        let retained_task_attempt_ids = unsafe {
            optional_cstr_array(
                retained_task_attempt_ids,
                retained_task_attempt_count,
                "retained_task_attempt_ids",
            )?
        };
        let retained_task_attempt_ids: HashSet<_> = retained_task_attempt_ids.into_iter().collect();
        if retained_task_attempt_ids.len() != retained_task_attempt_count {
            return Err(distributed_error(
                "distributed Lance attempt cleanup retained duplicate task identities",
            ));
        }

        match runtime::block_on(async {
            let dataset = open_dataset(path.as_str(), storage_options, None).await?;
            cleanup_attempt_manifests(&dataset, operation_id.as_str(), &retained_task_attempt_ids)
                .await
        }) {
            Ok(result) => result,
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

fn cleanup_candidate_artifacts(transaction: &Transaction) -> FfiResult<Vec<DistributedArtifact>> {
    let Operation::Append { fragments } = &transaction.operation else {
        return Err(distributed_error(
            "distributed Lance cleanup requires an append transaction",
        ));
    };
    if fragments.is_empty()
        || fragments
            .iter()
            .any(|fragment| fragment.deletion_file.is_some() || !fragment.overlays.is_empty())
    {
        return Err(distributed_error(
            "distributed Lance cleanup requires pure append fragments",
        ));
    }

    let mut artifact_paths = HashSet::new();
    let mut artifacts = Vec::new();
    for file in fragments.iter().flat_map(|fragment| fragment.files.iter()) {
        if file.base_id.is_some() || validate_relative_data_path(file.path.as_str()).is_err() {
            continue;
        }
        let relative = format!("data/{}", file.path);
        if !artifact_paths.insert(relative.clone()) {
            continue;
        }
        let Ok(path) = CString::new(relative) else {
            continue;
        };
        let size_bytes = file.file_size_bytes.get().map_or(0, |size| size.get());
        artifacts.push(DistributedArtifact { path, size_bytes });
    }
    if artifacts.is_empty() {
        return Err(distributed_error(
            "distributed Lance append transaction has no safe cleanup artifacts",
        ));
    }
    Ok(artifacts)
}

fn cleanup_append_artifacts(
    path: &str,
    storage_options: HashMap<String, String>,
    artifacts: &[DistributedArtifact],
) -> FfiResult<()> {
    match runtime::block_on(async {
        let dataset = open_dataset(path, storage_options, None).await?;
        let store = dataset_object_store(&dataset).await?;
        let live_paths = live_artifact_paths(&dataset);
        cleanup_append_artifacts_in_dataset(&dataset, store.as_ref(), &live_paths, artifacts).await
    }) {
        Ok(result) => result,
        Err(err) => Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    }
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub unsafe extern "C" fn lance_distributed_cleanup_append_transaction_handle(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    transaction: *mut c_void,
) -> i32 {
    let result = (|| {
        if transaction.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "transaction is null",
            ));
        }
        // SAFETY: this FFI consumes one live Transaction handle on every return path.
        let transaction = unsafe { Box::from_raw(transaction as *mut Transaction) };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: option arrays are validated before constructing Rust slices.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
        let artifacts = cleanup_candidate_artifacts(&transaction)?;
        cleanup_append_artifacts(path.as_str(), storage_options, &artifacts)
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
pub unsafe extern "C" fn lance_distributed_abort_uncommitted_writer(
    writer: *mut c_void,
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    expected_version: u64,
    operation_id: *const c_char,
    query_id: *const c_char,
    task_attempt_id: *const c_char,
) -> i32 {
    let result = (|| {
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let query_id = unsafe { cstr_to_str(query_id, "query_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let task_attempt_id =
            unsafe { cstr_to_str(task_attempt_id, "task_attempt_id")? }.to_string();
        // SAFETY: option arrays are validated before constructing Rust slices.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };

        let mut transaction = ptr::null_mut();
        finish_distributed_writer_uncommitted(writer, &mut transaction)?;
        if transaction.is_null() {
            return Err(distributed_error(
                "aborted distributed Lance writer returned no transaction",
            ));
        }
        // SAFETY: successful writer finalization transfers one Transaction to this call.
        let transaction = unsafe { Box::from_raw(transaction as *mut Transaction) };
        let (artifacts, _, _) = inspect_append_transaction(
            &transaction,
            expected_version,
            operation_id.as_str(),
            Some(query_id.as_str()),
            Some(task_attempt_id.as_str()),
            None,
        )?;
        cleanup_append_artifacts(path.as_str(), storage_options, &artifacts)
    })();
    // SAFETY: this FFI consumes the writer handle on every return path.
    unsafe { super::write::lance_close_writer(writer) };
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
        cleanup_append_artifacts(path.as_str(), storage_options, &artifacts)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance_table::format::{DataFile, Fragment};

    fn append_transaction(files: Vec<DataFile>) -> Transaction {
        let mut fragment = Fragment::new(0);
        fragment.files = files;
        Transaction::new(
            7,
            Operation::Append {
                fragments: vec![fragment],
            },
            None,
        )
    }

    fn attempt_transaction(
        file: DataFile,
        expected_version: u64,
        operation_id: &str,
        query_id: &str,
        task_attempt_id: &str,
    ) -> Transaction {
        let mut fragment = Fragment::new(0);
        fragment.files = vec![file];
        fragment.physical_rows = Some(1);
        let mut transaction = Transaction::new(
            expected_version,
            Operation::Append {
                fragments: vec![fragment],
            },
            None,
        );
        transaction.transaction_properties = Some(Arc::new(HashMap::from([
            (OPERATION_PROPERTY.to_string(), operation_id.to_string()),
            (QUERY_PROPERTY.to_string(), query_id.to_string()),
            (
                TASK_ATTEMPT_PROPERTY.to_string(),
                task_attempt_id.to_string(),
            ),
        ])));
        transaction
    }

    fn attempt_manifest(
        transaction: &Transaction,
        operation_id: &str,
        query_id: &str,
        task_attempt_id: &str,
    ) -> DistributedAttemptManifest {
        DistributedAttemptManifest {
            operation_id: operation_id.to_string(),
            query_id: query_id.to_string(),
            task_attempt_id: task_attempt_id.to_string(),
            expected_version: transaction.read_version,
            transactions: vec![transaction_to_bytes(transaction)],
        }
    }

    #[test]
    fn commit_marker_remains_clear_for_pre_commit_failure() {
        let mut commit_started = 1;
        // SAFETY: the zero transaction count makes every array and string
        // pointer unreachable after the writable marker is initialized.
        let result = unsafe {
            lance_distributed_commit_append_transactions(
                ptr::null(),
                ptr::null(),
                ptr::null(),
                0,
                ptr::null_mut(),
                0,
                ptr::null(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                0,
                &mut commit_started,
            )
        };

        assert_eq!(result, -1);
        assert_eq!(commit_started, 0);
    }

    #[test]
    fn cleanup_candidates_survive_strict_encoding_validation_failure() {
        let transaction = append_transaction(vec![
            DataFile::new("safe.lance", vec![], vec![], 2, 2, None, None),
            DataFile::new("safe.lance", vec![], vec![], 2, 2, None, None),
            DataFile::new("../escape.lance", vec![], vec![], 2, 2, None, None),
            DataFile::new("routed.lance", vec![], vec![], 2, 2, None, Some(1)),
        ]);

        assert!(
            inspect_append_transaction(&transaction, 7, "operation", None, None, None).is_err()
        );

        let artifacts = cleanup_candidate_artifacts(&transaction).unwrap();
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].path.to_str().unwrap(), "data/safe.lance");
    }

    #[test]
    fn selected_attempt_manifest_protects_winner_and_cleans_loser() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
            .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let uri = format!("memory://attempt-cleanup-{}", rand::random::<u64>());
        let dataset = runtime::block_on(Dataset::write(reader, uri.as_str(), None))
            .unwrap()
            .unwrap();
        let store = runtime::block_on(dataset.object_store(None))
            .unwrap()
            .unwrap();

        let operation_id = "00000000-0000-4000-8000-000000000001";
        let query_id = "query";
        let selected_task_attempt_id = "query.0.0.0";
        let loser_task_attempt_id = "query.0.0.1";
        let live_file = dataset.manifest().fragments[0].files[0].clone();
        let live_path = dataset.data_dir().join(live_file.path.as_str());
        let loser_file = DataFile::new("loser.lance", vec![], vec![], 1, 1, None, None);
        let loser_path = dataset.data_dir().join(loser_file.path.as_str());
        runtime::block_on(store.put(&loser_path, b"uncommitted loser"))
            .unwrap()
            .unwrap();

        let selected_transaction = attempt_transaction(
            live_file,
            dataset.version_id(),
            operation_id,
            query_id,
            selected_task_attempt_id,
        );
        let loser_transaction = attempt_transaction(
            loser_file,
            dataset.version_id(),
            operation_id,
            query_id,
            loser_task_attempt_id,
        );
        let selected_manifest = attempt_manifest(
            &selected_transaction,
            operation_id,
            query_id,
            selected_task_attempt_id,
        );
        let loser_manifest = attempt_manifest(
            &loser_transaction,
            operation_id,
            query_id,
            loser_task_attempt_id,
        );
        runtime::block_on(publish_attempt_manifest(&dataset, &selected_manifest))
            .unwrap()
            .unwrap();
        runtime::block_on(publish_attempt_manifest(&dataset, &loser_manifest))
            .unwrap()
            .unwrap();

        let selected_manifest_path =
            attempt_manifest_path(&dataset, operation_id, &selected_transaction.uuid).unwrap();
        let loser_manifest_path =
            attempt_manifest_path(&dataset, operation_id, &loser_transaction.uuid).unwrap();
        let retained = HashSet::from([selected_task_attempt_id.to_string()]);
        runtime::block_on(cleanup_attempt_manifests(&dataset, operation_id, &retained))
            .unwrap()
            .unwrap();

        assert!(runtime::block_on(store.exists(&live_path))
            .unwrap()
            .unwrap());
        assert!(!runtime::block_on(store.exists(&loser_path))
            .unwrap()
            .unwrap());
        assert!(runtime::block_on(store.exists(&selected_manifest_path))
            .unwrap()
            .unwrap());
        assert!(!runtime::block_on(store.exists(&loser_manifest_path))
            .unwrap()
            .unwrap());

        runtime::block_on(cleanup_attempt_manifests(
            &dataset,
            operation_id,
            &HashSet::new(),
        ))
        .unwrap()
        .unwrap();
        assert!(runtime::block_on(store.exists(&live_path))
            .unwrap()
            .unwrap());
        assert!(!runtime::block_on(store.exists(&selected_manifest_path))
            .unwrap()
            .unwrap());
    }
}
