#![cfg(feature = "vane-distributed")]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{c_char, c_void, CString};
use std::fmt;
use std::ptr;
use std::sync::{Arc, Mutex};

use datafusion::common::DFSchema;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ExprSchemable;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::transaction::{Operation, Transaction, UpdateMode};
use lance::dataset::{CommitBuilder, Dataset, InsertBuilder, WriteMode, WriteParams};
use lance_arrow::RecordBatchExt;
use lance_io::object_store::WrappingObjectStore;
use lance_table::format::{Fragment, RowIdMeta};
use lance_table::io::deletion::relative_deletion_file_path;
use lance_table::rowids::{rechunk_sequences, write_row_ids, RowIdSequence};
use object_store::path::Path as ObjectStorePath;
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, RenameOptions, Result as ObjectStoreResult,
};
use prost::Message;
use roaring::RoaringTreemap;
use url::Url;

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::expr_ir::parse_expr_ir;
use crate::runtime;

use super::dataset::dataset_snapshot_identity;
use super::session::{record_commit, record_dataset_open};
use super::update::{apply_deletions, build_row_id_index};
use super::util::{
    cstr_to_str, optional_cstr_array, optional_session_handle, slice_from_ptr,
    with_explicit_aws_credentials, FfiError, FfiResult,
};
use super::write::distributed_storage_options;

const OPERATION_PROPERTY: &str = "vane.operation_id";
const QUERY_PROPERTY: &str = "vane.query_id";
const TASK_ATTEMPT_PROPERTY: &str = "vane.task_attempt_id";
const MUTATION_KIND_PROPERTY: &str = "vane.mutation_kind";
const ROW_COUNT_PROPERTY: &str = "vane.row_count";
const SCHEMA_FINGERPRINT_PROPERTY: &str = "vane.schema_fingerprint";
const ATTEMPT_MANIFEST_MAGIC: &[u8; 4] = b"LMM1";
const ATTEMPT_MANIFEST_DIRECTORY: &str = "_vane_distributed_mutation_attempts";
const ATTEMPT_MANIFEST_SUFFIX: &str = ".manifest";
const UPDATE_TAKE_CHUNK_SIZE: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum MutationKind {
    Delete = 3,
    Update = 4,
}

impl MutationKind {
    fn parse(value: u8) -> FfiResult<Self> {
        match value {
            3 => Ok(Self::Delete),
            4 => Ok(Self::Update),
            _ => Err(mutation_error("unknown distributed Lance mutation kind")),
        }
    }

    fn token(self) -> &'static str {
        match self {
            Self::Delete => "delete",
            Self::Update => "update",
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct MutationAttemptManifest {
    #[prost(string, tag = "1")]
    operation_id: String,
    #[prost(string, tag = "2")]
    query_id: String,
    #[prost(string, tag = "3")]
    task_attempt_id: String,
    #[prost(uint64, tag = "4")]
    expected_version: u64,
    #[prost(uint32, tag = "5")]
    mutation_kind: u32,
    #[prost(string, tag = "6")]
    expected_generation: String,
    #[prost(string, tag = "7")]
    schema_fingerprint: String,
    #[prost(uint64, repeated, tag = "8")]
    source_fragment_ids: Vec<u64>,
    #[prost(bytes = "vec", repeated, tag = "9")]
    transactions: Vec<Vec<u8>>,
}

struct MutationArtifact {
    path: CString,
    size_bytes: u64,
}

struct EncodedMutationTransaction {
    bytes: Vec<u8>,
    artifacts: Vec<MutationArtifact>,
    byte_count: u64,
}

struct LoadedAttemptManifest {
    path: object_store::path::Path,
    task_attempt_id: String,
    artifacts: Vec<MutationArtifact>,
}

struct InspectedMutation {
    artifacts: Vec<MutationArtifact>,
    byte_count: u64,
    source_fragment_ids: Vec<u64>,
    row_count: u64,
}

#[derive(Clone, Copy)]
struct MutationValidation<'a> {
    mutation_kind: MutationKind,
    expected_version: u64,
    operation_id: &'a str,
    query_id: Option<&'a str>,
    task_attempt_id: Option<&'a str>,
    schema_fingerprint: &'a str,
    source_fragment_ids: &'a HashSet<u64>,
}

#[derive(Clone, Copy)]
struct MutationWriteLimits {
    max_rows_per_file: usize,
    max_rows_per_group: usize,
    max_bytes_per_file: usize,
}

#[derive(Clone)]
struct TrackedMutationArtifact {
    store: Arc<dyn ObjectStore>,
    path: ObjectStorePath,
}

impl fmt::Debug for TrackedMutationArtifact {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TrackedMutationArtifact")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct MutationArtifactTracker {
    data_prefix: ObjectStorePath,
    deletion_prefix: ObjectStorePath,
    artifacts: Arc<Mutex<HashMap<String, TrackedMutationArtifact>>>,
}

impl MutationArtifactTracker {
    fn new(dataset: &Dataset) -> FfiResult<Self> {
        let root = dataset
            .data_dir()
            .parent()
            .ok_or_else(|| mutation_error("mutation target has no object-store root"))?;
        Ok(Self {
            data_prefix: root.clone().join("data"),
            deletion_prefix: root.join("_deletions"),
            artifacts: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn cleanup(&self) -> FfiResult<()> {
        let artifacts = std::mem::take(&mut *self.artifacts.lock().unwrap());
        let mut first_error = None;
        for artifact in artifacts.into_values() {
            match artifact.store.delete(&artifact.path).await {
                Ok(()) | Err(ObjectStoreError::NotFound { .. }) => {}
                Err(err) => {
                    first_error.get_or_insert_with(|| err.to_string());
                }
            };
        }
        if let Some(error) = first_error {
            return Err(mutation_error(format!(
                "clean up worker mutation artifacts: {error}"
            )));
        }
        Ok(())
    }

    fn validate_transaction_artifacts(
        &self,
        dataset: &Dataset,
        transaction: &EncodedMutationTransaction,
    ) -> FfiResult<()> {
        let root = dataset
            .data_dir()
            .parent()
            .ok_or_else(|| mutation_error("mutation target has no object-store root"))?;
        let expected: HashSet<_> = transaction
            .artifacts
            .iter()
            .map(|artifact| {
                join_relative_artifact_path(&root, artifact.path.to_string_lossy().as_ref())
            })
            .collect::<FfiResult<_>>()?;
        let tracked: HashSet<_> = self
            .artifacts
            .lock()
            .unwrap()
            .values()
            .map(|artifact| artifact.path.clone())
            .collect();
        if expected != tracked {
            return Err(mutation_error(
                "worker mutation artifact tracking does not match its transaction",
            ));
        }
        Ok(())
    }
}

impl WrappingObjectStore for MutationArtifactTracker {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(MutationTrackingStore {
            store_prefix: store_prefix.to_string(),
            original,
            tracker: self.artifacts.clone(),
            data_prefix: self.data_prefix.clone(),
            deletion_prefix: self.deletion_prefix.clone(),
        })
    }
}

#[derive(Debug)]
struct MutationTrackingStore {
    store_prefix: String,
    original: Arc<dyn ObjectStore>,
    tracker: Arc<Mutex<HashMap<String, TrackedMutationArtifact>>>,
    data_prefix: ObjectStorePath,
    deletion_prefix: ObjectStorePath,
}

impl MutationTrackingStore {
    fn record(&self, path: &ObjectStorePath) {
        if !path.prefix_matches(&self.data_prefix) && !path.prefix_matches(&self.deletion_prefix) {
            return;
        }
        let key = format!("{}|{path}", self.store_prefix);
        self.tracker.lock().unwrap().insert(
            key,
            TrackedMutationArtifact {
                store: self.original.clone(),
                path: path.clone(),
            },
        );
    }
}

impl fmt::Display for MutationTrackingStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "MutationTrackingStore({})", self.original)
    }
}

#[async_trait::async_trait]
impl ObjectStore for MutationTrackingStore {
    async fn put_opts(
        &self,
        location: &ObjectStorePath,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        let result = self.original.put_opts(location, payload, options).await?;
        self.record(location);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectStorePath,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        let upload = self.original.put_multipart_opts(location, options).await?;
        self.record(location);
        Ok(upload)
    }

    async fn get_opts(
        &self,
        location: &ObjectStorePath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.original.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<ObjectStorePath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectStorePath>> {
        self.original.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.original.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectStorePath>,
        offset: &ObjectStorePath,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.original.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectStorePath>,
    ) -> ObjectStoreResult<ListResult> {
        self.original.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.original.copy_opts(from, to, options).await?;
        self.record(to);
        Ok(())
    }

    async fn rename_opts(
        &self,
        from: &ObjectStorePath,
        to: &ObjectStorePath,
        options: RenameOptions,
    ) -> ObjectStoreResult<()> {
        self.original.rename_opts(from, to, options).await?;
        self.record(to);
        Ok(())
    }
}

fn mutation_error(message: impl Into<String>) -> FfiError {
    FfiError::new(ErrorCode::DistributedWrite, message)
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

fn validate_task_attempt_identity(query_id: &str, task_attempt_id: &str) -> FfiResult<()> {
    let suffix = task_attempt_id
        .strip_prefix(query_id)
        .and_then(|suffix| suffix.strip_prefix('.'))
        .ok_or_else(|| mutation_error("mutation attempt does not match its query"))?;
    let components: Vec<_> = suffix.split('.').collect();
    if query_id.is_empty()
        || components.len() != 3
        || components.iter().any(|component| {
            component.is_empty()
                || (component.len() > 1 && component.starts_with('0'))
                || !component.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(mutation_error(
            "distributed Lance mutation has an invalid Vane task identity",
        ));
    }
    Ok(())
}

fn transaction_to_bytes(transaction: &Transaction) -> Vec<u8> {
    let transaction: lance_table::format::Transaction = transaction.into();
    transaction.inner.encode_to_vec()
}

fn transaction_from_bytes(bytes: &[u8]) -> FfiResult<Transaction> {
    if bytes.is_empty() {
        return Err(mutation_error("mutation transaction payload is empty"));
    }
    let transaction = lance_table::format::pb::Transaction::decode(bytes)
        .map_err(|err| mutation_error(format!("decode mutation transaction: {err}")))?;
    Transaction::try_from(transaction)
        .map_err(|err| mutation_error(format!("validate mutation transaction: {err}")))
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
        .ok_or_else(|| mutation_error(format!("mutation transaction is missing '{name}'")))?;
    if expected.is_some_and(|expected| expected != value) {
        return Err(mutation_error(format!(
            "mutation transaction property '{name}' does not match its frozen bind"
        )));
    }
    Ok(value)
}

fn validate_relative_path(path: &str) -> FfiResult<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.starts_with('\\')
        || path.contains('\\')
        || path.contains("://")
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(mutation_error(
            "mutation transaction contains an unsafe artifact path",
        ));
    }
    Ok(())
}

fn join_relative_artifact_path(
    root: &ObjectStorePath,
    relative: &str,
) -> FfiResult<ObjectStorePath> {
    validate_relative_path(relative)?;
    let relative = ObjectStorePath::parse(relative)
        .map_err(|err| mutation_error(format!("parse mutation artifact path: {err}")))?;
    let mut path = root.clone();
    path.extend(relative.parts());
    Ok(path)
}

fn add_artifact(
    paths: &mut HashSet<String>,
    artifacts: &mut Vec<MutationArtifact>,
    path: String,
    size_bytes: u64,
) -> FfiResult<()> {
    validate_relative_path(path.as_str())?;
    if !paths.insert(path.clone()) {
        return Err(mutation_error(
            "mutation transaction contains a duplicate artifact path",
        ));
    }
    let path = CString::new(path)
        .map_err(|err| mutation_error(format!("mutation artifact path contains NUL: {err}")))?;
    artifacts.push(MutationArtifact { path, size_bytes });
    Ok(())
}

fn inspect_transaction(
    transaction: &Transaction,
    validation: MutationValidation<'_>,
    expected_row_count: Option<u64>,
) -> FfiResult<InspectedMutation> {
    if validation.expected_version == 0 || transaction.read_version != validation.expected_version {
        return Err(mutation_error(
            "mutation transaction read version does not match the frozen target",
        ));
    }
    if !is_canonical_uuid(&transaction.uuid) {
        return Err(mutation_error("mutation transaction UUID is invalid"));
    }
    validate_property(
        transaction,
        OPERATION_PROPERTY,
        Some(validation.operation_id),
    )?;
    validate_property(transaction, QUERY_PROPERTY, validation.query_id)?;
    validate_property(
        transaction,
        TASK_ATTEMPT_PROPERTY,
        validation.task_attempt_id,
    )?;
    validate_property(
        transaction,
        MUTATION_KIND_PROPERTY,
        Some(validation.mutation_kind.token()),
    )?;
    validate_property(
        transaction,
        SCHEMA_FINGERPRINT_PROPERTY,
        Some(validation.schema_fingerprint),
    )?;
    let row_count = validate_property(transaction, ROW_COUNT_PROPERTY, None)?
        .parse::<u64>()
        .map_err(|_| mutation_error("mutation transaction row count is invalid"))?;
    if row_count == 0 || expected_row_count.is_some_and(|expected| expected != row_count) {
        return Err(mutation_error(
            "mutation transaction row count does not match its worker envelope",
        ));
    }

    let mut referenced = HashSet::new();
    let mut artifact_paths = HashSet::new();
    let mut artifacts = Vec::new();
    let mut byte_count = 0u64;

    let mut inspect_updated_fragment = |fragment: &Fragment| -> FfiResult<()> {
        if !validation.source_fragment_ids.contains(&fragment.id) || !referenced.insert(fragment.id)
        {
            return Err(mutation_error(
                "mutation transaction references an unauthorized or duplicate source fragment",
            ));
        }
        let deletion_file = fragment
            .deletion_file
            .as_ref()
            .ok_or_else(|| mutation_error("updated mutation fragment has no deletion file"))?;
        if deletion_file.base_id.is_some() {
            return Err(mutation_error(
                "distributed mutations do not support routed deletion files",
            ));
        }
        add_artifact(
            &mut artifact_paths,
            &mut artifacts,
            relative_deletion_file_path(fragment.id, deletion_file),
            0,
        )
    };

    match (&transaction.operation, validation.mutation_kind) {
        (
            Operation::Delete {
                updated_fragments,
                deleted_fragment_ids,
                predicate,
            },
            MutationKind::Delete,
        ) => {
            if predicate.is_empty()
                || (updated_fragments.is_empty() && deleted_fragment_ids.is_empty())
            {
                return Err(mutation_error("DELETE transaction has no source mutation"));
            }
            for fragment in updated_fragments {
                inspect_updated_fragment(fragment)?;
            }
            for fragment_id in deleted_fragment_ids {
                if !validation.source_fragment_ids.contains(fragment_id)
                    || !referenced.insert(*fragment_id)
                {
                    return Err(mutation_error(
                        "DELETE transaction removes an unauthorized or duplicate source fragment",
                    ));
                }
            }
        }
        (
            Operation::Update {
                removed_fragment_ids,
                updated_fragments,
                new_fragments,
                fields_modified,
                merged_generations,
                fields_for_preserving_frag_bitmap,
                update_mode,
                inserted_rows_filter,
                updated_fragment_offsets,
            },
            MutationKind::Update,
        ) => {
            if (updated_fragments.is_empty() && removed_fragment_ids.is_empty())
                || new_fragments.is_empty()
                || !fields_modified.is_empty()
                || !merged_generations.is_empty()
                || fields_for_preserving_frag_bitmap.is_empty()
                || !matches!(update_mode, Some(UpdateMode::RewriteRows))
                || inserted_rows_filter.is_some()
                || updated_fragment_offsets.is_some()
            {
                return Err(mutation_error(
                    "UPDATE transaction has invalid rewrite state",
                ));
            }
            for fragment in updated_fragments {
                inspect_updated_fragment(fragment)?;
            }
            for fragment_id in removed_fragment_ids {
                if !validation.source_fragment_ids.contains(fragment_id)
                    || !referenced.insert(*fragment_id)
                {
                    return Err(mutation_error(
                        "UPDATE transaction removes an unauthorized or duplicate source fragment",
                    ));
                }
            }
            let mut written_rows = 0u64;
            for fragment in new_fragments {
                if fragment.deletion_file.is_some()
                    || !fragment.overlays.is_empty()
                    || fragment.files.is_empty()
                {
                    return Err(mutation_error(
                        "UPDATE replacement fragment contains invalid state",
                    ));
                }
                written_rows = written_rows
                    .checked_add(fragment.physical_rows.unwrap_or_default() as u64)
                    .ok_or_else(|| mutation_error("UPDATE replacement row count overflow"))?;
                for file in &fragment.files {
                    if file.base_id.is_some() {
                        return Err(mutation_error(
                            "distributed mutations do not support routed data files",
                        ));
                    }
                    let size_bytes = file.file_size_bytes.get().map_or(0, |size| size.get());
                    byte_count = byte_count
                        .checked_add(size_bytes)
                        .ok_or_else(|| mutation_error("mutation byte count overflow"))?;
                    add_artifact(
                        &mut artifact_paths,
                        &mut artifacts,
                        format!("data/{}", file.path),
                        size_bytes,
                    )?;
                }
                if let Some(RowIdMeta::External(_)) = &fragment.row_id_meta {
                    return Err(mutation_error(
                        "distributed UPDATE does not support external replacement row-id state",
                    ));
                }
            }
            if written_rows != row_count {
                return Err(mutation_error(
                    "UPDATE replacement rows do not match the mutation row count",
                ));
            }
        }
        _ => {
            return Err(mutation_error(
                "mutation transaction operation does not match its capability",
            ))
        }
    }

    let mut source_fragment_ids: Vec<_> = referenced.into_iter().collect();
    source_fragment_ids.sort_unstable();
    Ok(InspectedMutation {
        artifacts,
        byte_count,
        source_fragment_ids,
        row_count,
    })
}

fn encoded_transaction(
    transaction: &Transaction,
    validation: MutationValidation<'_>,
    row_count: u64,
) -> FfiResult<EncodedMutationTransaction> {
    let inspected = inspect_transaction(transaction, validation, Some(row_count))?;
    Ok(EncodedMutationTransaction {
        bytes: transaction_to_bytes(transaction),
        artifacts: inspected.artifacts,
        byte_count: inspected.byte_count,
    })
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
    let dataset = builder
        .load()
        .await
        .map_err(|err| mutation_error(format!("open distributed mutation target: {err}")))?;
    record_dataset_open();
    Ok(dataset)
}

fn mutation_worker_uri(path: &str) -> FfiResult<String> {
    let parsed = Url::parse(path).ok();
    if parsed
        .as_ref()
        .is_some_and(|url| !matches!(url.scheme(), "file" | "file+uring"))
    {
        return Ok(path.to_string());
    }
    let local_path = match parsed {
        Some(url) => std::path::PathBuf::from(url.path()),
        None => std::path::PathBuf::from(path),
    };
    let absolute = std::fs::canonicalize(&local_path).map_err(|err| {
        mutation_error(format!(
            "resolve local distributed mutation target '{}': {err}",
            local_path.display()
        ))
    })?;
    let file_uri = Url::from_file_path(&absolute).map_err(|_| {
        mutation_error(format!(
            "encode local distributed mutation target '{}'",
            absolute.display()
        ))
    })?;
    Ok(file_uri.as_str().replacen("file:", "file-object-store:", 1))
}

async fn validate_generation(
    dataset: &Dataset,
    expected_version: u64,
    expected_generation: &str,
) -> FfiResult<()> {
    if expected_version == 0 || dataset.version_id() != expected_version {
        return Err(mutation_error(
            "distributed mutation target version changed",
        ));
    }
    let identity = format!("snapshot|{}", dataset_snapshot_identity(dataset).await?);
    if identity != expected_generation {
        return Err(mutation_error(
            "distributed mutation target generation changed",
        ));
    }
    Ok(())
}

fn transaction_properties(
    mutation_kind: MutationKind,
    operation_id: &str,
    query_id: &str,
    task_attempt_id: &str,
    schema_fingerprint: &str,
    row_count: u64,
) -> Arc<HashMap<String, String>> {
    Arc::new(HashMap::from([
        (OPERATION_PROPERTY.to_string(), operation_id.to_string()),
        (QUERY_PROPERTY.to_string(), query_id.to_string()),
        (
            TASK_ATTEMPT_PROPERTY.to_string(),
            task_attempt_id.to_string(),
        ),
        (
            MUTATION_KIND_PROPERTY.to_string(),
            mutation_kind.token().to_string(),
        ),
        (ROW_COUNT_PROPERTY.to_string(), row_count.to_string()),
        (
            SCHEMA_FINGERPRINT_PROPERTY.to_string(),
            schema_fingerprint.to_string(),
        ),
    ]))
}

async fn resolve_row_addresses(
    dataset: &Dataset,
    row_ids: &[u64],
    source_fragment_ids: &HashSet<u64>,
) -> FfiResult<RoaringTreemap> {
    let mut row_addrs = RoaringTreemap::new();
    if dataset.manifest.uses_stable_row_ids() {
        let index = build_row_id_index(dataset).await.map_err(mutation_error)?;
        for row_id in row_ids {
            let address = index
                .get(*row_id)
                .ok_or_else(|| mutation_error(format!("row id is not live: {row_id}")))?;
            let address = u64::from(address);
            let fragment_id = address >> 32;
            if !source_fragment_ids.contains(&fragment_id) || !row_addrs.insert(address) {
                return Err(mutation_error(
                    "row id references an unauthorized or duplicate source row",
                ));
            }
        }
    } else {
        for address in row_ids {
            let fragment_id = address >> 32;
            if !source_fragment_ids.contains(&fragment_id) || !row_addrs.insert(*address) {
                return Err(mutation_error(
                    "row address references an unauthorized or duplicate source row",
                ));
            }
        }
    }

    let fragments = dataset.get_fragments();
    let mut offsets = BTreeMap::<u64, Vec<u32>>::new();
    for address in row_addrs.iter() {
        offsets
            .entry(address >> 32)
            .or_default()
            .push(address as u32);
    }
    for (fragment_id, fragment_offsets) in offsets {
        let fragment = fragments
            .iter()
            .find(|fragment| fragment.id() as u64 == fragment_id)
            .ok_or_else(|| mutation_error("row address source fragment is missing"))?;
        let physical_rows = fragment
            .metadata()
            .physical_rows
            .ok_or_else(|| mutation_error("source fragment row count is unknown"))?;
        let deletion_vector = fragment
            .get_deletion_vector()
            .await
            .map_err(|err| mutation_error(format!("read source deletions: {err}")))?;
        for offset in fragment_offsets {
            if offset as usize >= physical_rows
                || deletion_vector
                    .as_ref()
                    .is_some_and(|deletions| deletions.contains(offset))
            {
                return Err(mutation_error(
                    "row address does not identify a live source row",
                ));
            }
        }
    }
    Ok(row_addrs)
}

async fn create_delete_transaction(
    dataset: &Dataset,
    row_addrs: &RoaringTreemap,
    properties: Arc<HashMap<String, String>>,
) -> FfiResult<Transaction> {
    let (updated_fragments, deleted_fragment_ids) = apply_deletions(dataset, row_addrs)
        .await
        .map_err(mutation_error)?;
    let operation = Operation::Delete {
        updated_fragments,
        deleted_fragment_ids,
        predicate: "vane distributed row-id mutation".to_string(),
    };
    let mut transaction = Transaction::new(dataset.version_id(), operation, None);
    transaction.transaction_properties = Some(properties);
    Ok(transaction)
}

async fn create_update_transaction(
    dataset: Arc<Dataset>,
    row_ids: &[u64],
    row_addrs: &RoaringTreemap,
    set_columns: &[String],
    set_expr_irs: &[Vec<u8>],
    limits: MutationWriteLimits,
    properties: Arc<HashMap<String, String>>,
) -> FfiResult<Transaction> {
    if set_columns.is_empty() || set_columns.len() != set_expr_irs.len() {
        return Err(mutation_error("UPDATE SET expression state is invalid"));
    }
    let arrow_schema: Arc<arrow_schema::Schema> = Arc::new(dataset.schema().into());
    let df_schema = DFSchema::try_from(arrow_schema.as_ref().clone())
        .map_err(|err| mutation_error(err.to_string()))?;
    let session_ctx = SessionContext::new();
    let mut update_exprs = Vec::with_capacity(set_columns.len());
    let mut fields_for_preserving_frag_bitmap = Vec::with_capacity(set_columns.len());
    let mut seen_columns = HashSet::new();
    for (column, value_ir) in set_columns.iter().zip(set_expr_irs) {
        if column.contains('.') || !seen_columns.insert(column.clone()) {
            return Err(mutation_error(
                "distributed UPDATE requires unique top-level SET columns",
            ));
        }
        let field = dataset
            .schema()
            .field(column.as_str())
            .ok_or_else(|| mutation_error(format!("UPDATE column does not exist: {column}")))?;
        let mut value_expr = parse_expr_ir(value_ir, Some(&session_ctx)).map_err(mutation_error)?;
        let destination_type = field.data_type();
        let source_type = value_expr
            .get_type(&df_schema)
            .map_err(|err| mutation_error(err.to_string()))?;
        if destination_type != source_type {
            value_expr = value_expr
                .cast_to(&destination_type, &df_schema)
                .map_err(|err| mutation_error(err.to_string()))?;
        }
        let physical_expr = session_ctx
            .create_physical_expr(value_expr, &df_schema)
            .map_err(|err| mutation_error(err.to_string()))?;
        update_exprs.push((column.clone(), physical_expr));
        let field_id = dataset
            .schema()
            .field_id(column.as_str())
            .map_err(|err| mutation_error(err.to_string()))?;
        fields_for_preserving_frag_bitmap.push(field_id as u32);
    }

    let mut updated_batches = Vec::new();
    for row_id_chunk in row_ids.chunks(UPDATE_TAKE_CHUNK_SIZE) {
        let original = dataset
            .take_rows(row_id_chunk, dataset.schema().clone())
            .await
            .map_err(|err| mutation_error(format!("take UPDATE source rows: {err}")))?;
        if original.num_rows() != row_id_chunk.len() {
            return Err(mutation_error(
                "UPDATE source take returned an unexpected row count",
            ));
        }
        let mut replacements = Vec::with_capacity(update_exprs.len());
        for (column, expression) in &update_exprs {
            let values = expression
                .evaluate(&original)
                .and_then(|value| value.into_array(original.num_rows()))
                .map_err(|err| mutation_error(err.to_string()))?;
            replacements.push((column.clone(), values));
        }
        let mut updated = original;
        for (column, values) in replacements {
            updated = updated
                .replace_column_by_name(column.as_str(), values)
                .map_err(|err| mutation_error(err.to_string()))?;
        }
        updated_batches.push(updated);
    }

    let write_params = WriteParams {
        mode: WriteMode::Append,
        max_rows_per_file: limits.max_rows_per_file,
        max_rows_per_group: limits.max_rows_per_group,
        max_bytes_per_file: limits.max_bytes_per_file,
        skip_auto_cleanup: true,
        ..Default::default()
    };
    let append_transaction = InsertBuilder::new(dataset.clone())
        .with_params(&write_params)
        .execute_uncommitted(updated_batches)
        .await
        .map_err(|err| mutation_error(format!("write UPDATE replacements: {err}")))?;
    let Operation::Append { mut fragments } = append_transaction.operation else {
        return Err(mutation_error(
            "UPDATE replacement writer returned a non-append transaction",
        ));
    };

    if dataset.manifest.uses_stable_row_ids() {
        let mut sequence = RowIdSequence::new();
        sequence.extend(row_ids.into());
        let fragment_sizes = fragments
            .iter()
            .map(|fragment| fragment.physical_rows.unwrap_or_default() as u64);
        let sequences = rechunk_sequences(vec![sequence], fragment_sizes, false)
            .map_err(|err| mutation_error(err.to_string()))?;
        for (fragment, sequence) in fragments.iter_mut().zip(sequences) {
            fragment.row_id_meta = Some(RowIdMeta::Inline(write_row_ids(&sequence)));
        }
    }

    let (updated_fragments, removed_fragment_ids) = apply_deletions(dataset.as_ref(), row_addrs)
        .await
        .map_err(mutation_error)?;
    let operation = Operation::Update {
        removed_fragment_ids,
        updated_fragments,
        new_fragments: fragments,
        fields_modified: Vec::new(),
        merged_generations: Vec::new(),
        fields_for_preserving_frag_bitmap,
        update_mode: Some(UpdateMode::RewriteRows),
        inserted_rows_filter: None,
        updated_fragment_offsets: None,
    };
    let mut transaction = Transaction::new(dataset.version_id(), operation, None);
    transaction.transaction_properties = Some(properties);
    Ok(transaction)
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub unsafe extern "C" fn lance_distributed_create_mutation_transaction(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
    mutation_kind: u8,
    expected_version: u64,
    expected_generation: *const c_char,
    operation_id: *const c_char,
    query_id: *const c_char,
    task_attempt_id: *const c_char,
    schema_fingerprint: *const c_char,
    source_fragment_ids: *const u64,
    source_fragment_count: usize,
    row_ids: *const u64,
    row_id_count: usize,
    set_columns: *const *const c_char,
    set_expr_irs: *const *const u8,
    set_expr_ir_lengths: *const usize,
    set_count: usize,
    max_rows_per_file: u64,
    max_rows_per_group: u64,
    max_bytes_per_file: u64,
) -> *mut c_void {
    let result = (|| {
        let mutation_kind = MutationKind::parse(mutation_kind)?;
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let expected_generation =
            unsafe { cstr_to_str(expected_generation, "expected_generation")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let query_id = unsafe { cstr_to_str(query_id, "query_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let task_attempt_id =
            unsafe { cstr_to_str(task_attempt_id, "task_attempt_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let schema_fingerprint =
            unsafe { cstr_to_str(schema_fingerprint, "schema_fingerprint")? }.to_string();
        if expected_version == 0
            || expected_generation.is_empty()
            || !is_canonical_uuid(operation_id.as_str())
            || schema_fingerprint.len() != 32
            || row_id_count == 0
        {
            return Err(mutation_error("mutation worker bind is incomplete"));
        }
        validate_task_attempt_identity(query_id.as_str(), task_attempt_id.as_str())?;
        // SAFETY: the fragment-id array is validated against its element count.
        let source_fragment_ids = unsafe {
            slice_from_ptr(
                source_fragment_ids,
                source_fragment_count,
                "source_fragment_ids",
            )?
        };
        let source_fragment_ids: HashSet<_> = source_fragment_ids.iter().copied().collect();
        if source_fragment_ids.len() != source_fragment_count {
            return Err(mutation_error("mutation source fragments are duplicated"));
        }
        // SAFETY: the row-id array is validated against its element count.
        let row_ids = unsafe { slice_from_ptr(row_ids, row_id_count, "row_ids")? };
        let mut sorted_row_ids = row_ids.to_vec();
        sorted_row_ids.sort_unstable();
        if sorted_row_ids.windows(2).any(|rows| rows[0] == rows[1]) {
            return Err(mutation_error("mutation worker received duplicate row ids"));
        }

        // SAFETY: optional_cstr_array validates every pointer before conversion.
        let set_columns_raw =
            unsafe { optional_cstr_array(set_columns, set_count, "set_columns")? };
        let set_expr_ptrs = if set_count == 0 {
            &[]
        } else {
            // SAFETY: the expression-pointer array is validated against set_count.
            unsafe { slice_from_ptr(set_expr_irs, set_count, "set_expr_irs")? }
        };
        let set_expr_lengths = if set_count == 0 {
            &[]
        } else {
            // SAFETY: the expression-length array is validated against set_count.
            unsafe { slice_from_ptr(set_expr_ir_lengths, set_count, "set_expr_ir_lengths")? }
        };
        let mut set_expr_values = Vec::with_capacity(set_count);
        for (pointer, length) in set_expr_ptrs.iter().zip(set_expr_lengths) {
            // SAFETY: every expression pointer is validated with its paired length.
            let value = unsafe { slice_from_ptr(*pointer, *length, "set_expr_ir")? };
            set_expr_values.push(value.to_vec());
        }
        if (mutation_kind == MutationKind::Delete && set_count != 0)
            || (mutation_kind == MutationKind::Update
                && (set_count == 0 || set_columns_raw.len() != set_expr_values.len()))
        {
            return Err(mutation_error("mutation SET expression state is invalid"));
        }
        let max_rows_per_file = usize::try_from(max_rows_per_file)
            .map_err(|err| mutation_error(format!("invalid max_rows_per_file: {err}")))?;
        let max_rows_per_group = usize::try_from(max_rows_per_group)
            .map_err(|err| mutation_error(format!("invalid max_rows_per_group: {err}")))?;
        let max_bytes_per_file = usize::try_from(max_bytes_per_file)
            .map_err(|err| mutation_error(format!("invalid max_bytes_per_file: {err}")))?;
        let write_limits = MutationWriteLimits {
            max_rows_per_file,
            max_rows_per_group,
            max_bytes_per_file,
        };
        // SAFETY: the storage-option arrays are validated as equal-length C strings.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
        // SAFETY: the optional session handle is validated before dereferencing.
        let session = unsafe { optional_session_handle(session)? };

        let encoded = match runtime::block_on(async {
            let worker_uri = mutation_worker_uri(path.as_str())?;
            let dataset = open_dataset(worker_uri.as_str(), storage_options, session).await?;
            validate_generation(&dataset, expected_version, expected_generation.as_str()).await?;
            let tracker = Arc::new(MutationArtifactTracker::new(&dataset)?);
            let dataset = Arc::new(
                dataset
                    .with_object_store_wrappers([tracker.clone() as Arc<dyn WrappingObjectStore>]),
            );
            let attempt_result = async {
                let row_addrs = resolve_row_addresses(
                    dataset.as_ref(),
                    sorted_row_ids.as_slice(),
                    &source_fragment_ids,
                )
                .await?;
                let properties = transaction_properties(
                    mutation_kind,
                    operation_id.as_str(),
                    query_id.as_str(),
                    task_attempt_id.as_str(),
                    schema_fingerprint.as_str(),
                    sorted_row_ids.len() as u64,
                );
                let transaction = match mutation_kind {
                    MutationKind::Delete => {
                        create_delete_transaction(dataset.as_ref(), &row_addrs, properties).await
                    }
                    MutationKind::Update => {
                        create_update_transaction(
                            dataset.clone(),
                            sorted_row_ids.as_slice(),
                            &row_addrs,
                            &set_columns_raw,
                            &set_expr_values,
                            write_limits,
                            properties,
                        )
                        .await
                    }
                }?;
                let encoded = encoded_transaction(
                    &transaction,
                    MutationValidation {
                        mutation_kind,
                        expected_version,
                        operation_id: operation_id.as_str(),
                        query_id: Some(query_id.as_str()),
                        task_attempt_id: Some(task_attempt_id.as_str()),
                        schema_fingerprint: schema_fingerprint.as_str(),
                        source_fragment_ids: &source_fragment_ids,
                    },
                    sorted_row_ids.len() as u64,
                )?;
                tracker.validate_transaction_artifacts(dataset.as_ref(), &encoded)?;
                Ok::<_, FfiError>(encoded)
            }
            .await;
            match attempt_result {
                Ok(encoded) => Ok(encoded),
                Err(error) => match tracker.cleanup().await {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(mutation_error(format!(
                        "{}; {}",
                        error.message, cleanup_error.message
                    ))),
                },
            }
        }) {
            Ok(result) => result?,
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        Ok(encoded)
    })();
    match result {
        Ok(transaction) => {
            clear_last_error();
            Box::into_raw(Box::new(transaction)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub unsafe extern "C" fn lance_distributed_decode_mutation_transaction(
    bytes: *const u8,
    bytes_len: usize,
    mutation_kind: u8,
    expected_version: u64,
    operation_id: *const c_char,
    query_id: *const c_char,
    task_attempt_id: *const c_char,
    schema_fingerprint: *const c_char,
    source_fragment_ids: *const u64,
    source_fragment_count: usize,
    row_count: u64,
) -> *mut c_void {
    let result = (|| {
        let mutation_kind = MutationKind::parse(mutation_kind)?;
        // SAFETY: the transaction payload pointer is validated with its length.
        let bytes = unsafe { slice_from_ptr(bytes, bytes_len, "transaction bytes")? };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let query_id = unsafe { cstr_to_str(query_id, "query_id")? };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let task_attempt_id = unsafe { cstr_to_str(task_attempt_id, "task_attempt_id")? };
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let schema_fingerprint = unsafe { cstr_to_str(schema_fingerprint, "schema_fingerprint")? };
        // SAFETY: the fragment-id array is validated against its element count.
        let source_fragment_ids = unsafe {
            slice_from_ptr(
                source_fragment_ids,
                source_fragment_count,
                "source_fragment_ids",
            )?
        };
        let source_fragment_ids: HashSet<_> = source_fragment_ids.iter().copied().collect();
        if source_fragment_ids.len() != source_fragment_count {
            return Err(mutation_error("mutation source fragments are duplicated"));
        }
        let transaction = transaction_from_bytes(bytes)?;
        let inspected = inspect_transaction(
            &transaction,
            MutationValidation {
                mutation_kind,
                expected_version,
                operation_id,
                query_id: Some(query_id),
                task_attempt_id: Some(task_attempt_id),
                schema_fingerprint,
                source_fragment_ids: &source_fragment_ids,
            },
            Some(row_count),
        )?;
        Ok(EncodedMutationTransaction {
            bytes: bytes.to_vec(),
            artifacts: inspected.artifacts,
            byte_count: inspected.byte_count,
        })
    })();
    match result {
        Ok(transaction) => {
            clear_last_error();
            Box::into_raw(Box::new(transaction)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_mutation_transaction_bytes(
    transaction: *mut c_void,
    out_len: *mut usize,
) -> *const u8 {
    if transaction.is_null() || out_len.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "mutation transaction handle is null",
        );
        return ptr::null();
    }
    // SAFETY: the caller supplies a live handle returned by this module.
    let transaction = unsafe { &*(transaction as *const EncodedMutationTransaction) };
    // SAFETY: out_len is non-null and points to caller-owned writable storage.
    unsafe { ptr::write_unaligned(out_len, transaction.bytes.len()) };
    clear_last_error();
    transaction.bytes.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_mutation_transaction_artifact_count(
    transaction: *mut c_void,
) -> usize {
    if transaction.is_null() {
        return 0;
    }
    // SAFETY: the caller supplies a live handle returned by this module.
    let transaction = unsafe { &*(transaction as *const EncodedMutationTransaction) };
    transaction.artifacts.len()
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_mutation_transaction_artifact_path(
    transaction: *mut c_void,
    index: usize,
) -> *const c_char {
    if transaction.is_null() {
        return ptr::null();
    }
    // SAFETY: the caller supplies a live handle returned by this module.
    let transaction = unsafe { &*(transaction as *const EncodedMutationTransaction) };
    transaction
        .artifacts
        .get(index)
        .map_or(ptr::null(), |artifact| artifact.path.as_ptr())
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_mutation_transaction_artifact_size(
    transaction: *mut c_void,
    index: usize,
) -> u64 {
    if transaction.is_null() {
        return 0;
    }
    // SAFETY: the caller supplies a live handle returned by this module.
    let transaction = unsafe { &*(transaction as *const EncodedMutationTransaction) };
    transaction
        .artifacts
        .get(index)
        .map_or(0, |artifact| artifact.size_bytes)
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_mutation_transaction_byte_count(
    transaction: *mut c_void,
) -> u64 {
    if transaction.is_null() {
        return 0;
    }
    // SAFETY: the caller supplies a live handle returned by this module.
    let transaction = unsafe { &*(transaction as *const EncodedMutationTransaction) };
    transaction.byte_count
}

#[no_mangle]
pub unsafe extern "C" fn lance_free_distributed_mutation_transaction(transaction: *mut c_void) {
    if !transaction.is_null() {
        // SAFETY: ownership of the handle is transferred back exactly once by the caller.
        unsafe {
            let _ = Box::from_raw(transaction as *mut EncodedMutationTransaction);
        }
    }
}

fn validate_source_fragment_metadata(
    dataset: &Dataset,
    inspected: &InspectedMutation,
    transaction: &Transaction,
) -> FfiResult<()> {
    let current_by_id: HashMap<_, _> = dataset
        .manifest()
        .fragments
        .iter()
        .map(|fragment| (fragment.id, fragment))
        .collect();
    let updated_fragments = match &transaction.operation {
        Operation::Delete {
            updated_fragments, ..
        }
        | Operation::Update {
            updated_fragments, ..
        } => updated_fragments,
        _ => return Err(mutation_error("unexpected mutation operation")),
    };
    for updated in updated_fragments {
        let current = current_by_id
            .get(&updated.id)
            .ok_or_else(|| mutation_error("mutation source fragment no longer exists"))?;
        let mut expected = (*current).clone();
        expected.deletion_file = updated.deletion_file.clone();
        if expected != *updated {
            return Err(mutation_error(
                "mutation worker changed source fragment state other than deletions",
            ));
        }
        if updated
            .deletion_file
            .as_ref()
            .is_some_and(|file| file.read_version != dataset.version_id())
        {
            return Err(mutation_error(
                "mutation deletion artifact was written from a stale version",
            ));
        }
    }
    for fragment_id in &inspected.source_fragment_ids {
        if !current_by_id.contains_key(fragment_id) {
            return Err(mutation_error(
                "mutation transaction references a missing source fragment",
            ));
        }
    }
    Ok(())
}

fn merge_transactions(
    dataset: &Dataset,
    transactions: Vec<Transaction>,
    mutation_kind: MutationKind,
    expected_version: u64,
    operation_id: &str,
    schema_fingerprint: &str,
    source_fragment_ids: &HashSet<u64>,
) -> FfiResult<Transaction> {
    let mut seen_transaction_ids = HashSet::new();
    let mut seen_source_fragments = HashSet::new();
    let mut total_rows = 0u64;
    let mut delete_updated = Vec::new();
    let mut delete_removed = Vec::new();
    let mut update_updated = Vec::new();
    let mut update_removed = Vec::new();
    let mut update_new = Vec::new();
    let mut update_fields = None::<Vec<u32>>;
    let mut next_fragment_id =
        dataset
            .manifest()
            .max_fragment_id()
            .map_or(Ok(0), |fragment_id| {
                fragment_id
                    .checked_add(1)
                    .ok_or_else(|| mutation_error("replacement fragment id overflow"))
            })?;

    for transaction in &transactions {
        if !seen_transaction_ids.insert(transaction.uuid.clone()) {
            return Err(mutation_error(
                "coordinator selected a duplicate mutation transaction",
            ));
        }
        let inspected = inspect_transaction(
            transaction,
            MutationValidation {
                mutation_kind,
                expected_version,
                operation_id,
                query_id: None,
                task_attempt_id: None,
                schema_fingerprint,
                source_fragment_ids,
            },
            None,
        )?;
        validate_source_fragment_metadata(dataset, &inspected, transaction)?;
        for fragment_id in &inspected.source_fragment_ids {
            if !seen_source_fragments.insert(*fragment_id) {
                return Err(mutation_error(
                    "selected mutation tasks overlap one source fragment",
                ));
            }
        }
        total_rows = total_rows
            .checked_add(inspected.row_count)
            .ok_or_else(|| mutation_error("mutation row count overflow"))?;
        match &transaction.operation {
            Operation::Delete {
                updated_fragments,
                deleted_fragment_ids,
                ..
            } => {
                delete_updated.extend(updated_fragments.clone());
                delete_removed.extend(deleted_fragment_ids.clone());
            }
            Operation::Update {
                removed_fragment_ids,
                updated_fragments,
                new_fragments,
                fields_for_preserving_frag_bitmap,
                ..
            } => {
                if update_fields
                    .as_ref()
                    .is_some_and(|fields| fields != fields_for_preserving_frag_bitmap)
                {
                    return Err(mutation_error(
                        "selected UPDATE tasks disagree on modified fields",
                    ));
                }
                update_fields.get_or_insert_with(|| fields_for_preserving_frag_bitmap.clone());
                update_removed.extend(removed_fragment_ids.clone());
                update_updated.extend(updated_fragments.clone());
                for fragment in new_fragments {
                    let mut fragment = fragment.clone();
                    fragment.id = next_fragment_id;
                    next_fragment_id = next_fragment_id
                        .checked_add(1)
                        .ok_or_else(|| mutation_error("replacement fragment id overflow"))?;
                    update_new.push(fragment);
                }
            }
            _ => unreachable!("inspect_transaction accepted the matching mutation operation"),
        }
    }

    let operation = match mutation_kind {
        MutationKind::Delete => Operation::Delete {
            updated_fragments: delete_updated,
            deleted_fragment_ids: delete_removed,
            predicate: format!("vane distributed mutation {operation_id}"),
        },
        MutationKind::Update => Operation::Update {
            removed_fragment_ids: update_removed,
            updated_fragments: update_updated,
            new_fragments: update_new,
            fields_modified: Vec::new(),
            merged_generations: Vec::new(),
            fields_for_preserving_frag_bitmap: update_fields.unwrap_or_default(),
            update_mode: Some(UpdateMode::RewriteRows),
            inserted_rows_filter: None,
            updated_fragment_offsets: None,
        },
    };
    let mut merged = Transaction::new(expected_version, operation, None);
    merged.uuid = operation_id.to_string();
    merged.transaction_properties = Some(Arc::new(HashMap::from([
        (OPERATION_PROPERTY.to_string(), operation_id.to_string()),
        (
            MUTATION_KIND_PROPERTY.to_string(),
            mutation_kind.token().to_string(),
        ),
        (ROW_COUNT_PROPERTY.to_string(), total_rows.to_string()),
        (
            SCHEMA_FINGERPRINT_PROPERTY.to_string(),
            schema_fingerprint.to_string(),
        ),
    ])));
    Ok(merged)
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub unsafe extern "C" fn lance_distributed_commit_mutation_transactions(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
    mutation_kind: u8,
    expected_version: u64,
    expected_generation: *const c_char,
    operation_id: *const c_char,
    schema_fingerprint: *const c_char,
    source_fragment_ids: *const u64,
    source_fragment_count: usize,
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
        // SAFETY: the caller provided a non-null commit phase marker.
        unsafe { ptr::write_unaligned(out_commit_started, 0) };
        if transaction_count == 0 {
            return Err(mutation_error("mutation commit requires transactions"));
        }
        let mutation_kind = MutationKind::parse(mutation_kind)?;
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let expected_generation =
            unsafe { cstr_to_str(expected_generation, "expected_generation")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let schema_fingerprint =
            unsafe { cstr_to_str(schema_fingerprint, "schema_fingerprint")? }.to_string();
        // SAFETY: the fragment-id array is validated against its element count.
        let source_fragment_ids = unsafe {
            slice_from_ptr(
                source_fragment_ids,
                source_fragment_count,
                "source_fragment_ids",
            )?
        };
        let source_fragment_ids: HashSet<_> = source_fragment_ids.iter().copied().collect();
        if source_fragment_ids.len() != source_fragment_count {
            return Err(mutation_error("mutation source fragments are duplicated"));
        }
        // SAFETY: the payload-pointer array is validated against transaction_count.
        let payloads =
            unsafe { slice_from_ptr(transaction_bytes, transaction_count, "transaction_bytes")? };
        // SAFETY: the payload-length array is validated against transaction_count.
        let lengths = unsafe {
            slice_from_ptr(
                transaction_lengths,
                transaction_count,
                "transaction_lengths",
            )?
        };
        // SAFETY: the storage-option arrays are validated as equal-length C strings.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
        // SAFETY: the optional session handle is validated before dereferencing.
        let session = unsafe { optional_session_handle(session)? };

        let (dataset, transactions) = match runtime::block_on(async {
            let dataset = open_dataset(path.as_str(), storage_options, session).await?;
            validate_generation(&dataset, expected_version, expected_generation.as_str()).await?;
            let mut transactions = Vec::with_capacity(transaction_count);
            for (payload, length) in payloads.iter().zip(lengths) {
                // SAFETY: every transaction pointer is validated with its paired length.
                let bytes = unsafe { slice_from_ptr(*payload, *length, "transaction payload")? };
                transactions.push(transaction_from_bytes(bytes)?);
            }
            Ok::<_, FfiError>((dataset, transactions))
        }) {
            Ok(result) => result?,
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        let merged = merge_transactions(
            &dataset,
            transactions,
            mutation_kind,
            expected_version,
            operation_id.as_str(),
            schema_fingerprint.as_str(),
            &source_fragment_ids,
        )?;
        let builder = CommitBuilder::new(Arc::new(dataset))
            .with_max_retries(0)
            .with_skip_auto_cleanup(true);
        // SAFETY: out_commit_started was validated above and remains writable.
        unsafe { ptr::write_unaligned(out_commit_started, 1) };
        let committed = match runtime::block_on(builder.execute(merged)) {
            Ok(Ok(dataset)) => dataset,
            Ok(Err(err)) => {
                return Err(mutation_error(format!(
                    "distributed mutation coordinator commit outcome is unknown: {err}"
                )))
            }
            Err(err) => {
                return Err(FfiError::new(
                    ErrorCode::Runtime,
                    format!("distributed mutation coordinator commit outcome is unknown: {err}"),
                ))
            }
        };
        if committed.version_id() != expected_version.saturating_add(1) {
            return Err(mutation_error(
                "mutation coordinator commit returned an unexpected version",
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

fn manifest_bytes(manifest: &MutationAttemptManifest) -> FfiResult<Vec<u8>> {
    let mut bytes = Vec::with_capacity(ATTEMPT_MANIFEST_MAGIC.len() + manifest.encoded_len());
    bytes.extend_from_slice(ATTEMPT_MANIFEST_MAGIC);
    manifest
        .encode(&mut bytes)
        .map_err(|err| mutation_error(format!("encode mutation attempt manifest: {err}")))?;
    Ok(bytes)
}

fn manifest_from_bytes(bytes: &[u8]) -> FfiResult<MutationAttemptManifest> {
    let payload = bytes
        .strip_prefix(ATTEMPT_MANIFEST_MAGIC)
        .ok_or_else(|| mutation_error("mutation attempt manifest has invalid magic"))?;
    MutationAttemptManifest::decode(payload)
        .map_err(|err| mutation_error(format!("decode mutation attempt manifest: {err}")))
}

fn validate_manifest(
    manifest: &MutationAttemptManifest,
    expected_operation_id: &str,
) -> FfiResult<(Vec<MutationArtifact>, String)> {
    let mutation_kind =
        MutationKind::parse(u8::try_from(manifest.mutation_kind).map_err(|_| {
            mutation_error("mutation attempt manifest has an invalid mutation kind")
        })?)?;
    if manifest.operation_id != expected_operation_id
        || !is_canonical_uuid(manifest.operation_id.as_str())
        || manifest.expected_version == 0
        || manifest.expected_generation.is_empty()
        || manifest.schema_fingerprint.len() != 32
        || manifest.transactions.is_empty()
    {
        return Err(mutation_error("mutation attempt manifest is invalid"));
    }
    validate_task_attempt_identity(
        manifest.query_id.as_str(),
        manifest.task_attempt_id.as_str(),
    )?;
    let source_fragment_ids: HashSet<_> = manifest.source_fragment_ids.iter().copied().collect();
    if source_fragment_ids.len() != manifest.source_fragment_ids.len() {
        return Err(mutation_error(
            "mutation manifest source fragments are duplicated",
        ));
    }
    let mut transaction_ids = HashSet::new();
    let mut artifact_paths = HashSet::new();
    let mut artifacts = Vec::new();
    let mut manifest_id = None;
    for bytes in &manifest.transactions {
        let transaction = transaction_from_bytes(bytes)?;
        if !transaction_ids.insert(transaction.uuid.clone()) {
            return Err(mutation_error(
                "mutation manifest contains duplicate transactions",
            ));
        }
        manifest_id.get_or_insert_with(|| transaction.uuid.clone());
        let inspected = inspect_transaction(
            &transaction,
            MutationValidation {
                mutation_kind,
                expected_version: manifest.expected_version,
                operation_id: manifest.operation_id.as_str(),
                query_id: Some(manifest.query_id.as_str()),
                task_attempt_id: Some(manifest.task_attempt_id.as_str()),
                schema_fingerprint: manifest.schema_fingerprint.as_str(),
                source_fragment_ids: &source_fragment_ids,
            },
            None,
        )?;
        for artifact in inspected.artifacts {
            let path = artifact.path.to_string_lossy().into_owned();
            if !artifact_paths.insert(path) {
                return Err(mutation_error(
                    "mutation manifest contains duplicate artifacts",
                ));
            }
            artifacts.push(artifact);
        }
    }
    Ok((
        artifacts,
        manifest_id.expect("non-empty manifest has a transaction"),
    ))
}

fn attempt_directory(dataset: &Dataset, operation_id: &str) -> FfiResult<object_store::path::Path> {
    if !is_canonical_uuid(operation_id) {
        return Err(mutation_error("mutation operation identity is invalid"));
    }
    let root = dataset
        .data_dir()
        .parent()
        .ok_or_else(|| mutation_error("mutation target has no object-store root"))?;
    Ok(root.join(ATTEMPT_MANIFEST_DIRECTORY).join(operation_id))
}

fn attempt_path(
    dataset: &Dataset,
    operation_id: &str,
    manifest_id: &str,
) -> FfiResult<object_store::path::Path> {
    if !is_canonical_uuid(manifest_id) {
        return Err(mutation_error("mutation manifest identity is invalid"));
    }
    Ok(attempt_directory(dataset, operation_id)?
        .join(format!("{manifest_id}{ATTEMPT_MANIFEST_SUFFIX}")))
}

async fn dataset_store(dataset: &Dataset) -> FfiResult<Arc<lance_io::object_store::ObjectStore>> {
    dataset
        .object_store(None)
        .await
        .map_err(|err| mutation_error(format!("resolve mutation object store: {err}")))
}

async fn publish_manifest(dataset: &Dataset, manifest: &MutationAttemptManifest) -> FfiResult<()> {
    let (_, manifest_id) = validate_manifest(manifest, manifest.operation_id.as_str())?;
    let bytes = manifest_bytes(manifest)?;
    let path = attempt_path(
        dataset,
        manifest.operation_id.as_str(),
        manifest_id.as_str(),
    )?;
    let store = dataset_store(dataset).await?;
    match store
        .inner
        .put_opts(&path, bytes.clone().into(), PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(()),
        Err(ObjectStoreError::AlreadyExists { .. }) => {
            let existing = store
                .read_one_all(&path)
                .await
                .map_err(|err| mutation_error(format!("read mutation manifest: {err}")))?;
            if existing.as_ref() != bytes.as_slice() {
                return Err(mutation_error(
                    "mutation attempt reused a manifest with different contents",
                ));
            }
            Ok(())
        }
        Err(err) => Err(mutation_error(format!("publish mutation manifest: {err}"))),
    }
}

fn live_artifact_paths(dataset: &Dataset) -> HashSet<String> {
    let mut paths = HashSet::new();
    for fragment in dataset.manifest().fragments.iter() {
        for file in &fragment.files {
            if file.base_id.is_none() {
                paths.insert(format!("data/{}", file.path));
            }
        }
        if let Some(deletion_file) = &fragment.deletion_file {
            if deletion_file.base_id.is_none() {
                paths.insert(relative_deletion_file_path(fragment.id, deletion_file));
            }
        }
        if let Some(RowIdMeta::External(file)) = &fragment.row_id_meta {
            paths.insert(file.path.clone());
        }
    }
    paths
}

async fn delete_if_present(
    store: &lance_io::object_store::ObjectStore,
    path: &object_store::path::Path,
) -> FfiResult<()> {
    match store.inner.delete(path).await {
        Ok(()) | Err(ObjectStoreError::NotFound { .. }) => Ok(()),
        Err(err) => Err(mutation_error(format!("delete mutation artifact: {err}"))),
    }
}

async fn cleanup_artifacts(
    dataset: &Dataset,
    store: &lance_io::object_store::ObjectStore,
    artifacts: &[MutationArtifact],
) -> FfiResult<()> {
    let live = live_artifact_paths(dataset);
    let root = dataset
        .data_dir()
        .parent()
        .ok_or_else(|| mutation_error("mutation target has no object-store root"))?;
    for artifact in artifacts {
        let relative = artifact.path.to_string_lossy();
        if live.contains(relative.as_ref()) {
            continue;
        }
        if !relative.starts_with("data/") && !relative.starts_with("_deletions/") {
            return Err(mutation_error(
                "mutation cleanup artifact is outside supported directories",
            ));
        }
        let path = join_relative_artifact_path(&root, relative.as_ref())?;
        delete_if_present(store, &path).await?;
        if let Some(file_name) = relative.strip_prefix("data/") {
            if let Some(stem) = std::path::Path::new(file_name)
                .file_stem()
                .and_then(|stem| stem.to_str())
            {
                let sidecar = dataset.data_dir().join(stem);
                if let Err(err) = store.remove_dir_all(sidecar).await {
                    if !err.is_not_found() {
                        return Err(mutation_error(format!(
                            "delete mutation data sidecars: {err}"
                        )));
                    }
                }
            }
        }
    }
    Ok(())
}

async fn load_manifests(
    dataset: &Dataset,
    operation_id: &str,
) -> FfiResult<(
    Arc<lance_io::object_store::ObjectStore>,
    Vec<LoadedAttemptManifest>,
)> {
    let store = dataset_store(dataset).await?;
    let directory = attempt_directory(dataset, operation_id)?;
    let objects = store
        .list(Some(directory.clone()))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|err| mutation_error(format!("list mutation manifests: {err}")))?;
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
            return Err(mutation_error(
                "mutation manifest directory contains an unexpected object",
            ));
        }
        let bytes = store
            .read_one_all(&object.location)
            .await
            .map_err(|err| mutation_error(format!("read mutation manifest: {err}")))?;
        let manifest = manifest_from_bytes(bytes.as_ref())?;
        let (artifacts, manifest_id) = validate_manifest(&manifest, operation_id)?;
        if object.location != attempt_path(dataset, operation_id, manifest_id.as_str())?
            || !task_attempt_ids.insert(manifest.task_attempt_id.clone())
        {
            return Err(mutation_error(
                "mutation manifest path or task identity is duplicated",
            ));
        }
        for artifact in &artifacts {
            let path = artifact.path.to_string_lossy().into_owned();
            if artifact_owners
                .insert(path, manifest.task_attempt_id.clone())
                .is_some()
            {
                return Err(mutation_error(
                    "mutation attempt manifests share an artifact path",
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

async fn cleanup_manifests(
    dataset: &Dataset,
    operation_id: &str,
    retained: &HashSet<String>,
) -> FfiResult<()> {
    let (store, manifests) = load_manifests(dataset, operation_id).await?;
    let present: HashSet<_> = manifests
        .iter()
        .map(|manifest| manifest.task_attempt_id.as_str())
        .collect();
    if retained
        .iter()
        .any(|task_attempt_id| !present.contains(task_attempt_id.as_str()))
    {
        return Err(mutation_error(
            "selected mutation attempt has no cleanup manifest",
        ));
    }
    for manifest in manifests {
        if retained.contains(&manifest.task_attempt_id) {
            continue;
        }
        cleanup_artifacts(dataset, store.as_ref(), &manifest.artifacts).await?;
        delete_if_present(store.as_ref(), &manifest.path).await?;
    }
    Ok(())
}

async fn release_manifests(
    dataset: &Dataset,
    operation_id: &str,
    released: &HashSet<String>,
) -> FfiResult<()> {
    if released.is_empty() {
        return Err(mutation_error("mutation manifest release is empty"));
    }
    let (store, manifests) = load_manifests(dataset, operation_id).await?;
    if manifests
        .iter()
        .any(|manifest| !released.contains(&manifest.task_attempt_id))
    {
        return Err(mutation_error(
            "mutation manifest release found an unselected attempt",
        ));
    }
    for manifest in manifests {
        delete_if_present(store.as_ref(), &manifest.path).await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[no_mangle]
pub unsafe extern "C" fn lance_distributed_publish_mutation_attempt_manifest(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    mutation_kind: u8,
    expected_version: u64,
    expected_generation: *const c_char,
    operation_id: *const c_char,
    query_id: *const c_char,
    task_attempt_id: *const c_char,
    schema_fingerprint: *const c_char,
    source_fragment_ids: *const u64,
    source_fragment_count: usize,
    transaction_bytes: *const *const u8,
    transaction_lengths: *const usize,
    transaction_count: usize,
) -> i32 {
    let result = (|| {
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let path = unsafe { cstr_to_str(path, "path")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let expected_generation =
            unsafe { cstr_to_str(expected_generation, "expected_generation")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let query_id = unsafe { cstr_to_str(query_id, "query_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let task_attempt_id =
            unsafe { cstr_to_str(task_attempt_id, "task_attempt_id")? }.to_string();
        // SAFETY: C string pointers are validated by cstr_to_str before use.
        let schema_fingerprint =
            unsafe { cstr_to_str(schema_fingerprint, "schema_fingerprint")? }.to_string();
        // SAFETY: the fragment-id array is validated against its element count.
        let source_fragment_ids = unsafe {
            slice_from_ptr(
                source_fragment_ids,
                source_fragment_count,
                "source_fragment_ids",
            )?
        }
        .to_vec();
        // SAFETY: the payload-pointer array is validated against transaction_count.
        let payloads =
            unsafe { slice_from_ptr(transaction_bytes, transaction_count, "transaction_bytes")? };
        // SAFETY: the payload-length array is validated against transaction_count.
        let lengths = unsafe {
            slice_from_ptr(
                transaction_lengths,
                transaction_count,
                "transaction_lengths",
            )?
        };
        let mut transactions = Vec::with_capacity(transaction_count);
        for (payload, length) in payloads.iter().zip(lengths) {
            // SAFETY: every transaction pointer is validated with its paired length.
            transactions.push(
                unsafe { slice_from_ptr(*payload, *length, "transaction payload")? }.to_vec(),
            );
        }
        let manifest = MutationAttemptManifest {
            operation_id,
            query_id,
            task_attempt_id,
            expected_version,
            mutation_kind: u32::from(mutation_kind),
            expected_generation,
            schema_fingerprint,
            source_fragment_ids,
            transactions,
        };
        validate_manifest(&manifest, manifest.operation_id.as_str())?;
        // SAFETY: the storage-option arrays are validated as equal-length C strings.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
        match runtime::block_on(async {
            let dataset = open_dataset(path.as_str(), storage_options, None).await?;
            publish_manifest(&dataset, &manifest).await
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

// This adapter intentionally mirrors the raw storage and attempt arguments of
// the two exported manifest lifecycle entry points below.
#[allow(clippy::too_many_arguments)]
unsafe fn mutation_manifest_action(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    operation_id: *const c_char,
    task_attempt_ids: *const *const c_char,
    task_attempt_count: usize,
    release: bool,
) -> FfiResult<()> {
    // SAFETY: C string pointers are validated by cstr_to_str before use.
    let path = unsafe { cstr_to_str(path, "path")? }.to_string();
    // SAFETY: C string pointers are validated by cstr_to_str before use.
    let operation_id = unsafe { cstr_to_str(operation_id, "operation_id")? }.to_string();
    // SAFETY: optional_cstr_array validates every pointer before conversion.
    let task_attempt_ids =
        unsafe { optional_cstr_array(task_attempt_ids, task_attempt_count, "task_attempt_ids")? };
    let task_attempt_ids: HashSet<_> = task_attempt_ids.into_iter().collect();
    if task_attempt_ids.len() != task_attempt_count {
        return Err(mutation_error(
            "mutation manifest task identities are duplicated",
        ));
    }
    // SAFETY: the storage-option arrays are validated as equal-length C strings.
    let storage_options =
        unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
    match runtime::block_on(async {
        let dataset = open_dataset(path.as_str(), storage_options, None).await?;
        if release {
            release_manifests(&dataset, operation_id.as_str(), &task_attempt_ids).await
        } else {
            cleanup_manifests(&dataset, operation_id.as_str(), &task_attempt_ids).await
        }
    }) {
        Ok(result) => result,
        Err(err) => Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_cleanup_mutation_attempt_manifests(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    operation_id: *const c_char,
    retained_task_attempt_ids: *const *const c_char,
    retained_task_attempt_count: usize,
) -> i32 {
    // SAFETY: this wrapper forwards the same validated raw FFI arguments.
    let result = unsafe {
        mutation_manifest_action(
            path,
            option_keys,
            option_values,
            options_len,
            operation_id,
            retained_task_attempt_ids,
            retained_task_attempt_count,
            false,
        )
    };
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

#[no_mangle]
pub unsafe extern "C" fn lance_distributed_release_mutation_attempt_manifests(
    path: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    operation_id: *const c_char,
    released_task_attempt_ids: *const *const c_char,
    released_task_attempt_count: usize,
) -> i32 {
    // SAFETY: this wrapper forwards the same validated raw FFI arguments.
    let result = unsafe {
        mutation_manifest_action(
            path,
            option_keys,
            option_values,
            options_len,
            operation_id,
            released_task_attempt_ids,
            released_task_attempt_count,
            true,
        )
    };
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
pub unsafe extern "C" fn lance_distributed_cleanup_mutation_transaction(
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
        // SAFETY: the transaction payload pointer is validated with its length.
        let bytes = unsafe { slice_from_ptr(bytes, bytes_len, "transaction bytes")? };
        let transaction = transaction_from_bytes(bytes)?;
        let mutation_kind = match &transaction.operation {
            Operation::Delete { .. } => MutationKind::Delete,
            Operation::Update { .. } => MutationKind::Update,
            _ => return Err(mutation_error("cleanup requires a mutation transaction")),
        };
        let schema_fingerprint =
            validate_property(&transaction, SCHEMA_FINGERPRINT_PROPERTY, None)?.to_string();
        let source_fragment_ids: HashSet<_> = match &transaction.operation {
            Operation::Delete {
                updated_fragments,
                deleted_fragment_ids,
                ..
            } => updated_fragments
                .iter()
                .map(|fragment| fragment.id)
                .chain(deleted_fragment_ids.iter().copied())
                .collect(),
            Operation::Update {
                updated_fragments,
                removed_fragment_ids,
                ..
            } => updated_fragments
                .iter()
                .map(|fragment| fragment.id)
                .chain(removed_fragment_ids.iter().copied())
                .collect(),
            _ => unreachable!(),
        };
        let inspected = inspect_transaction(
            &transaction,
            MutationValidation {
                mutation_kind,
                expected_version: transaction.read_version,
                operation_id: operation_id.as_str(),
                query_id: None,
                task_attempt_id: None,
                schema_fingerprint: schema_fingerprint.as_str(),
                source_fragment_ids: &source_fragment_ids,
            },
            None,
        )?;
        // SAFETY: the storage-option arrays are validated as equal-length C strings.
        let storage_options =
            unsafe { distributed_storage_options(option_keys, option_values, options_len)? };
        match runtime::block_on(async {
            let dataset = open_dataset(path.as_str(), storage_options, None).await?;
            let store = dataset_store(&dataset).await?;
            cleanup_artifacts(&dataset, store.as_ref(), &inspected.artifacts).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};

    const OPERATION_ID: &str = "00000000-0000-4000-8000-000000000001";
    const QUERY_ID: &str = "query";
    const SCHEMA_FINGERPRINT: &str = "00000000000000000000000000000000";

    fn dataset() -> Dataset {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let uri = format!("memory://mutation-test-{}", rand::random::<u64>());
        runtime::block_on(Dataset::write(reader, uri.as_str(), None))
            .unwrap()
            .unwrap()
    }

    fn delete_transaction(
        dataset: &Dataset,
        row_offset: u64,
        task_attempt_id: &str,
    ) -> Transaction {
        let row_addrs = RoaringTreemap::from_iter([row_offset]);
        let properties = transaction_properties(
            MutationKind::Delete,
            OPERATION_ID,
            QUERY_ID,
            task_attempt_id,
            SCHEMA_FINGERPRINT,
            1,
        );
        runtime::block_on(create_delete_transaction(dataset, &row_addrs, properties))
            .unwrap()
            .unwrap()
    }

    fn attempt_manifest(
        transaction: &Transaction,
        task_attempt_id: &str,
    ) -> MutationAttemptManifest {
        MutationAttemptManifest {
            operation_id: OPERATION_ID.to_string(),
            query_id: QUERY_ID.to_string(),
            task_attempt_id: task_attempt_id.to_string(),
            expected_version: transaction.read_version,
            mutation_kind: u32::from(MutationKind::Delete as u8),
            expected_generation: "snapshot|test".to_string(),
            schema_fingerprint: SCHEMA_FINGERPRINT.to_string(),
            source_fragment_ids: vec![0],
            transactions: vec![transaction_to_bytes(transaction)],
        }
    }

    fn artifact_path(dataset: &Dataset, transaction: &Transaction) -> ObjectStorePath {
        let inspected = inspect_transaction(
            transaction,
            MutationValidation {
                mutation_kind: MutationKind::Delete,
                expected_version: dataset.version_id(),
                operation_id: OPERATION_ID,
                query_id: Some(QUERY_ID),
                task_attempt_id: Some(
                    validate_property(transaction, TASK_ATTEMPT_PROPERTY, None).unwrap(),
                ),
                schema_fingerprint: SCHEMA_FINGERPRINT,
                source_fragment_ids: &HashSet::from([0]),
            },
            Some(1),
        )
        .unwrap();
        assert_eq!(inspected.artifacts.len(), 1);
        let root = dataset.data_dir().parent().unwrap();
        join_relative_artifact_path(&root, inspected.artifacts[0].path.to_str().unwrap()).unwrap()
    }

    #[test]
    fn commit_marker_remains_clear_for_pre_commit_failure() {
        let mut commit_started = 1;
        // SAFETY: the zero transaction count makes all input pointers
        // unreachable after the writable marker is initialized.
        let result = unsafe {
            lance_distributed_commit_mutation_transactions(
                ptr::null(),
                ptr::null(),
                ptr::null(),
                0,
                ptr::null_mut(),
                MutationKind::Delete as u8,
                0,
                ptr::null(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                0,
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
    fn duplicate_and_overlapping_selected_mutations_are_rejected() {
        let dataset = dataset();
        let first = delete_transaction(&dataset, 0, "query.0.0.0");
        let source_fragments = HashSet::from([0]);
        assert!(merge_transactions(
            &dataset,
            vec![first.clone(), first],
            MutationKind::Delete,
            dataset.version_id(),
            OPERATION_ID,
            SCHEMA_FINGERPRINT,
            &source_fragments,
        )
        .is_err());

        let first = delete_transaction(&dataset, 0, "query.0.0.0");
        let retry = delete_transaction(&dataset, 1, "query.0.0.1");
        assert!(merge_transactions(
            &dataset,
            vec![first, retry],
            MutationKind::Delete,
            dataset.version_id(),
            OPERATION_ID,
            SCHEMA_FINGERPRINT,
            &source_fragments,
        )
        .is_err());
    }

    #[test]
    fn retry_loser_cleanup_preserves_only_the_selected_attempt() {
        let dataset = dataset();
        let selected_attempt = "query.0.0.0";
        let loser_attempt = "query.0.0.1";
        let selected = delete_transaction(&dataset, 0, selected_attempt);
        let loser = delete_transaction(&dataset, 1, loser_attempt);
        let selected_artifact = artifact_path(&dataset, &selected);
        let loser_artifact = artifact_path(&dataset, &loser);
        let selected_manifest = attempt_manifest(&selected, selected_attempt);
        let loser_manifest = attempt_manifest(&loser, loser_attempt);
        runtime::block_on(publish_manifest(&dataset, &selected_manifest))
            .unwrap()
            .unwrap();
        runtime::block_on(publish_manifest(&dataset, &loser_manifest))
            .unwrap()
            .unwrap();

        let store = runtime::block_on(dataset_store(&dataset)).unwrap().unwrap();
        assert!(runtime::block_on(store.exists(&selected_artifact))
            .unwrap()
            .unwrap());
        assert!(runtime::block_on(store.exists(&loser_artifact))
            .unwrap()
            .unwrap());

        let retained = HashSet::from([selected_attempt.to_string()]);
        runtime::block_on(cleanup_manifests(&dataset, OPERATION_ID, &retained))
            .unwrap()
            .unwrap();
        assert!(runtime::block_on(store.exists(&selected_artifact))
            .unwrap()
            .unwrap());
        assert!(!runtime::block_on(store.exists(&loser_artifact))
            .unwrap()
            .unwrap());

        runtime::block_on(release_manifests(&dataset, OPERATION_ID, &retained))
            .unwrap()
            .unwrap();
        assert!(runtime::block_on(store.exists(&selected_artifact))
            .unwrap()
            .unwrap());
    }
}
