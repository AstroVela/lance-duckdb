use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr};
use std::ptr;
use std::sync::Arc;

use arrow::compute;
use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use lance::dataset::transaction::{Operation, Transaction, UpdateMap, UpdateMapEntry};
use lance::dataset::{BatchUDF, CommitBuilder, NewColumnTransform};
use lance::Dataset;
use lance_table::io::commit::ManifestNamingScheme;

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;

use super::schema_evolution::{parse_arrow_schema, parse_batch_size_from_config};
use super::session::record_commit;
use super::types::DatasetHandle;
use super::util::{cstr_to_str, optional_cstr_array, FfiError, FfiResult};

/// A transaction-local Lance snapshot.
///
/// Each staged operation is written as a detached Lance commit. Detached
/// manifests are readable by this DuckDB transaction but are not visible as
/// the latest dataset version. Commit promotes the final detached manifest in
/// one Restore operation; rollback only drops this handle.
pub(crate) struct DatasetTransactionHandle {
    base_version: u64,
    working: Arc<Dataset>,
    detached_versions: Vec<u64>,
    published: bool,
}

fn transaction_handle<'a>(ptr: *mut c_void) -> FfiResult<&'a DatasetTransactionHandle> {
    if ptr.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "dataset transaction is null",
        ));
    }
    // SAFETY: The pointer is created by `lance_dataset_transaction_new` and
    // remains owned by the C++ transaction manager for the duration of this
    // call.
    Ok(unsafe { &*(ptr as *const DatasetTransactionHandle) })
}

fn transaction_handle_mut<'a>(ptr: *mut c_void) -> FfiResult<&'a mut DatasetTransactionHandle> {
    if ptr.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "dataset transaction is null",
        ));
    }
    // SAFETY: The pointer is created by `lance_dataset_transaction_new`, and
    // DuckDB serializes mutation of a connection-local transaction handle.
    Ok(unsafe { &mut *(ptr as *mut DatasetTransactionHandle) })
}

async fn stage_transaction(
    handle: &mut DatasetTransactionHandle,
    transaction: Transaction,
) -> FfiResult<()> {
    let working_version = handle.working.version_id();
    if transaction.read_version != working_version {
        return Err(FfiError::new(
            ErrorCode::DatasetCommitTransaction,
            format!(
                "transaction read version {} does not match transaction-local Lance version {}",
                transaction.read_version, working_version
            ),
        ));
    }

    let dataset = CommitBuilder::new(handle.working.clone())
        .with_detached(true)
        .execute(transaction)
        .await
        .map_err(|err| {
            FfiError::new(
                ErrorCode::DatasetCommitTransaction,
                format!("stage detached Lance transaction: {err}"),
            )
        })?;
    handle.detached_versions.push(dataset.version_id());
    handle.working = Arc::new(dataset);
    Ok(())
}

enum PreparedNewColumnTransform {
    AllNulls {
        output_schema: Arc<ArrowSchema>,
    },
    BatchUdf {
        output_schema: Arc<ArrowSchema>,
        physical: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        read_columns: Vec<String>,
    },
}

impl PreparedNewColumnTransform {
    fn build(&self) -> (NewColumnTransform, Option<Vec<String>>) {
        match self {
            Self::AllNulls { output_schema } => {
                (NewColumnTransform::AllNulls(output_schema.clone()), None)
            }
            Self::BatchUdf {
                output_schema,
                physical,
                read_columns,
            } => {
                let output_fields = output_schema.fields().to_vec();
                let schema_ref = output_schema.clone();
                let physical = physical.clone();
                let mapper = move |batch: &RecordBatch| {
                    let num_rows = batch.num_rows();
                    let mut arrays = Vec::with_capacity(physical.len());
                    for (idx, (field, expr)) in
                        output_fields.iter().zip(physical.iter()).enumerate()
                    {
                        let arr = expr
                            .evaluate(batch)
                            .map_err(|err| {
                                lance::Error::invalid_input(format!(
                                    "expression[{idx}] evaluate: {err}"
                                ))
                            })?
                            .into_array(num_rows)
                            .map_err(|err| {
                                lance::Error::invalid_input(format!(
                                    "expression[{idx}] into_array: {err}"
                                ))
                            })?;
                        let arr = if arr.data_type() != field.data_type() {
                            compute::cast(&arr, field.data_type()).map_err(|err| {
                                lance::Error::invalid_input(format!(
                                    "expression[{idx}] cast: {err}"
                                ))
                            })?
                        } else {
                            arr
                        };
                        arrays.push(arr);
                    }
                    RecordBatch::try_new(schema_ref.clone(), arrays)
                        .map_err(|err| lance::Error::invalid_input(format!("output batch: {err}")))
                };

                (
                    NewColumnTransform::BatchUDF(BatchUDF {
                        mapper: Box::new(mapper),
                        output_schema: output_schema.clone(),
                        result_checkpoint: None,
                    }),
                    Some(read_columns.clone()),
                )
            }
        }
    }
}

fn prepare_new_column_transform(
    dataset: &Dataset,
    output_schema: Arc<ArrowSchema>,
    expressions: &[String],
) -> FfiResult<PreparedNewColumnTransform> {
    if expressions.is_empty() {
        return Ok(PreparedNewColumnTransform::AllNulls { output_schema });
    }
    if expressions.len() != output_schema.fields().len() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "expressions_len must match new_columns_schema field count",
        ));
    }

    let full_read_schema = Arc::new(ArrowSchema::from(dataset.schema()));
    let planner = lance::io::exec::Planner::new(full_read_schema);
    let parsed = expressions
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            let expr = planner.parse_expr(expr).map_err(|err| {
                FfiError::new(
                    ErrorCode::DatasetAddColumns,
                    format!("expression[{idx}] parse: {err}"),
                )
            })?;
            planner.optimize_expr(expr).map_err(|err| {
                FfiError::new(
                    ErrorCode::DatasetAddColumns,
                    format!("expression[{idx}] optimize: {err}"),
                )
            })
        })
        .collect::<FfiResult<Vec<_>>>()?;

    let needed_columns = parsed
        .iter()
        .flat_map(lance::io::exec::Planner::column_names_in_expr)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let read_schema = dataset.schema().project(&needed_columns).map_err(|err| {
        FfiError::new(
            ErrorCode::DatasetAddColumns,
            format!("read schema project: {err}"),
        )
    })?;
    let read_schema = Arc::new(ArrowSchema::from(&read_schema));
    let planner = lance::io::exec::Planner::new(read_schema.clone());
    let physical = parsed
        .into_iter()
        .enumerate()
        .map(|(idx, expr)| {
            planner.create_physical_expr(&expr).map_err(|err| {
                FfiError::new(
                    ErrorCode::DatasetAddColumns,
                    format!("expression[{idx}] physical: {err}"),
                )
            })
        })
        .collect::<FfiResult<Vec<_>>>()?;

    let read_columns = read_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    Ok(PreparedNewColumnTransform::BatchUdf {
        output_schema,
        physical,
        read_columns,
    })
}

async fn build_add_columns_transaction(
    dataset: &Dataset,
    output_schema: Arc<ArrowSchema>,
    expressions: &[String],
    batch_size: Option<u32>,
) -> FfiResult<Transaction> {
    let fragments = dataset.get_fragments();
    let mut updated_fragments = Vec::with_capacity(fragments.len());
    let mut merged_schema = None;
    let prepared = prepare_new_column_transform(dataset, output_schema.clone(), expressions)?;

    for fragment in fragments {
        let (transform, read_columns) = prepared.build();
        let (updated, schema) = fragment
            .add_columns(transform, read_columns, batch_size)
            .await
            .map_err(|err| {
                FfiError::new(
                    ErrorCode::DatasetAddColumns,
                    format!("prepare add_columns: {err}"),
                )
            })?;
        updated_fragments.push(updated);
        merged_schema = Some(schema);
    }

    let schema = if let Some(schema) = merged_schema {
        schema
    } else {
        // FileFragment::add_columns normally constructs this schema. Empty
        // datasets have no fragments, so perform the same metadata-only merge.
        let mut schema = dataset
            .schema()
            .merge(output_schema.as_ref())
            .map_err(|err| {
                FfiError::new(
                    ErrorCode::DatasetAddColumns,
                    format!("add_columns schema merge: {err}"),
                )
            })?;
        schema.set_field_id(Some(dataset.manifest.max_field_id()));
        schema
    };

    Ok(Transaction::new(
        dataset.version_id(),
        Operation::Merge {
            fragments: updated_fragments,
            schema,
        },
        None,
    ))
}

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_transaction_new(dataset: *mut c_void) -> *mut c_void {
    let result = (|| {
        // SAFETY: The caller passes a live `DatasetHandle` allocated by this
        // FFI library and retains ownership of it.
        let dataset = unsafe { super::util::dataset_handle(dataset)? };
        Ok::<_, FfiError>(DatasetTransactionHandle {
            base_version: dataset.dataset.version_id(),
            working: dataset.dataset.clone(),
            detached_versions: Vec::new(),
            published: false,
        })
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
pub unsafe extern "C" fn lance_dataset_transaction_open(transaction: *mut c_void) -> *mut c_void {
    match transaction_handle(transaction) {
        Ok(handle) => {
            clear_last_error();
            Box::into_raw(Box::new(DatasetHandle::new(handle.working.clone()))) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_transaction_stage(
    dataset_transaction: *mut c_void,
    transaction: *mut c_void,
) -> i32 {
    if transaction.is_null() {
        set_last_error(ErrorCode::InvalidArgument, "transaction is null");
        return -1;
    }
    // The raw Lance transaction is transferred to this function on every path.
    // SAFETY: Ownership of the transaction is explicitly transferred to this
    // function by the C++ caller, including on the error path.
    let transaction = unsafe { Box::from_raw(transaction as *mut Transaction) };
    let result = transaction_handle_mut(dataset_transaction).and_then(|handle| {
        match runtime::block_on(stage_transaction(handle, *transaction)) {
            Ok(result) => result,
            Err(err) => Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        }
    });
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
pub unsafe extern "C" fn lance_dataset_transaction_add_columns(
    dataset_transaction: *mut c_void,
    new_columns_schema: *const c_void,
    expressions: *const *const c_char,
    expressions_len: usize,
    batch_size: u32,
) -> i32 {
    let result = (|| {
        let handle = transaction_handle_mut(dataset_transaction)?;
        let output_schema = parse_arrow_schema(new_columns_schema, "new_columns_schema")?;
        if output_schema.fields().is_empty() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                "new_columns_schema must have at least one field",
            ));
        }
        // SAFETY: The caller guarantees that the pointer array and each string
        // remain valid for this synchronous FFI call.
        let expressions =
            unsafe { optional_cstr_array(expressions, expressions_len, "expressions")? };
        let batch_size = if batch_size == 0 {
            parse_batch_size_from_config(handle.working.as_ref())
        } else {
            Some(batch_size)
        };
        let transaction = match runtime::block_on(build_add_columns_transaction(
            handle.working.as_ref(),
            output_schema,
            &expressions,
            batch_size,
        )) {
            Ok(result) => result?,
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        match runtime::block_on(stage_transaction(handle, transaction)) {
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

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_transaction_update_field_metadata(
    dataset_transaction: *mut c_void,
    field_path: *const c_char,
    key: *const c_char,
    value: *const c_char,
) -> i32 {
    let result = (|| {
        let handle = transaction_handle_mut(dataset_transaction)?;
        // SAFETY: Both required strings are owned by the caller and remain
        // valid for this synchronous FFI call.
        let field_path = unsafe { cstr_to_str(field_path, "field_path")? };
        // SAFETY: See the pointer lifetime contract above.
        let key = unsafe { cstr_to_str(key, "key")? }.to_string();
        let value = if value.is_null() {
            None
        } else {
            Some(
                // SAFETY: A non-null optional value is a NUL-terminated string
                // that remains valid for this synchronous FFI call.
                unsafe { CStr::from_ptr(value) }
                    .to_str()
                    .map_err(|err| FfiError::new(ErrorCode::Utf8, format!("value utf8: {err}")))?
                    .to_string(),
            )
        };
        let field = handle.working.schema().field(field_path).ok_or_else(|| {
            FfiError::new(
                ErrorCode::DatasetUpdateFieldMetadata,
                format!("field not found: '{field_path}'"),
            )
        })?;
        let field_metadata_updates = HashMap::from([(
            field.id,
            UpdateMap {
                update_entries: vec![UpdateMapEntry { key, value }],
                replace: false,
            },
        )]);
        let transaction = Transaction::new(
            handle.working.version_id(),
            Operation::UpdateConfig {
                config_updates: None,
                table_metadata_updates: None,
                schema_metadata_updates: None,
                field_metadata_updates,
            },
            None,
        );
        match runtime::block_on(stage_transaction(handle, transaction)) {
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

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_transaction_update_table_metadata(
    dataset_transaction: *mut c_void,
    key: *const c_char,
    value: *const c_char,
) -> i32 {
    let result = (|| {
        let handle = transaction_handle_mut(dataset_transaction)?;
        // SAFETY: The required key is owned by the caller and remains valid
        // for this synchronous FFI call.
        let key = unsafe { cstr_to_str(key, "key")? }.to_string();
        let value = if value.is_null() {
            None
        } else {
            Some(
                // SAFETY: A non-null optional value is a NUL-terminated string
                // that remains valid for this synchronous FFI call.
                unsafe { CStr::from_ptr(value) }
                    .to_str()
                    .map_err(|err| FfiError::new(ErrorCode::Utf8, format!("value utf8: {err}")))?
                    .to_string(),
            )
        };
        let transaction = Transaction::new(
            handle.working.version_id(),
            Operation::UpdateConfig {
                config_updates: None,
                table_metadata_updates: Some(UpdateMap {
                    update_entries: vec![UpdateMapEntry { key, value }],
                    replace: false,
                }),
                schema_metadata_updates: None,
                field_metadata_updates: HashMap::new(),
            },
            None,
        );
        match runtime::block_on(stage_transaction(handle, transaction)) {
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

fn commit_dataset_transaction(handle: &mut DatasetTransactionHandle) -> FfiResult<()> {
    if handle.published {
        return Err(FfiError::new(
            ErrorCode::DatasetCommitTransaction,
            "transaction-local Lance snapshot is already published",
        ));
    }
    if handle.working.version_id() == handle.base_version {
        return Ok(());
    }
    let transaction = Transaction::new(
        handle.base_version,
        Operation::Restore {
            version: handle.working.version_id(),
        },
        None,
    );
    let latest_version = match runtime::block_on(handle.working.latest_version_id()) {
        Ok(Ok(version)) => version,
        Ok(Err(err)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetCommitTransaction,
                format!("resolve latest Lance version before commit: {err}"),
            ))
        }
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };
    if latest_version != handle.base_version {
        return Err(FfiError::new(
            ErrorCode::DatasetCommitTransaction,
            format!(
                "Lance dataset changed concurrently (transaction started at version {}, latest is {})",
                handle.base_version, latest_version
            ),
        ));
    }
    match runtime::block_on(
        CommitBuilder::new(handle.working.clone())
            .with_max_retries(0)
            .execute(transaction),
    ) {
        Ok(Ok(_)) => {
            record_commit();
            handle.published = true;
            Ok(())
        }
        Ok(Err(err)) => Err(FfiError::new(
            ErrorCode::DatasetCommitTransaction,
            format!("publish transaction-local Lance snapshot: {err}"),
        )),
        Err(err) => Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_dataset_transaction_commit(dataset_transaction: *mut c_void) -> i32 {
    let result = transaction_handle_mut(dataset_transaction).and_then(commit_dataset_transaction);

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
pub unsafe extern "C" fn lance_dataset_transaction_free(dataset_transaction: *mut c_void) {
    if !dataset_transaction.is_null() {
        // SAFETY: This function is the unique owner-reclaiming counterpart to
        // `lance_dataset_transaction_new`.
        let handle = unsafe { Box::from_raw(dataset_transaction as *mut DatasetTransactionHandle) };
        // Detached manifests are only transaction staging pointers. Once the
        // final snapshot has been published (or the DuckDB transaction rolls
        // back), remove them so they do not accumulate as visible artifacts.
        // Data files are retained when referenced by the published manifest;
        // unreferenced files remain eligible for Lance's normal cleanup.
        let _ = runtime::block_on(async {
            let object_store = handle.working.object_store(None).await?;
            let base = handle.working.branch_location().path;
            for version in handle.detached_versions {
                let path = ManifestNamingScheme::V2.manifest_path(&base, version);
                let _ = object_store.delete(&path).await;
            }
            Ok::<(), lance::Error>(())
        });
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use arrow_array::{Int32Array, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::WriteParams;

    use super::*;

    #[test]
    fn commit_publishes_the_staged_snapshot() {
        let dataset_dir =
            std::env::temp_dir().join(format!("ffi-transaction-{}", rand::random::<u64>()));
        let uri = dataset_dir.to_string_lossy().to_string();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let reader = RecordBatchIterator::new(
            vec![Ok(RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap())]
            .into_iter(),
            schema,
        );
        let dataset = runtime::block_on(Dataset::write(reader, &uri, Some(WriteParams::default())))
            .unwrap()
            .unwrap();
        let base_version = dataset.version_id();
        let mut handle = DatasetTransactionHandle {
            base_version,
            working: Arc::new(dataset),
            detached_versions: Vec::new(),
            published: false,
        };
        let transaction = Transaction::new(
            base_version,
            Operation::UpdateConfig {
                config_updates: None,
                table_metadata_updates: Some(UpdateMap {
                    update_entries: vec![UpdateMapEntry {
                        key: "transaction-test".to_string(),
                        value: Some("published".to_string()),
                    }],
                    replace: false,
                }),
                schema_metadata_updates: None,
                field_metadata_updates: HashMap::new(),
            },
            None,
        );
        runtime::block_on(stage_transaction(&mut handle, transaction))
            .unwrap()
            .unwrap();

        let handle_ptr = Box::into_raw(Box::new(handle)) as *mut c_void;
        unsafe {
            assert_eq!(lance_dataset_transaction_commit(handle_ptr), 0);
        }
        let published = runtime::block_on(Dataset::open(&uri)).unwrap().unwrap();
        assert_eq!(published.version_id(), base_version + 1);
        assert_eq!(
            published.manifest().table_metadata.get("transaction-test"),
            Some(&"published".to_string())
        );

        unsafe {
            lance_dataset_transaction_free(handle_ptr);
        }
        let _ = fs::remove_dir_all(dataset_dir);
    }
}
