use arrow::array::{
    Array, ArrayRef, Int64Array, LargeBinaryArray, RecordBatch, RecordBatchIterator, StringArray,
    StructArray, UInt64Array,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use lance::dataset::{WriteMode, WriteParams};
use lance_core::datatypes::BLOB_V2_USER_TYPE;
use lance_io::object_store::ObjectStoreRegistry;
use lance_table::format::BasePath;
use std::collections::HashMap;
use std::ffi::CStr;
use std::sync::atomic::Ordering;

use super::*;
use crate::error::{lance_free_string, lance_last_error_code, lance_last_error_message};
use crate::ffi::blob_store::{
    lance_blob_access_create, lance_blob_access_free, METADATA, PROVIDER, READ,
};
use crate::ffi::blob_test_support::{assert_released, Access, TempDir};
use crate::ffi::dataset::{
    lance_close_dataset, lance_dataset_checkout_version, lance_dataset_generation_id,
    lance_dataset_version,
};

// These FFI opens contribute to the existing process-wide diagnostics.
fn counter_guard() -> std::sync::MutexGuard<'static, ()> {
    crate::ffi::session::DEBUG_COUNTER_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Fixture {
    _directory: TempDir,
    uri: CString,
    batch: RecordBatch,
    expected: Vec<Option<Vec<u8>>>,
    external: std::path::PathBuf,
}

impl Fixture {
    fn new(stable: bool, alternate_base: bool) -> Self {
        Self::build(stable, alternate_base, true)
    }

    fn build(stable: bool, alternate_base: bool, include_nested: bool) -> Self {
        let directory = TempDir::new();
        let dataset_path = directory.0.join("table.lance");
        let external_dir = directory.0.join("external");
        std::fs::create_dir_all(&external_dir).unwrap();
        let external = external_dir.join("object.bin");
        let external_bytes = b"PREFIX!external-slice-data!SUFFIX".to_vec();
        std::fs::write(&external, &external_bytes).unwrap();
        let external_uri = url::Url::from_file_path(&external).unwrap().to_string();
        let expected = vec![
            Some(b"abc".to_vec()),
            Some((0..64).collect()),
            Some((64..144).collect()),
            Some((0..128 * 1024).map(|index| (index % 251) as u8).collect()),
            Some(external_bytes[7..26].to_vec()),
            Some(external_bytes.clone()),
            Some(vec![]),
            None,
        ];
        let fields = match &*BLOB_V2_USER_TYPE {
            DataType::Struct(fields) => fields.clone(),
            _ => unreachable!(),
        };
        let data: ArrayRef = Arc::new(LargeBinaryArray::from_iter(
            expected.iter().enumerate().map(|(index, value)| {
                if index == 4 || index == 5 {
                    None
                } else {
                    value.as_deref()
                }
            }),
        ));
        let uris: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            None,
            None,
            None,
            Some(external_uri.as_str()),
            Some(external_uri.as_str()),
            None,
            None,
        ]));
        let blob: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![
                data,
                uris,
                Arc::new(UInt64Array::from(vec![
                    None,
                    None,
                    None,
                    None,
                    Some(7),
                    Some(0),
                    None,
                    None,
                ])),
                Arc::new(UInt64Array::from(vec![
                    None,
                    None,
                    None,
                    None,
                    Some(19),
                    Some(0),
                    None,
                    None,
                ])),
            ],
            Some(NullBuffer::from(vec![
                true, true, true, true, true, true, true, false,
            ])),
        ));
        let metadata = HashMap::from([
            (
                "ARROW:extension:name".to_owned(),
                "lance.blob.v2".to_owned(),
            ),
            (
                "lance-encoding:blob-inline-size-threshold".to_owned(),
                "32".to_owned(),
            ),
            (
                "lance-encoding:blob-dedicated-size-threshold".to_owned(),
                "128".to_owned(),
            ),
            (
                "lance-encoding:blob-pack-file-size-threshold".to_owned(),
                "1024".to_owned(),
            ),
        ]);
        let blob_field =
            Field::new("video", blob.data_type().clone(), true).with_metadata(metadata);
        let nested = StructArray::new(
            vec![Arc::new(blob_field.clone())].into(),
            vec![blob.clone()],
            None,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            blob_field,
            Field::new("info", nested.data_type().clone(), true),
            Field::new("plain", DataType::LargeBinary, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..8)),
                blob,
                Arc::new(nested),
                Arc::new(LargeBinaryArray::from_iter_values((0..8).map(|_| b"plain"))),
            ],
        )
        .unwrap();
        let batch = if include_nested {
            batch
        } else {
            batch.project(&[0, 1, 3]).unwrap()
        };
        let schema = batch.schema();
        let uri = CString::new(dataset_path.to_str().unwrap()).unwrap();
        runtime::block_on(async {
            let dataset = Dataset::write(
                RecordBatchIterator::new(vec![Ok(batch.clone())], schema),
                uri.to_str().unwrap(),
                Some(WriteParams {
                    data_storage_version: Some("2.2".parse().unwrap()),
                    enable_stable_row_ids: stable,
                    allow_external_blob_outside_bases: !alternate_base,
                    initial_bases: alternate_base.then(|| {
                        vec![BasePath {
                            id: 1,
                            name: Some("media".to_owned()),
                            path: url::Url::from_directory_path(external_dir)
                                .unwrap()
                                .to_string(),
                            is_dataset_root: false,
                        }]
                    }),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
            let batch = dataset
                .scan()
                .project(&["video"])
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            let descriptors = batch
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let kinds = descriptors
                .column_by_name("kind")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::UInt8Array>()
                .unwrap();
            assert_eq!(
                (0..7).map(|index| kinds.value(index)).collect::<Vec<_>>(),
                [0, 1, 1, 2, 3, 3, 0]
            );
            let positions = descriptors
                .column_by_name("position")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert!(positions.value(2) > positions.value(1));
        })
        .unwrap();
        Self {
            _directory: directory,
            uri,
            batch,
            expected,
            external,
        }
    }

    fn append(&self) {
        runtime::block_on(Dataset::write(
            RecordBatchIterator::new(vec![Ok(self.batch.clone())], self.batch.schema()),
            self.uri.to_str().unwrap(),
            Some(WriteParams {
                mode: WriteMode::Append,
                allow_external_blob_outside_bases: true,
                ..Default::default()
            }),
        ))
        .unwrap()
        .unwrap();
    }
}

struct Binding {
    dataset: *mut c_void,
    token: CString,
    version: u64,
}

impl Binding {
    fn new(uri: &CStr, access: &Access) -> Self {
        // SAFETY: Inputs remain live; each returned owner is explicitly freed.
        unsafe {
            let dataset = lance_blob_open_binding_dataset(uri.as_ptr(), access.handle);
            assert!(!dataset.is_null(), "{}", last_error());
            let token = lance_blob_snapshot_token(dataset, access.handle);
            assert!(!token.is_null(), "{}", last_error());
            let owned_token = CStr::from_ptr(token).to_owned();
            lance_free_string(token);
            Self {
                dataset,
                token: owned_token,
                version: lance_dataset_version(dataset),
            }
        }
    }
}

impl Drop for Binding {
    fn drop(&mut self) {
        // SAFETY: This owner consumes its DatasetHandle once.
        unsafe { lance_close_dataset(self.dataset) };
    }
}

struct Blob(*mut c_void, u64);

impl Blob {
    fn open(
        uri: &CStr,
        binding: &Binding,
        access: &Access,
        row: u64,
        column: &str,
    ) -> Result<Self, (i32, String)> {
        let column = CString::new(column).unwrap();
        let options = LanceBlobOpenOptions {
            dataset_uri: uri.as_ptr(),
            version: binding.version,
            snapshot_token: binding.token.as_ptr(),
            column_path: column.as_ptr(),
            row_id: row,
            access_context: access.handle,
        };
        let mut blob = ptr::dangling_mut();
        let mut size = u64::MAX;
        // SAFETY: All borrowed strings and handles remain live through the call.
        let result = unsafe { lance_blob_open(&options, &mut blob, &mut size) };
        if result != 0 {
            assert!(blob.is_null());
            assert_eq!(size, 0);
            return Err((lance_last_error_code(), last_error()));
        }
        Ok(Self(blob, size))
    }

    fn read(&self, offset: u64, length: usize) -> Vec<u8> {
        let mut buffer = vec![0; length];
        let mut read = usize::MAX;
        // SAFETY: Live handle; buffer and read output are writable and disjoint.
        assert_eq!(
            unsafe { lance_blob_read_at(self.0, offset, buffer.as_mut_ptr(), length, &mut read) },
            0,
            "{}",
            last_error()
        );
        assert_eq!(read, length);
        buffer
    }
}

impl Drop for Blob {
    fn drop(&mut self) {
        // SAFETY: This owner consumes the Blob after its synchronous reads end.
        unsafe { lance_blob_free(self.0) };
    }
}

fn last_error() -> String {
    let message = lance_last_error_message();
    if message.is_null() {
        return String::new();
    }
    // SAFETY: The error API returns an owned C string until lance_free_string.
    unsafe {
        let result = CStr::from_ptr(message).to_string_lossy().into_owned();
        lance_free_string(message);
        result
    }
}

#[test]
fn four_kinds_nested_paths_ranges_and_empty_values() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let access = Access::new();
    let binding = Binding::new(&fixture.uri, &access);
    for column in ["video", "info.video"] {
        for (row, expected) in fixture.expected.iter().enumerate().take(7) {
            let blob = Blob::open(&fixture.uri, &binding, &access, row as u64, column).unwrap();
            let expected = expected.as_ref().unwrap();
            assert_eq!(blob.1, expected.len() as u64);
            assert_eq!(blob.read(0, expected.len()), *expected);
            if !expected.is_empty() {
                for offset in [0, expected.len() / 2, expected.len() - 1] {
                    assert_eq!(blob.read(offset as u64, 1), expected[offset..offset + 1]);
                }
            }
            let calls = access.call_count();
            assert!(blob.read(0, 0).is_empty());
            assert!(blob.read(blob.1, 0).is_empty());
            assert_eq!(access.call_count(), calls);
        }
    }
    for (row, column) in [
        (7, "video"),
        (999, "video"),
        (0, "plain"),
        (0, "missing"),
        (0, "video.data"),
    ] {
        assert!(Blob::open(&fixture.uri, &binding, &access, row, column).is_err());
    }
}

#[test]
fn row_ids_after_append_delete_and_compaction_in_both_modes() {
    let _counter_guard = counter_guard();
    for stable in [false, true] {
        let fixture = Fixture::build(stable, false, false);
        fixture.append();
        let row_ids = runtime::block_on(async {
            let mut dataset = Dataset::open(fixture.uri.to_str().unwrap()).await.unwrap();
            dataset.delete("id = 0").await.unwrap();
            if stable {
                lance::dataset::optimize::compact_files(&mut dataset, Default::default(), None)
                    .await
                    .unwrap();
            }
            let batch = dataset
                .scan()
                .project(&["id"])
                .unwrap()
                .with_row_id()
                .try_into_batch()
                .await
                .unwrap();
            let rows = batch
                .column_by_name("_rowid")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            rows.values()
                .iter()
                .copied()
                .zip(ids.values().iter().copied())
                .collect::<Vec<_>>()
        })
        .unwrap();
        let access = Access::new();
        let binding = Binding::new(&fixture.uri, &access);
        assert_eq!(
            Blob::open(&fixture.uri, &binding, &access, 0, "video")
                .err()
                .unwrap()
                .0,
            ErrorCode::BlobNotFound as i32
        );
        // In non-stable mode the appended fragment uses high row-ID bits. In
        // stable mode deleting/compacting makes IDs differ from row positions.
        assert!(row_ids
            .iter()
            .enumerate()
            .any(|(index, (row, _))| *row != index as u64));
        for (row, id) in row_ids.into_iter().filter(|(_, id)| *id == 1 || *id == 2) {
            let blob = Blob::open(&fixture.uri, &binding, &access, row, "video").unwrap();
            assert_eq!(
                blob.read(0, blob.1 as usize),
                *fixture.expected[id as usize].as_ref().unwrap()
            );
        }
    }
}

#[test]
fn absent_rows_are_not_found_without_masking_deletion_file_errors() {
    let _counter_guard = counter_guard();
    for stable in [false, true] {
        let fixture = Fixture::build(stable, false, false);
        runtime::block_on(async {
            let mut dataset = Dataset::open(fixture.uri.to_str().unwrap()).await.unwrap();
            dataset.delete("id = 0").await.unwrap();
        })
        .unwrap();
        let access = Access::new();
        let binding = Binding::new(&fixture.uri, &access);
        // Deleted row, null blob, out-of-range offset, missing fragment and
        // tombstone ID. Stable IDs must go through Lance's row-ID index.
        for row in [0, 7, 999, 1 << 32, u64::MAX] {
            let error = Blob::open(&fixture.uri, &binding, &access, row, "video")
                .err()
                .unwrap();
            assert_eq!(
                error.0,
                ErrorCode::BlobNotFound as i32,
                "stable={stable}, row={row}: {error:?}"
            );
        }
        let blob = Blob::open(&fixture.uri, &binding, &access, 1, "video").unwrap();
        assert_eq!(blob.read(0, 64), *fixture.expected[1].as_ref().unwrap());

        access.deny("_deletions", Some(READ));
        for row in [0, 1] {
            assert_eq!(
                Blob::open(&fixture.uri, &binding, &access, row, "video")
                    .err()
                    .unwrap()
                    .0,
                ErrorCode::BlobAccess as i32
            );
        }
        access.policy.deny.lock().unwrap().clear();
        // A malformed deletion file is an I/O/format error, not evidence that
        // a row is absent. Each Blob open must inspect the backing snapshot.
        let deletion_dir = std::path::Path::new(fixture.uri.to_str().unwrap()).join("_deletions");
        for entry in std::fs::read_dir(deletion_dir).unwrap() {
            std::fs::write(entry.unwrap().path(), b"invalid deletion file").unwrap();
        }
        assert_eq!(
            Blob::open(&fixture.uri, &binding, &access, 1, "video")
                .err()
                .unwrap()
                .0,
            ErrorCode::BlobOpen as i32
        );
    }
}

#[test]
fn missing_deletion_files_are_open_errors() {
    let _counter_guard = counter_guard();
    for stable in [false, true] {
        let fixture = Fixture::build(stable, false, false);
        runtime::block_on(async {
            let mut dataset = Dataset::open(fixture.uri.to_str().unwrap()).await.unwrap();
            dataset.delete("id = 0").await.unwrap();
        })
        .unwrap();
        let access = Access::new();
        let binding = Binding::new(&fixture.uri, &access);
        let blob = Blob::open(&fixture.uri, &binding, &access, 1, "video").unwrap();
        assert_eq!(blob.read(0, 64), *fixture.expected[1].as_ref().unwrap());
        let deletion_dir = std::path::Path::new(fixture.uri.to_str().unwrap()).join("_deletions");
        let files: Vec<_> = std::fs::read_dir(deletion_dir).unwrap().collect();
        assert!(!files.is_empty());
        for file in files {
            std::fs::remove_file(file.unwrap().path()).unwrap();
        }
        let error = Blob::open(&fixture.uri, &binding, &access, 1, "video")
            .err()
            .unwrap();
        assert_eq!(
            error.0,
            ErrorCode::BlobOpen as i32,
            "stable={stable}: {error:?}"
        );
        assert!(!error.1.contains(fixture.uri.to_str().unwrap()));
    }
}

#[test]
fn snapshot_reopen_survives_append_compaction_but_detects_cleanup_and_replacement() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::build(true, false, false);
    let access = Access::new();
    let binding = Binding::new(&fixture.uri, &access);
    let identity_without_etag = || {
        let dataset = runtime::block_on(
            crate::ffi::blob_store::tests::dataset_without_manifest_etag(
                fixture.uri.to_str().unwrap(),
            ),
        )
        .unwrap();
        assert!(dataset.manifest_location().e_tag.is_none());
        token(
            &format_snapshot_identity(&dataset, dataset.manifest_location().size.unwrap(), None)
                .unwrap(),
        )
    };
    let original_without_etag = identity_without_etag();
    // Verify the canonical token hashes the full, unchanged Vane identity.
    // SAFETY: Binding keeps this DatasetHandle live; string is freed once.
    unsafe {
        let generation = lance_dataset_generation_id(binding.dataset);
        assert!(!generation.is_null());
        let expected = format!(
            "{:x}",
            Sha256::digest(CStr::from_ptr(generation).to_bytes())
        );
        assert_eq!(binding.token.to_str().unwrap(), expected);
        lance_free_string(generation);
    }
    fixture.append();
    runtime::block_on(async {
        let mut latest = Dataset::open(fixture.uri.to_str().unwrap()).await.unwrap();
        lance::dataset::optimize::compact_files(&mut latest, Default::default(), None)
            .await
            .unwrap();
    })
    .unwrap();
    let blob = Blob::open(&fixture.uri, &binding, &access, 1, "video").unwrap();
    assert_eq!(
        blob.read(0, 64),
        fixture.expected[1].as_ref().unwrap().clone()
    );
    let manifest = runtime::block_on(async {
        let dataset = exact_version_builder(fixture.uri.to_str().unwrap(), binding.version)
            .load()
            .await
            .unwrap();
        dataset.manifest_location().path.to_string()
    })
    .unwrap();
    let manifest = std::path::PathBuf::from(format!("/{manifest}"));
    std::fs::remove_file(&manifest).unwrap();
    assert_eq!(
        Blob::open(&fixture.uri, &binding, &access, 1, "video")
            .err()
            .unwrap()
            .0,
        ErrorCode::BlobSnapshot as i32
    );
    // Captured handles still exist, so success cannot be explained by dropping a
    // stale cache. Replace the entire directory with a normal new version 1.
    std::fs::remove_dir_all(fixture.uri.to_str().unwrap()).unwrap();
    runtime::block_on(Dataset::write(
        RecordBatchIterator::new(vec![Ok(fixture.batch.clone())], fixture.batch.schema()),
        fixture.uri.to_str().unwrap(),
        Some(WriteParams {
            data_storage_version: Some("2.2".parse().unwrap()),
            allow_external_blob_outside_bases: true,
            ..Default::default()
        }),
    ))
    .unwrap()
    .unwrap();
    let failure = Blob::open(&fixture.uri, &binding, &access, 1, "video")
        .err()
        .unwrap();
    assert_eq!(failure.0, ErrorCode::BlobSnapshot as i32);
    assert!(failure.1.contains("identity mismatch"));
    // Normal recreation also changes the canonical identity when the backend
    // supplies no ETag; this is still an identity check, not a payload checksum.
    assert_ne!(identity_without_etag(), original_without_etag);
}

#[test]
fn local_uri_aliases_reopen_the_same_snapshot() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let access = Access::new();
    let binding = Binding::new(&fixture.uri, &access);
    let uri = url::Url::from_file_path(fixture.uri.to_str().unwrap())
        .unwrap()
        .to_string();
    for separator in ["//", "%2F%2F"] {
        let alias =
            CString::new(uri.replace("/table.lance", &format!("{separator}table.lance"))).unwrap();
        let alias_binding = Binding::new(&alias, &access);
        assert_eq!(alias_binding.version, binding.version);
        assert_eq!(alias_binding.token, binding.token);
        let blob = Blob::open(&alias, &binding, &access, 0, "video").unwrap();
        assert_eq!(blob.read(0, 3), b"abc");
    }
}

#[test]
fn local_uri_authorities_cannot_bypass_dataset_policy() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let access = Access::new();
    let binding = Binding::new(&fixture.uri, &access);
    let canonical = url::Url::from_file_path(fixture.uri.to_str().unwrap()).unwrap();
    for scheme in ["file", "file-object-store", "file+uring"] {
        let canonical = canonical
            .as_str()
            .replacen("file:", &format!("{scheme}:"), 1);
        access.deny(&canonical, None);
        let alias = CString::new(canonical.replacen(":///", "://host/", 1)).unwrap();
        let count = access.call_count();
        let error = Blob::open(&alias, &binding, &access, 0, "video")
            .err()
            .unwrap();
        assert_eq!(error.0, ErrorCode::BlobAccess as i32);
        // SAFETY: URI and access owner remain live through the binding attempt.
        let dataset = unsafe { lance_blob_open_binding_dataset(alias.as_ptr(), access.handle) };
        assert!(dataset.is_null());
        assert_eq!(lance_last_error_code(), ErrorCode::BlobAccess as i32);
        assert_eq!(access.call_count(), count);
    }
}

#[test]
fn local_scheme_aliases_share_dataset_snapshot_and_policy() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let canonical = url::Url::from_file_path(fixture.uri.to_str().unwrap()).unwrap();
    let access = Access::new();
    let binding = Binding::new(&fixture.uri, &access);
    for scheme in ["file", "file-object-store", "file+uring"] {
        let alias = CString::new(
            canonical
                .as_str()
                .replacen("file:", &format!("{scheme}:"), 1),
        )
        .unwrap();
        let alias_binding = Binding::new(&alias, &access);
        assert_eq!(alias_binding.token, binding.token);
        let blob = Blob::open(&alias, &binding, &access, 0, "video").unwrap();
        assert_eq!(blob.read(0, 3), b"abc");
    }
    access.deny(canonical.as_str(), None);
    for scheme in ["file", "file-object-store", "file+uring"] {
        let alias = CString::new(
            canonical
                .as_str()
                .replacen("file:", &format!("{scheme}:"), 1),
        )
        .unwrap();
        let error = Blob::open(&alias, &binding, &access, 0, "video")
            .err()
            .unwrap();
        assert_eq!(error.0, ErrorCode::BlobAccess as i32);
        // SAFETY: The URI and access context outlive the binding call.
        assert!(
            unsafe { lance_blob_open_binding_dataset(alias.as_ptr(), access.handle) }.is_null()
        );
        assert_eq!(lance_last_error_code(), ErrorCode::BlobAccess as i32);
    }
    assert!(access
        .policy
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|(uri, _, _)| uri.starts_with("file:///")));
}

fn external_uri_fixture(fixture: &Fixture, external_uri: &str) -> (CString, Arc<Dataset>) {
    let blob = fixture
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let mut blob_columns = blob.columns().to_vec();
    blob_columns[1] = Arc::new(StringArray::from(vec![
        None,
        None,
        None,
        None,
        Some(external_uri),
        Some(external_uri),
        None,
        None,
    ]));
    let mut columns = fixture.batch.columns().to_vec();
    columns[1] = Arc::new(StructArray::new(
        blob.fields().clone(),
        blob_columns,
        blob.nulls().cloned(),
    ));
    let batch = RecordBatch::try_new(fixture.batch.schema(), columns).unwrap();
    let uri = CString::new(
        fixture
            ._directory
            .0
            .join("external-alias.lance")
            .to_str()
            .unwrap(),
    )
    .unwrap();
    let dataset = runtime::block_on(Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        uri.to_str().unwrap(),
        Some(WriteParams {
            data_storage_version: Some("2.2".parse().unwrap()),
            allow_external_blob_outside_bases: true,
            ..Default::default()
        }),
    ))
    .unwrap()
    .unwrap();
    (uri, Arc::new(dataset))
}

#[test]
fn external_local_uri_authority_cannot_bypass_object_policy() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let canonical = url::Url::from_file_path(&fixture.external).unwrap();
    let external_uri = canonical.as_str().replacen("file:///", "file://host/", 1);
    let (uri, _) = external_uri_fixture(&fixture, &external_uri);
    let access = Access::new();
    let binding = Binding::new(&uri, &access);
    access.deny(canonical.as_str(), None);
    for row in [4, 5] {
        let error = Blob::open(&uri, &binding, &access, row, "video")
            .err()
            .unwrap();
        assert_eq!(error.0, ErrorCode::BlobAccess as i32);
    }
    assert!(!access
        .policy
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(uri, _, _)| uri.contains("://host/")));
}

#[test]
fn external_local_uri_alias_uses_canonical_policy_targets() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let external_uri = url::Url::from_file_path(&fixture.external)
        .unwrap()
        .to_string()
        .replace("/object.bin", "//object.bin");
    let (uri, dataset) = external_uri_fixture(&fixture, &external_uri);
    runtime::block_on(async {
        // The original external URI is accepted by Lance's filesystem provider.
        let files = dataset
            .take_blobs_by_addresses(&[4], "video")
            .await
            .unwrap();
        assert_eq!(
            files[0].read_range(0..19).await.unwrap().as_ref(),
            fixture.expected[4].as_ref().unwrap()
        );
    })
    .unwrap();
    let access = Access::new();
    let binding = Binding::new(&uri, &access);
    for row in [4, 5] {
        let blob = Blob::open(&uri, &binding, &access, row, "video").unwrap();
        let expected = fixture.expected[row as usize].as_ref().unwrap();
        assert_eq!(blob.read(0, expected.len()), *expected);
    }
    let canonical = url::Url::from_file_path(&fixture.external)
        .unwrap()
        .to_string();
    assert!(access
        .policy
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(uri, op, _)| uri == &canonical && *op == READ));
    access.deny(&canonical, Some(METADATA));
    let failure = Blob::open(&uri, &binding, &access, 5, "video")
        .err()
        .unwrap();
    assert_eq!(failure.0, ErrorCode::BlobAccess as i32);
}

#[test]
fn external_local_scheme_aliases_use_canonical_permissions() {
    let _counter_guard = counter_guard();
    for scheme in ["file", "file-object-store", "file+uring"] {
        if ObjectStoreRegistry::default()
            .get_provider(scheme)
            .is_none()
        {
            continue;
        }
        let fixture = Fixture::new(false, false);
        let canonical = url::Url::from_file_path(&fixture.external).unwrap();
        let alias = canonical
            .as_str()
            .replacen("file:", &format!("{scheme}:"), 1);
        let (uri, _) = external_uri_fixture(&fixture, &alias);
        let access = Access::new();
        let binding = Binding::new(&uri, &access);
        for row in [4, 5] {
            let blob = Blob::open(&uri, &binding, &access, row, "video").unwrap();
            let expected = fixture.expected[row as usize].as_ref().unwrap();
            assert_eq!(blob.read(0, expected.len()), *expected);
            drop(blob);
            // Open the client before revocation, but force a fresh backing read
            // rather than asking Lance to return bytes it has already buffered.
            let blob = Blob::open(&uri, &binding, &access, row, "video").unwrap();
            access.deny(canonical.as_str(), Some(READ));
            let mut buffer = [91u8; 3];
            let mut read = 123;
            // SAFETY: Live Blob and writable output buffers for this read.
            assert_eq!(
                unsafe {
                    lance_blob_read_at(blob.0, 0, buffer.as_mut_ptr(), buffer.len(), &mut read)
                },
                -1
            );
            assert_eq!(lance_last_error_code(), ErrorCode::BlobAccess as i32);
            assert_eq!(buffer, [91; 3]);
            assert_eq!(read, 0);
            access.policy.deny.lock().unwrap().clear();
        }
        access.deny(canonical.as_str(), Some(METADATA));
        let error = Blob::open(&uri, &binding, &access, 5, "video")
            .err()
            .unwrap();
        assert_eq!(error.0, ErrorCode::BlobAccess as i32);
        access.deny(canonical.as_str(), None);
        for row in [4, 5] {
            let error = Blob::open(&uri, &binding, &access, row, "video")
                .err()
                .unwrap();
            assert_eq!(error.0, ErrorCode::BlobAccess as i32);
        }
        assert!(access
            .policy
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(uri, _, _)| uri.starts_with("file:///")));
    }
}

#[test]
fn external_unknown_size_and_alternate_base_are_authorized_during_open() {
    let _counter_guard = counter_guard();
    for alternate_base in [false, true] {
        let fixture = Fixture::new(false, alternate_base);
        let access = Access::new();
        let binding = Binding::new(&fixture.uri, &access);
        access.deny("external", Some(METADATA));
        let failure = Blob::open(&fixture.uri, &binding, &access, 5, "video")
            .err()
            .unwrap();
        assert_eq!(failure.0, ErrorCode::BlobAccess as i32);
        assert!(!failure.1.contains("secret-key"));
        assert!(access
            .policy
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(uri, op, _)| uri.ends_with("external/object.bin") && *op == METADATA));
        access.policy.deny.lock().unwrap().clear();
        let blob = Blob::open(&fixture.uri, &binding, &access, 4, "video").unwrap();
        assert_eq!(
            blob.read(0, 19),
            fixture.expected[4].as_ref().unwrap().clone()
        );
        assert!(access
            .policy
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(uri, op, _)| uri.contains("external") && *op == PROVIDER));
        std::fs::remove_file(&fixture.external).unwrap();
        let missing = Blob::open(&fixture.uri, &binding, &access, 5, "video")
            .err()
            .unwrap();
        assert_eq!(missing.0, ErrorCode::BlobNotFound as i32);
        assert!(missing.1.contains("not found"));
    }
}

#[test]
fn invalid_ranges_do_not_touch_buffer_or_backing_store() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let access = Access::new();
    let binding = Binding::new(&fixture.uri, &access);
    let blob = Blob::open(&fixture.uri, &binding, &access, 1, "video").unwrap();
    let count = access.call_count();
    for (offset, length) in [(65, 0), (64, 1), (63, 2), (u64::MAX, 1), (0, usize::MAX)] {
        let mut buffer = [91u8; 2];
        let mut read = 123;
        // SAFETY: An invalid logical range must be rejected before accessing the
        // small sentinel buffer, including the intentionally huge length.
        assert_eq!(
            unsafe { lance_blob_read_at(blob.0, offset, buffer.as_mut_ptr(), length, &mut read) },
            -1
        );
        assert_eq!(lance_last_error_code(), ErrorCode::BlobRange as i32);
        assert_eq!(read, 0);
        assert_eq!(buffer, [91, 91]);
    }
    assert_eq!(access.call_count(), count);
    assert!(checked_range(u64::MAX, u64::MAX, 0).is_ok());
    assert!(checked_range(u64::MAX, u64::MAX, 1).is_err());
}

#[test]
fn worker_read_denial_and_truncated_backing_object_preserve_errors_on_close() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let access = Access::new();
    let binding = Binding::new(&fixture.uri, &access);
    let blob = Blob::open(&fixture.uri, &binding, &access, 4, "video").unwrap();
    access.deny("external/object.bin", Some(READ));
    let mut buffer = [91u8; 19];
    let mut read = 123;
    // SAFETY: Blob and outputs are valid; failure does not write to the buffer.
    assert_eq!(
        unsafe { lance_blob_read_at(blob.0, 0, buffer.as_mut_ptr(), buffer.len(), &mut read) },
        -1
    );
    assert_eq!(lance_last_error_code(), ErrorCode::BlobAccess as i32);
    assert_eq!(read, 0);
    assert_eq!(buffer, [91; 19]);
    assert!(access
        .policy
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(_, op, thread)| *op == READ && *thread != std::thread::current().id()));
    drop(blob);
    assert_eq!(lance_last_error_code(), ErrorCode::BlobAccess as i32);
    assert!(!last_error().contains("secret-key"));
    access.policy.deny.lock().unwrap().clear();
    let blob = Blob::open(&fixture.uri, &binding, &access, 4, "video").unwrap();
    std::fs::write(&fixture.external, b"short").unwrap();
    // SAFETY: Valid handle and buffer, backing object is deliberately truncated.
    assert_eq!(
        unsafe { lance_blob_read_at(blob.0, 0, buffer.as_mut_ptr(), buffer.len(), &mut read) },
        -1
    );
    assert_eq!(lance_last_error_code(), ErrorCode::BlobRead as i32);
    drop(blob);
    assert_eq!(lance_last_error_code(), ErrorCode::BlobRead as i32);
}

#[test]
#[cfg(unix)]
fn filesystem_permission_denial_remains_an_access_error() {
    use std::os::unix::fs::PermissionsExt;

    struct RestorePermissions(std::path::PathBuf, std::fs::Permissions);
    impl Drop for RestorePermissions {
        fn drop(&mut self) {
            std::fs::set_permissions(&self.0, self.1.clone()).unwrap();
        }
    }

    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let access = Access::new();
    let binding = Binding::new(&fixture.uri, &access);
    let blob = Blob::open(&fixture.uri, &binding, &access, 4, "video").unwrap();
    let _restore = RestorePermissions(
        fixture.external.clone(),
        std::fs::metadata(&fixture.external).unwrap().permissions(),
    );
    std::fs::set_permissions(&fixture.external, std::fs::Permissions::from_mode(0o000)).unwrap();
    let Err(error) = std::fs::read(&fixture.external) else {
        eprintln!("skipping chmod check: this process can read mode-000 files");
        return;
    };
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    let mut buffer = [91u8; 19];
    let mut read = 123;
    // SAFETY: Live Blob handle with valid writable output pointers.
    assert_eq!(
        unsafe { lance_blob_read_at(blob.0, 0, buffer.as_mut_ptr(), buffer.len(), &mut read) },
        -1
    );
    assert_eq!(lance_last_error_code(), ErrorCode::BlobAccess as i32);
    assert_eq!(read, 0);
    assert_eq!(buffer, [91; 19]);
    assert!(!last_error().contains(fixture.external.to_str().unwrap()));
    // Unknown-size external Blobs issue HEAD while opening, before any read.
    let error = Blob::open(&fixture.uri, &binding, &access, 5, "video")
        .err()
        .unwrap();
    assert_eq!(error.0, ErrorCode::BlobAccess as i32);
}

#[test]
fn access_lease_outlives_binding_and_parallel_handles() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let access = Access::new();
    let policy = access.policy.clone();
    let binding = Binding::new(&fixture.uri, &access);
    let first = Blob::open(&fixture.uri, &binding, &access, 1, "video").unwrap();
    let second = Blob::open(&fixture.uri, &binding, &access, 2, "video").unwrap();
    drop(binding);
    drop(access);
    assert_eq!(policy.released.load(Ordering::SeqCst), 0);
    // Ownership is transferred using integer addresses solely in this test;
    // each independent handle has one owner and no concurrent free/read.
    let (first_ptr, second_ptr) = (first.0 as usize, second.0 as usize);
    std::mem::forget(first);
    std::mem::forget(second);
    let first = std::thread::spawn(move || Blob(first_ptr as *mut c_void, 64).read(2, 8));
    let second = std::thread::spawn(move || Blob(second_ptr as *mut c_void, 80).read(3, 9));
    assert_eq!(
        first.join().unwrap(),
        fixture.expected[1].as_ref().unwrap()[2..10]
    );
    assert_eq!(
        second.join().unwrap(),
        fixture.expected[2].as_ref().unwrap()[3..12]
    );
    assert_released(&policy);
}

#[test]
fn ffi_pointer_validation_and_failed_open_release_every_owner() {
    let _counter_guard = counter_guard();
    let access = Access::new();
    let policy = access.policy.clone();
    // SAFETY: Nulls deliberately exercise validation; non-null outputs are valid.
    unsafe {
        let mut handle = ptr::dangling_mut();
        assert_eq!(lance_blob_access_create(ptr::null(), &mut handle), -1);
        assert!(handle.is_null());
        assert_eq!(lance_blob_access_create(ptr::null(), ptr::null_mut()), -1);
        let mut size = 9;
        assert_eq!(lance_blob_open(ptr::null(), &mut handle, &mut size), -1);
        assert!(handle.is_null());
        assert_eq!(size, 0);
        let mut read = 9;
        assert_eq!(
            lance_blob_read_at(ptr::null_mut(), 0, ptr::null_mut(), 0, &mut read),
            -1
        );
        assert_eq!(read, 0);
        assert_eq!(
            lance_blob_read_at(ptr::null_mut(), 0, ptr::null_mut(), 0, ptr::null_mut()),
            -1
        );
        assert!(lance_blob_snapshot_token(ptr::null_mut(), access.handle).is_null());
        assert!(lance_blob_normalize_uri(ptr::null(), false).is_null());
        assert_eq!(
            lance_blob_validate_column(ptr::null_mut(), c"video".as_ptr()),
            -1
        );
        assert!(lance_dataset_checkout_version(ptr::null_mut(), 1).is_null());
        lance_blob_free(ptr::null_mut());
        lance_blob_access_free(ptr::null_mut());
    }
    access.deny("missing", None);
    // SAFETY: C string and owned access handle remain live.
    assert!(unsafe {
        lance_blob_open_binding_dataset(c"s3://missing/table.lance".as_ptr(), access.handle)
    }
    .is_null());
    assert_eq!(lance_last_error_code(), ErrorCode::BlobAccess as i32);
    drop(access);
    assert_released(&policy);
}

#[test]
fn unsupported_collection_and_lookalike_columns_and_null_parents() {
    let _counter_guard = counter_guard();
    let uri = CString::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/test/data/blob_v2_fixture.lance"
    ))
    .unwrap();
    let access = Access::new();
    let binding = Binding::new(&uri, &access);
    // Binding validates the producer schema without materializing any row.
    // SAFETY: The binding and C strings remain alive during validation.
    unsafe {
        assert_eq!(
            lance_blob_validate_column(binding.dataset, c"video".as_ptr()),
            0
        );
        assert_eq!(
            lance_blob_validate_column(binding.dataset, c"info.video".as_ptr()),
            0
        );
        assert_eq!(lance_blob_validate_column(binding.dataset, ptr::null()), -1);
    }
    for column in ["clips", "clips.item", "lookalike", "plain", "info"] {
        assert_eq!(
            Blob::open(&uri, &binding, &access, 0, column)
                .err()
                .unwrap()
                .0,
            ErrorCode::BlobOpen as i32
        );
    }
    for (row, column) in [(4, "info.video"), (5, "video"), (u64::MAX, "video")] {
        assert!(Blob::open(&uri, &binding, &access, row, column).is_err());
    }
    // Legacy Blob is deliberately excluded even though Lance can take it.
    let directory = TempDir::new();
    let path = CString::new(directory.0.join("legacy.lance").to_str().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "legacy",
        DataType::LargeBinary,
        true,
    )
    .with_metadata(HashMap::from([(
        lance_arrow::BLOB_META_KEY.to_owned(),
        "true".to_owned(),
    )]))]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(LargeBinaryArray::from_iter_values([b"legacy"]))],
    )
    .unwrap();
    runtime::block_on(Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        path.to_str().unwrap(),
        None,
    ))
    .unwrap()
    .unwrap();
    let binding = Binding::new(&path, &access);
    assert_eq!(
        Blob::open(&path, &binding, &access, 0, "legacy")
            .err()
            .unwrap()
            .0,
        ErrorCode::BlobOpen as i32
    );
}

#[test]
fn bind_metadata_fill_and_reopen_use_the_current_access_context() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let access = Access::new();
    let binding = Binding::new(&fixture.uri, &access);
    let dataset = runtime::block_on(
        crate::ffi::blob_store::tests::dataset_without_manifest_etag(fixture.uri.to_str().unwrap()),
    )
    .unwrap();
    assert!(dataset.manifest_location().e_tag.is_none());
    let missing_metadata = Box::into_raw(Box::new(DatasetHandle::new(Arc::new(dataset)))).cast();
    // A store without retained ETags forces an authorized metadata fill.
    // SAFETY: Live dataset and access owners; each returned dataset is freed once.
    unsafe {
        let checked_out = lance_dataset_checkout_version(binding.dataset, binding.version);
        assert!(!checked_out.is_null());
        assert_eq!(lance_dataset_version(checked_out), binding.version);
        lance_close_dataset(checked_out);
        access.deny("_versions", Some(METADATA));
        assert!(lance_blob_snapshot_token(missing_metadata, access.handle).is_null());
        assert_eq!(lance_last_error_code(), ErrorCode::BlobAccess as i32);
        lance_close_dataset(missing_metadata);
    }
    let denied_opener = Access::new();
    denied_opener.deny("table.lance", None);
    assert_eq!(
        Blob::open(&fixture.uri, &binding, &denied_opener, 0, "video")
            .err()
            .unwrap()
            .0,
        ErrorCode::BlobAccess as i32
    );
}

#[test]
fn malformed_open_arguments_and_read_outputs_fail_before_io() {
    let _counter_guard = counter_guard();
    let fixture = Fixture::new(false, false);
    let access = Access::new();
    let binding = Binding::new(&fixture.uri, &access);
    let calls = access.call_count();
    for (version, token, column) in [
        (0, binding.token.as_c_str(), c"video"),
        (1, c"invalid", c"video"),
        (1, binding.token.as_c_str(), c""),
    ] {
        let options = LanceBlobOpenOptions {
            dataset_uri: fixture.uri.as_ptr(),
            version,
            snapshot_token: token.as_ptr(),
            column_path: column.as_ptr(),
            row_id: 0,
            access_context: access.handle,
        };
        let (mut handle, mut size) = (ptr::dangling_mut(), 99);
        // SAFETY: Valid borrowed options with invalid contents and valid outputs.
        assert_eq!(
            unsafe { lance_blob_open(&options, &mut handle, &mut size) },
            -1
        );
        assert!(handle.is_null());
        assert_eq!(size, 0);
        assert_eq!(lance_last_error_code(), ErrorCode::InvalidArgument as i32);
    }
    assert_eq!(access.call_count(), calls);
    let blob = Blob::open(&fixture.uri, &binding, &access, 0, "video").unwrap();
    let mut read = 99;
    // SAFETY: Null buffers are supported only for valid empty ranges.
    unsafe {
        assert_eq!(
            lance_blob_read_at(blob.0, blob.1, ptr::null_mut(), 0, &mut read),
            0
        );
        assert_eq!(read, 0);
        assert_eq!(
            lance_blob_read_at(blob.0, 0, ptr::null_mut(), 1, &mut read),
            -1
        );
        assert_eq!(read, 0);
        assert_eq!(lance_last_error_code(), ErrorCode::InvalidArgument as i32);
    }
}
