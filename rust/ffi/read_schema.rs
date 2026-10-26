//! The scan view of a schema is distinct from the stored, writable schema.
//! Lance returns Blob v2 descriptors by default, including inside containers.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, Schema, UnionFields};
use arrow::ffi::FFI_ArrowSchema;
use lance_arrow::{ARROW_EXT_NAME_KEY, BLOB_V2_EXT_NAME};
use lance_core::datatypes::{Field as LanceField, Schema as LanceSchema};

use crate::error::{clear_last_error, set_last_error, ErrorCode};

use super::util::{FfiError, FfiResult};

pub(crate) fn dataset_read_schema(storage: &LanceSchema) -> Arc<Schema> {
    let mut read = storage.clone();
    for field in &mut read.fields {
        field.unload_blobs_recursive();
    }
    Arc::new(Schema::from(&read))
}

// Namespace schemas arrive as Arrow schemas. Convert only marked Blob v2
// fields through Lance, preserving unrelated Arrow types and their metadata.
fn read_field(field: &FieldRef) -> FfiResult<FieldRef> {
    if field.metadata().get(ARROW_EXT_NAME_KEY).map(String::as_str) == Some(BLOB_V2_EXT_NAME) {
        let mut lance_field = LanceField::try_from(field.as_ref())
            .map_err(|err| schema_error(format!("invalid Blob v2 field: {err}")))?;
        lance_field.unload_blobs_recursive();
        return Ok(Arc::new(Field::from(&lance_field)));
    }
    Ok(Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(read_type(field.data_type())?),
    ))
}

fn read_type(data_type: &DataType) -> FfiResult<DataType> {
    Ok(match data_type {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(read_field)
                .collect::<FfiResult<Vec<_>>>()?
                .into(),
        ),
        DataType::List(field) => DataType::List(read_field(field)?),
        DataType::LargeList(field) => DataType::LargeList(read_field(field)?),
        DataType::ListView(field) => DataType::ListView(read_field(field)?),
        DataType::LargeListView(field) => DataType::LargeListView(read_field(field)?),
        DataType::FixedSizeList(field, size) => DataType::FixedSizeList(read_field(field)?, *size),
        DataType::Map(field, sorted) => DataType::Map(read_field(field)?, *sorted),
        DataType::Dictionary(key, value) => {
            DataType::Dictionary(key.clone(), Box::new(read_type(value)?))
        }
        DataType::RunEndEncoded(ends, values) => {
            DataType::RunEndEncoded(read_field(ends)?, read_field(values)?)
        }
        DataType::Union(fields, mode) => {
            let (ids, children): (Vec<_>, Vec<_>) = fields
                .iter()
                .map(|(id, field)| read_field(field).map(|field| (id, field)))
                .collect::<FfiResult<Vec<_>>>()?
                .into_iter()
                .unzip();
            DataType::Union(
                UnionFields::try_new(ids, children).map_err(|err| schema_error(err.to_string()))?,
                *mode,
            )
        }
        other => other.clone(),
    })
}

fn arrow_read_schema(storage: &Schema) -> FfiResult<Schema> {
    let fields = storage
        .fields()
        .iter()
        .map(read_field)
        .collect::<FfiResult<Vec<_>>>()?;
    Ok(Schema::new_with_metadata(
        fields,
        storage.metadata().clone(),
    ))
}

fn schema_error(message: String) -> FfiError {
    FfiError::new(ErrorCode::SchemaExport, format!("read schema: {message}"))
}

/// Borrow a live Arrow C schema and export an independently owned scan schema.
/// The caller supplies a distinct, empty output and retains ownership of input.
#[no_mangle]
pub unsafe extern "C" fn lance_arrow_schema_to_read(
    storage: *const FFI_ArrowSchema,
    out: *mut FFI_ArrowSchema,
) -> i32 {
    match arrow_schema_to_read_inner(storage, out) {
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

fn arrow_schema_to_read_inner(
    storage: *const FFI_ArrowSchema,
    out: *mut FFI_ArrowSchema,
) -> FfiResult<()> {
    if storage.is_null() || out.is_null() || std::ptr::eq(storage, out) {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "distinct non-null schema pointers required",
        ));
    }
    // SAFETY: The caller supplies valid, aligned ArrowSchema pointers. Input
    // is borrowed, never moved or released, and output is checked before use.
    let (storage, output) = unsafe { (&*storage, &mut *out) };
    if storage.release.is_none() || storage.format.is_null() || output.release.is_some() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "live input and empty output schemas required",
        ));
    }
    let storage = Schema::try_from(storage).map_err(|err| schema_error(err.to_string()))?;
    let read = arrow_read_schema(&storage)?;
    let exported = FFI_ArrowSchema::try_from(&read).map_err(|err| schema_error(err.to_string()))?;
    *output = exported;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ptr;

    use futures::TryStreamExt;
    use lance::Dataset;
    use lance_arrow::BLOB_META_KEY;
    use lance_core::datatypes::{BLOB_V2_DESC_TYPE, BLOB_V2_USER_TYPE};

    use super::*;

    fn blob_field(name: &str, nullable: bool) -> Field {
        Field::new(name, BLOB_V2_USER_TYPE.clone(), nullable).with_metadata(HashMap::from([
            (ARROW_EXT_NAME_KEY.to_string(), BLOB_V2_EXT_NAME.to_string()),
            ("user-key".to_string(), "storage-only".to_string()),
        ]))
    }

    #[test]
    fn nested_read_views_preserve_storage_and_unrelated_fields() {
        let blob = Arc::new(blob_field("item", false));
        let storage = Schema::new_with_metadata(
            vec![
                blob_field("video", true),
                Field::new(
                    "nested",
                    DataType::Struct(
                        vec![
                            blob.clone(),
                            Arc::new(Field::new("clips", DataType::List(blob.clone()), true)),
                            Arc::new(Field::new("fixed", DataType::FixedSizeList(blob, 2), true)),
                        ]
                        .into(),
                    ),
                    true,
                ),
                Field::new("plain", DataType::LargeBinary, true),
                Field::new("lookalike", BLOB_V2_USER_TYPE.clone(), true),
                Field::new("half", DataType::Float16, false),
                Field::new("legacy", DataType::LargeBinary, true).with_metadata(HashMap::from([(
                    BLOB_META_KEY.to_string(),
                    "true".to_string(),
                )])),
            ],
            HashMap::from([("schema-key".to_string(), "keep".to_string())]),
        );
        let original = storage.clone();
        let lance_storage = LanceSchema::try_from(&storage).unwrap();
        let original_lance = lance_storage.clone();
        let dataset_read = dataset_read_schema(&lance_storage);
        let namespace_read = arrow_read_schema(&storage).unwrap();
        assert_eq!(*dataset_read, namespace_read);
        assert_eq!(storage, original);
        assert_eq!(lance_storage, original_lance);
        assert_eq!(namespace_read.metadata(), storage.metadata());
        assert_eq!(namespace_read.field(0).data_type(), &*BLOB_V2_DESC_TYPE);
        assert!(namespace_read.field(0).is_nullable());
        assert!(!namespace_read
            .field(0)
            .metadata()
            .contains_key(ARROW_EXT_NAME_KEY));
        for index in 2..storage.fields().len() {
            assert_eq!(namespace_read.field(index), storage.field(index));
        }
        let DataType::Struct(nested) = namespace_read.field(1).data_type() else {
            panic!()
        };
        assert_eq!(nested[0].data_type(), &*BLOB_V2_DESC_TYPE);
        assert!(!nested[0].is_nullable());
        assert_eq!(arrow_read_schema(&namespace_read).unwrap(), namespace_read);
    }

    #[test]
    fn namespace_conversion_preserves_arrow_only_container_properties() {
        let item = Arc::new(blob_field("item", true));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(blob_field("value", true)),
                ]
                .into(),
            ),
            false,
        ));
        let storage = Schema::new(vec![
            Field::new("sorted_map", DataType::Map(entries, true), false),
            Field::new("view", DataType::LargeListView(item.clone()), true),
            Field::new("large", DataType::LargeList(item), true),
        ]);
        let read = arrow_read_schema(&storage).unwrap();
        let DataType::Map(entries, sorted) = read.field(0).data_type() else {
            panic!()
        };
        assert!(*sorted);
        let DataType::Struct(fields) = entries.data_type() else {
            panic!()
        };
        assert_eq!(fields[1].data_type(), &*BLOB_V2_DESC_TYPE);
        let DataType::LargeListView(item) = read.field(1).data_type() else {
            panic!()
        };
        assert_eq!(item.data_type(), &*BLOB_V2_DESC_TYPE);
        assert_eq!(arrow_read_schema(&read).unwrap(), read);
    }

    #[test]
    fn ffi_borrows_input_and_returns_independently_owned_output() {
        let storage = Schema::new_with_metadata(
            vec![blob_field("video", true)],
            HashMap::from([("root-key".to_string(), "keep".to_string())]),
        );
        let input = FFI_ArrowSchema::try_from(&storage).unwrap();
        let mut output = FFI_ArrowSchema::empty();
        // SAFETY: Both schemas are live, distinct and aligned; output is empty.
        assert_eq!(
            unsafe { lance_arrow_schema_to_read(&input, &mut output) },
            0
        );
        assert_eq!(Schema::try_from(&input).unwrap(), storage);
        drop(input);
        assert_eq!(
            Schema::try_from(&output).unwrap(),
            arrow_read_schema(&storage).unwrap()
        );
    }

    #[test]
    fn ffi_rejects_invalid_arguments_without_consuming_schemas() {
        let storage = Schema::new(vec![blob_field("video", true)]);
        let mut input = FFI_ArrowSchema::try_from(&storage).unwrap();
        let mut output = FFI_ArrowSchema::empty();
        // SAFETY: Null is explicitly permitted for argument validation tests.
        assert_eq!(
            unsafe { lance_arrow_schema_to_read(ptr::null(), &mut output) },
            -1
        );
        assert!(output.release.is_none());
        let alias = &mut input as *mut FFI_ArrowSchema;
        // SAFETY: Aliasing is rejected before either schema is borrowed.
        assert_eq!(unsafe { lance_arrow_schema_to_read(alias, alias) }, -1);
        assert_eq!(Schema::try_from(&input).unwrap(), storage);
        let mut live_output = FFI_ArrowSchema::try_from(&storage).unwrap();
        // SAFETY: Both inputs are valid; a nonempty output must be rejected.
        assert_eq!(
            unsafe { lance_arrow_schema_to_read(&input, &mut live_output) },
            -1
        );
        assert_eq!(Schema::try_from(&live_output).unwrap(), storage);
        let invalid = FFI_ArrowSchema::try_new("invalid-format", vec![], None).unwrap();
        // SAFETY: The Arrow schema is owned and valid in memory; its type is invalid.
        assert_eq!(
            unsafe { lance_arrow_schema_to_read(&invalid, &mut output) },
            -1
        );
        assert!(output.release.is_none());
    }

    #[test]
    fn namespace_json_and_dataset_schema_use_the_same_read_view() {
        let storage = Schema::new(vec![blob_field("video", true)]);
        let json = lance_namespace::schema::arrow_schema_to_json(&storage).unwrap();
        let json = std::ffi::CString::new(serde_json::to_string(&json).unwrap()).unwrap();
        let mut raw = FFI_ArrowSchema::empty();
        let mut read = FFI_ArrowSchema::empty();
        // SAFETY: CString remains alive; both FFI outputs are empty and owned.
        unsafe {
            assert_eq!(
                super::super::namespace::lance_json_arrow_schema_to_c(json.as_ptr(), &mut raw),
                0
            );
            assert_eq!(lance_arrow_schema_to_read(&raw, &mut read), 0);
        }
        assert_eq!(Schema::try_from(&raw).unwrap(), storage);
        assert_eq!(
            Schema::try_from(&read).unwrap(),
            *dataset_read_schema(&LanceSchema::try_from(&storage).unwrap())
        );

        #[cfg(feature = "vane-distributed")]
        {
            use super::super::vane_distributed_search::lance_vane_arrow_schema_fingerprint;
            let mut storage_fingerprint = [0; 32];
            let mut read_fingerprint = [0; 32];
            let mut original_fingerprint = [0; 32];
            let original = FFI_ArrowSchema::try_from(&storage).unwrap();
            // SAFETY: All schemas are live and outputs contain 32 writable bytes.
            unsafe {
                assert_eq!(
                    lance_vane_arrow_schema_fingerprint(&raw, storage_fingerprint.as_mut_ptr()),
                    0
                );
                assert_eq!(
                    lance_vane_arrow_schema_fingerprint(&read, read_fingerprint.as_mut_ptr()),
                    0
                );
                assert_eq!(
                    lance_vane_arrow_schema_fingerprint(
                        &original,
                        original_fingerprint.as_mut_ptr()
                    ),
                    0
                );
            }
            assert_eq!(storage_fingerprint, original_fingerprint);
            assert_ne!(storage_fingerprint, read_fingerprint);
        }
    }

    #[tokio::test]
    async fn fixture_read_schema_matches_actual_lance_scan() {
        let uri = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test/data/blob_v2_fixture.lance"
        );
        let dataset = Arc::new(Dataset::open(uri).await.unwrap());
        let handle = super::super::types::DatasetHandle::new(dataset.clone());
        let stream = dataset.scan().try_into_stream().await.unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            6
        );
        for batch in batches {
            assert_eq!(batch.schema().fields(), handle.read_arrow_schema.fields());
        }
        assert_eq!(
            handle
                .arrow_schema
                .field_with_name("video")
                .unwrap()
                .data_type(),
            &*BLOB_V2_USER_TYPE
        );
        assert_eq!(
            handle
                .read_arrow_schema
                .field_with_name("video")
                .unwrap()
                .data_type(),
            &*BLOB_V2_DESC_TYPE
        );
        assert_eq!(
            arrow_read_schema(&handle.arrow_schema).unwrap(),
            *handle.read_arrow_schema
        );
    }

    #[tokio::test]
    async fn descriptors_do_not_open_packed_or_dedicated_payload_files() {
        struct Scratch(std::path::PathBuf);
        impl Drop for Scratch {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        fn copy_without_payloads(source: &std::path::Path, dest: &std::path::Path) {
            std::fs::create_dir_all(dest).unwrap();
            for entry in std::fs::read_dir(source).unwrap() {
                let path = entry.unwrap().path();
                let target = dest.join(path.file_name().unwrap());
                if path.is_dir() {
                    copy_without_payloads(&path, &target);
                } else if path.extension().is_none_or(|ext| ext != "blob") {
                    std::fs::copy(&path, &target).unwrap();
                }
            }
        }
        let scratch = Scratch(
            std::env::temp_dir().join(format!("lance-descriptors-{}", rand::random::<u64>())),
        );
        let fixture = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/test/data/blob_v2_fixture.lance"
        ));
        copy_without_payloads(fixture, &scratch.0);
        let dataset = Dataset::open(scratch.0.to_str().unwrap()).await.unwrap();
        let mut scan = dataset.scan();
        scan.project(&["video"]).unwrap();
        let batches = scan
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            6
        );
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &*BLOB_V2_DESC_TYPE
        );
    }
}
