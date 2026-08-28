use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
#[cfg(feature = "vane-distributed")]
use std::fmt;
use std::ptr;
use std::sync::Arc;

use lance::dataset::builder::DatasetBuilder;
#[cfg(feature = "vane-distributed")]
use lance::session::Session;
use lance_core::Error as LanceError;
#[cfg(feature = "vane-distributed")]
use lance_io::object_store::providers::{aws::AwsStoreProvider, ObjectStoreProvider};
#[cfg(feature = "vane-distributed")]
use lance_io::object_store::{ObjectStore, ObjectStoreParams};
use lance_namespace::models::{DropTableRequest, ListTablesRequest};
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;
#[cfg(feature = "vane-distributed")]
use object_store::aws::AwsCredentialProvider;
#[cfg(feature = "vane-distributed")]
use object_store::path::Path as ObjectStorePath;
#[cfg(feature = "vane-distributed")]
use url::Url;

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;

use super::session::record_dataset_open;
use super::types::DatasetHandle;
use super::util::{
    cstr_to_str, optional_session_handle, slice_from_ptr, to_c_string, FfiError, FfiResult,
};
#[cfg(feature = "vane-distributed")]
use super::util::{
    explicit_aws_credentials, vane_external_location_error, with_explicit_aws_credentials,
};

#[cfg(feature = "vane-distributed")]
struct StaticAwsStoreProvider {
    delegate: Arc<dyn ObjectStoreProvider>,
    credentials: AwsCredentialProvider,
}

#[cfg(feature = "vane-distributed")]
impl fmt::Debug for StaticAwsStoreProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StaticAwsStoreProvider")
            .field("credentials", &"<redacted>")
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "vane-distributed")]
#[async_trait::async_trait]
impl ObjectStoreProvider for StaticAwsStoreProvider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> Result<ObjectStore, LanceError> {
        if params
            .storage_options()
            .and_then(|options| options.get("use_opendal"))
            .is_some_and(|value| value.eq_ignore_ascii_case("true"))
        {
            return Err(LanceError::invalid_input(
                "Vane-replayed static AWS credentials do not support use_opendal=true",
            ));
        }
        let mut params = params.clone();
        params.aws_credentials = Some(self.credentials.clone());
        self.delegate.new_store(base_path, &params).await
    }

    fn extract_path(&self, url: &Url) -> Result<ObjectStorePath, LanceError> {
        self.delegate.extract_path(url)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String, LanceError> {
        self.delegate
            .calculate_object_store_prefix(url, storage_options)
    }
}

#[cfg(feature = "vane-distributed")]
pub(super) fn with_explicit_aws_namespace_session(
    builder: DirectoryNamespaceBuilder,
    storage_options: &HashMap<String, String>,
) -> DirectoryNamespaceBuilder {
    let Some(credentials) = explicit_aws_credentials(storage_options) else {
        return builder;
    };
    let session = Arc::new(Session::default());
    let registry = session.store_registry();
    let credentials = Arc::new(object_store::StaticCredentialProvider::new(credentials));
    let provider = Arc::new(StaticAwsStoreProvider {
        delegate: Arc::new(AwsStoreProvider),
        credentials,
    });
    registry.insert("s3", provider.clone());
    registry.insert("s3+ddb", provider);
    builder.session(session)
}

fn parse_storage_options(
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
    Ok(storage_options)
}

fn dir_namespace_list_tables_inner(
    root: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
) -> FfiResult<Vec<String>> {
    let root = unsafe { cstr_to_str(root, "root")? };
    let storage_options = parse_storage_options(option_keys, option_values, options_len)?;

    let tables = runtime::block_on(async move {
        let mut builder = DirectoryNamespaceBuilder::new(root).manifest_enabled(false);
        #[cfg(feature = "vane-distributed")]
        {
            if !storage_options.is_empty() {
                builder = builder.storage_options(storage_options.clone());
            }
            builder = with_explicit_aws_namespace_session(builder, &storage_options);
        }
        #[cfg(not(feature = "vane-distributed"))]
        if !storage_options.is_empty() {
            builder = builder.storage_options(storage_options);
        }
        let namespace = builder.build().await.map_err(|err| {
            #[cfg(feature = "vane-distributed")]
            {
                vane_external_location_error(
                    ErrorCode::DirNamespaceListTables,
                    "dir namespace build",
                    root,
                    err,
                )
            }
            #[cfg(not(feature = "vane-distributed"))]
            FfiError::new(
                ErrorCode::DirNamespaceListTables,
                format!("dir namespace build '{root}': {err}"),
            )
        })?;

        let mut req = ListTablesRequest::new();
        req.id = Some(Vec::new());
        let resp = namespace.list_tables(req).await.map_err(|err| {
            #[cfg(feature = "vane-distributed")]
            {
                vane_external_location_error(
                    ErrorCode::DirNamespaceListTables,
                    "dir namespace list_tables",
                    root,
                    err,
                )
            }
            #[cfg(not(feature = "vane-distributed"))]
            FfiError::new(
                ErrorCode::DirNamespaceListTables,
                format!("dir namespace list_tables '{root}': {err}"),
            )
        })?;
        Ok::<_, FfiError>(resp.tables)
    })
    .map_err(|err| FfiError::new(ErrorCode::Runtime, format!("runtime: {err}")))??;

    Ok(tables)
}

#[no_mangle]
pub unsafe extern "C" fn lance_dir_namespace_list_tables(
    root: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
) -> *const c_char {
    match dir_namespace_list_tables_inner(root, option_keys, option_values, options_len) {
        Ok(tables) => {
            clear_last_error();
            let joined = tables.join("\n");
            to_c_string(joined).into_raw() as *const c_char
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null()
        }
    }
}

fn open_dataset_in_dir_namespace_inner(
    root: *const c_char,
    table_name: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
) -> FfiResult<(DatasetHandle, String)> {
    let root = unsafe { cstr_to_str(root, "root")? }
        .trim_end_matches('/')
        .to_string();
    let table_name = unsafe { cstr_to_str(table_name, "table_name")? };
    let storage_options = parse_storage_options(option_keys, option_values, options_len)?;
    let session = unsafe { optional_session_handle(session)? };

    let dataset = runtime::block_on(async {
        let mut ns_builder = DirectoryNamespaceBuilder::new(&root).manifest_enabled(false);
        #[cfg(feature = "vane-distributed")]
        {
            if !storage_options.is_empty() {
                ns_builder = ns_builder.storage_options(storage_options.clone());
            }
            ns_builder = with_explicit_aws_namespace_session(ns_builder, &storage_options);
        }
        #[cfg(not(feature = "vane-distributed"))]
        if !storage_options.is_empty() {
            // The same map is also consumed by DatasetBuilder below. Both the
            // namespace discovery and dataset open paths need these options.
            ns_builder = ns_builder.storage_options(storage_options.clone());
        }
        let namespace = ns_builder.build().await.map_err(|err| {
            #[cfg(feature = "vane-distributed")]
            {
                vane_external_location_error(
                    ErrorCode::DatasetOpen,
                    "dir namespace build",
                    &root,
                    err,
                )
            }
            #[cfg(not(feature = "vane-distributed"))]
            FfiError::new(
                ErrorCode::DatasetOpen,
                format!("dir namespace build '{root}': {err}"),
            )
        })?;
        let mut builder =
            DatasetBuilder::from_namespace(Arc::new(namespace), vec![table_name.to_string()])
                .await
                .map_err(|err| {
                    #[cfg(feature = "vane-distributed")]
                    {
                        vane_external_location_error(
                            ErrorCode::DatasetOpen,
                            "dir namespace describe",
                            &format!("{root}/{table_name}"),
                            err,
                        )
                    }
                    #[cfg(not(feature = "vane-distributed"))]
                    FfiError::new(
                        ErrorCode::DatasetOpen,
                        format!("dir namespace describe '{root}/{table_name}': {err}"),
                    )
                })?;
        #[cfg(feature = "vane-distributed")]
        {
            builder = with_explicit_aws_credentials(builder, &storage_options);
        }
        // Forward the caller's storage_options to the dataset open path.
        //
        // `DirectoryNamespace::describe_table` returns `storage_options: None`
        // when no credential vendor is configured, by design — to avoid
        // leaking the namespace's own static credentials to clients (see
        // lance-namespace-impls/src/dir.rs::get_storage_options_for_table).
        // Without this re-injection, `builder.load()` builds an ObjectStore
        // with empty options. For non-AWS S3-compatible endpoints (TOS,
        // MinIO, R2, ...) this falls into AWS bucket-region resolution
        // (lance-io/src/object_store/providers/aws.rs:217) and fails with
        // "Bucket '<name>' not found".
        //
        // `with_storage_options` merges into any accessor already attached
        // by `from_namespace`, preserving a `StorageOptionsProvider` if one
        // was returned by a credential vendor.
        if !storage_options.is_empty() {
            builder = builder.with_storage_options(storage_options);
        }
        if let Some(session) = session {
            builder = builder.with_session(session);
        }
        builder.load().await.map_err(|err| {
            #[cfg(feature = "vane-distributed")]
            {
                vane_external_location_error(
                    ErrorCode::DatasetOpen,
                    "dir namespace dataset open",
                    &root,
                    err,
                )
            }
            #[cfg(not(feature = "vane-distributed"))]
            FfiError::new(
                ErrorCode::DatasetOpen,
                format!("dir namespace dataset open: {err}"),
            )
        })
    })
    .map_err(|err| FfiError::new(ErrorCode::Runtime, format!("runtime: {err}")))??;

    let table_uri = dataset.uri().to_string();
    record_dataset_open();
    Ok((DatasetHandle::new(Arc::new(dataset)), table_uri))
}

#[no_mangle]
pub unsafe extern "C" fn lance_open_dataset_in_dir_namespace(
    root: *const c_char,
    table_name: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    out_table_uri: *mut *const c_char,
) -> *mut c_void {
    if !out_table_uri.is_null() {
        unsafe {
            std::ptr::write_unaligned(out_table_uri, ptr::null());
        }
    }

    match open_dataset_in_dir_namespace_inner(
        root,
        table_name,
        option_keys,
        option_values,
        options_len,
        ptr::null_mut(),
    ) {
        Ok((handle, table_uri)) => {
            clear_last_error();
            if !out_table_uri.is_null() {
                let uri_c = CString::new(table_uri).unwrap_or_else(|_| to_c_string("invalid uri"));
                unsafe {
                    std::ptr::write_unaligned(out_table_uri, uri_c.into_raw() as *const c_char);
                }
            }
            Box::into_raw(Box::new(handle)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_open_dataset_in_dir_namespace_with_session(
    root: *const c_char,
    table_name: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
    session: *mut c_void,
    out_table_uri: *mut *const c_char,
) -> *mut c_void {
    if !out_table_uri.is_null() {
        unsafe {
            std::ptr::write_unaligned(out_table_uri, ptr::null());
        }
    }

    match open_dataset_in_dir_namespace_inner(
        root,
        table_name,
        option_keys,
        option_values,
        options_len,
        session,
    ) {
        Ok((handle, table_uri)) => {
            clear_last_error();
            if !out_table_uri.is_null() {
                let uri_c = CString::new(table_uri).unwrap_or_else(|_| to_c_string("invalid uri"));
                unsafe {
                    std::ptr::write_unaligned(out_table_uri, uri_c.into_raw() as *const c_char);
                }
            }
            Box::into_raw(Box::new(handle)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

fn dir_namespace_drop_table_inner(
    root: *const c_char,
    table_name: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
) -> FfiResult<()> {
    let root = unsafe { cstr_to_str(root, "root")? };
    let table_name = unsafe { cstr_to_str(table_name, "table_name")? };
    let storage_options = parse_storage_options(option_keys, option_values, options_len)?;

    runtime::block_on(async move {
        let mut builder = DirectoryNamespaceBuilder::new(root).manifest_enabled(false);
        if !storage_options.is_empty() {
            builder = builder.storage_options(storage_options);
        }
        let namespace = builder.build().await.map_err(|err| {
            FfiError::new(
                ErrorCode::DirNamespaceDropTable,
                format!("dir namespace build '{root}': {err}"),
            )
        })?;

        let mut req = DropTableRequest::new();
        req.id = Some(vec![table_name.to_string()]);

        match namespace.drop_table(req).await {
            Ok(_) => Ok(()),
            Err(LanceError::NotFound { .. }) => Ok(()),
            Err(err) => Err(FfiError::new(
                ErrorCode::DirNamespaceDropTable,
                format!("dir namespace drop_table '{root}/{table_name}': {err}"),
            )),
        }
    })
    .map_err(|err| FfiError::new(ErrorCode::Runtime, format!("runtime: {err}")))?
}

#[no_mangle]
pub unsafe extern "C" fn lance_dir_namespace_drop_table(
    root: *const c_char,
    table_name: *const c_char,
    option_keys: *const *const c_char,
    option_values: *const *const c_char,
    options_len: usize,
) -> i32 {
    match dir_namespace_drop_table_inner(root, table_name, option_keys, option_values, options_len)
    {
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

#[cfg(all(test, feature = "vane-distributed"))]
mod tests {
    use std::sync::Mutex;

    use lance_io::object_store::StorageOptionsAccessor;
    use object_store::aws::AwsCredential;

    use super::*;

    #[derive(Debug)]
    struct CaptureStoreProvider {
        credentials: Arc<Mutex<Option<AwsCredentialProvider>>>,
    }

    #[async_trait::async_trait]
    impl ObjectStoreProvider for CaptureStoreProvider {
        async fn new_store(
            &self,
            _base_path: Url,
            params: &ObjectStoreParams,
        ) -> Result<ObjectStore, LanceError> {
            *self.credentials.lock().unwrap() = params.aws_credentials.clone();
            Err(LanceError::invalid_input("captured object-store params"))
        }
    }

    fn static_provider(delegate: Arc<dyn ObjectStoreProvider>) -> StaticAwsStoreProvider {
        let credentials = AwsCredential {
            key_id: "test-access".to_string(),
            secret_key: "test-secret".to_string(),
            token: None,
        };
        StaticAwsStoreProvider {
            delegate,
            credentials: Arc::new(object_store::StaticCredentialProvider::new(credentials)),
        }
    }

    #[test]
    fn static_provider_injects_tokenless_credentials_and_redacts_debug() {
        let captured = Arc::new(Mutex::new(None));
        let provider = static_provider(Arc::new(CaptureStoreProvider {
            credentials: captured.clone(),
        }));
        let options = HashMap::from([("aws_session_token".to_string(), String::new())]);
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                options,
            ))),
            ..Default::default()
        };

        let result =
            runtime::block_on(provider.new_store(Url::parse("s3://bucket/root").unwrap(), &params))
                .unwrap();
        assert!(result.is_err());
        let credentials = captured.lock().unwrap().clone().unwrap();
        let credentials = runtime::block_on(credentials.get_credential())
            .unwrap()
            .unwrap();
        assert_eq!(credentials.key_id, "test-access");
        assert_eq!(credentials.secret_key, "test-secret");
        assert_eq!(credentials.token, None);

        let debug = format!("{provider:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("test-access"));
        assert!(!debug.contains("test-secret"));
    }

    #[test]
    fn static_provider_rejects_opendal_before_delegation() {
        let captured = Arc::new(Mutex::new(None));
        let provider = static_provider(Arc::new(CaptureStoreProvider {
            credentials: captured.clone(),
        }));
        let options = HashMap::from([("use_opendal".to_string(), "true".to_string())]);
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                options,
            ))),
            ..Default::default()
        };

        let error =
            runtime::block_on(provider.new_store(Url::parse("s3://bucket/root").unwrap(), &params))
                .unwrap()
                .unwrap_err();
        assert!(error.to_string().contains("use_opendal=true"));
        assert!(captured.lock().unwrap().is_none());
    }
}
