//! Private, read-only stores for Blob opens. No shared registry or inherited
//! dataset credentials may bypass the opener's access callbacks.

use std::collections::HashMap;
use std::ffi::{c_char, c_void, CString};
use std::fmt;
use std::ptr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use lance::session::Session;
use lance_io::object_store::{
    uri_to_url, ObjectStore, ObjectStoreParams, ObjectStoreProvider, ObjectStoreRegistry,
    StorageOptionsAccessor,
};
use object_store::{
    path::Path, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore as Store, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    StaticCredentialProvider,
};
use sha2::{Digest, Sha256};
use tokio::sync::OnceCell;
use url::Url;

use crate::error::{clear_last_error, set_last_error, ErrorCode};

use super::util::{cstr_to_str, explicit_aws_credentials, FfiError, FfiResult};

pub(super) const PROVIDER: u32 = 1;
pub(super) const LIST: u32 = 2;
pub(super) const METADATA: u32 = 3;
pub(super) const READ: u32 = 4;

#[repr(C)]
pub struct LanceBlobAccessResult {
    pub normalized_uri: *const c_char,
    pub option_keys: *const *const c_char,
    pub option_values: *const *const c_char,
    pub option_count: usize,
    pub safe_error: *const c_char,
    pub result_owner: *mut c_void,
}

impl Default for LanceBlobAccessResult {
    fn default() -> Self {
        Self {
            normalized_uri: ptr::null(),
            option_keys: ptr::null(),
            option_values: ptr::null(),
            option_count: 0,
            safe_error: ptr::null(),
            result_owner: ptr::null_mut(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LanceBlobAccessCallbacks {
    pub context: *mut c_void,
    pub retain: Option<unsafe extern "C" fn(*mut c_void)>,
    pub release: Option<unsafe extern "C" fn(*mut c_void)>,
    pub resolve: Option<
        unsafe extern "C" fn(*mut c_void, *const c_char, u32, *mut LanceBlobAccessResult) -> i32,
    >,
    pub release_result: Option<unsafe extern "C" fn(*mut c_void, *mut LanceBlobAccessResult)>,
}

// SAFETY: Creating an access handle requires an owned context whose retain and
// release are thread-safe and whose resolve/release_result may run on any thread.
// BlobAccessContext serializes the latter pair, including copying their results.
// The last Arc releases the context only after all stores/tasks have dropped it.
// Callbacks must not unwind or re-enter this access handle. A ClientContext or
// FileOpener borrowed from the initiating connection does not satisfy this ABI.
unsafe impl Send for LanceBlobAccessCallbacks {}
// SAFETY: Same ABI contract; the callbacks are only invoked under callback_lock.
unsafe impl Sync for LanceBlobAccessCallbacks {}

pub(super) struct BlobAccessContext {
    callbacks: LanceBlobAccessCallbacks,
    callback_lock: Mutex<()>,
}

impl fmt::Debug for BlobAccessContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobAccessContext")
    }
}

impl Drop for BlobAccessContext {
    fn drop(&mut self) {
        // SAFETY: create validated the callback table and retained this context.
        // No other Arc (and therefore no concurrent callback) remains.
        unsafe { (self.callbacks.release.unwrap())(self.callbacks.context) };
    }
}

impl BlobAccessContext {
    fn resolve(&self, target: &Url, operation: u32) -> FfiResult<HashMap<String, String>> {
        let _guard = self
            .callback_lock
            .lock()
            .map_err(|_| access_error("access callback lock is poisoned"))?;
        let target_string =
            CString::new(target.as_str()).map_err(|_| access_error("invalid access target"))?;
        let mut result = LanceBlobAccessResult::default();
        // SAFETY: The retained callback context is live; inputs and empty result
        // are valid throughout the call. The ABI forbids exceptions/unwinding.
        let status = unsafe {
            (self.callbacks.resolve.unwrap())(
                self.callbacks.context,
                target_string.as_ptr(),
                operation,
                &mut result,
            )
        };
        // Copy inside a closure so release_result runs on every returned error.
        let copied = (|| {
            if status != 0 {
                // Never forward an arbitrary provider/callback message: it can
                // contain credentials, headers or a signed URL, even on failure.
                return Err(access_error("access callback denied the backing operation"));
            }
            // SAFETY: The callback ABI supplies readable C strings until release_result.
            let normalized = unsafe { cstr_to_str(result.normalized_uri, "normalized_uri")? };
            if normalize_uri(normalized)? != *target {
                return Err(access_error("access callback changed the backing target"));
            }
            if result.option_count > 1024
                || (result.option_count != 0
                    && (result.option_keys.is_null() || result.option_values.is_null()))
            {
                return Err(access_error("invalid access callback options"));
            }
            let mut options = HashMap::new();
            for index in 0..result.option_count {
                // SAFETY: The callback owns two arrays of option_count C strings.
                let (key, value) = unsafe {
                    (
                        cstr_to_str(*result.option_keys.add(index), "option key")?,
                        cstr_to_str(*result.option_values.add(index), "option value")?,
                    )
                };
                options.insert(key.to_owned(), value.to_owned());
            }
            Ok(options)
        })();
        // SAFETY: Exactly one release, including errors and empty results, while
        // the context is live and the callback lock is held.
        unsafe { (self.callbacks.release_result.unwrap())(self.callbacks.context, &mut result) };
        copied.map_err(|_| access_error("access callback denied or returned an invalid result"))
    }
}

pub(super) fn access_error(message: &str) -> FfiError {
    FfiError::new(ErrorCode::BlobAccess, message)
}

/// Deliberately do not interpolate the underlying error in a Blob diagnostic.
/// Wrapped stores expose only these stable categories through Lance's scheduler.
pub(super) fn io_error(code: ErrorCode, error: &lance::Error, operation: &str) -> FfiError {
    io_error_with_not_found(code, code, error, operation)
}

pub(super) fn blob_open_error(error: &lance::Error) -> FfiError {
    // Only value retrieval may classify a missing backing object as a missing
    // Blob. Row-ID, fragment and deletion metadata are required to establish
    // whether a row exists, so their failures must retain the open-error code.
    io_error_with_not_found(
        ErrorCode::BlobOpen,
        ErrorCode::BlobNotFound,
        error,
        "open Blob row",
    )
}

fn io_error_with_not_found(
    code: ErrorCode,
    not_found_code: ErrorCode,
    error: &lance::Error,
    operation: &str,
) -> FfiError {
    let detail = error.to_string();
    let (code, reason) = if is_access_denied(error) || detail.contains("lance-blob: access denied")
    {
        (ErrorCode::BlobAccess, "backing access denied")
    } else if error.is_not_found()
        || matches!(
            error,
            lance::Error::DatasetNotFound { .. } | lance::Error::VersionNotFound { .. }
        )
        || detail.contains("lance-blob: not found")
    {
        (not_found_code, "backing object not found")
    } else {
        (code, "backing operation failed")
    };
    FfiError::new(code, format!("{operation}: {reason}"))
}

fn denied() -> lance::Error {
    store_denied().into()
}

fn store_denied() -> object_store::Error {
    object_store::Error::PermissionDenied {
        path: String::new(),
        source: "lance-blob: access denied".into(),
    }
}

fn is_access_denied(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if error
            .downcast_ref::<object_store::Error>()
            .is_some_and(|error| {
                matches!(
                    error,
                    object_store::Error::PermissionDenied { .. }
                        | object_store::Error::Unauthenticated { .. }
                )
            })
            || error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied)
            // object_store_opendal wraps PermissionDenied in Generic, retaining
            // the typed OpenDAL error as its source.
            || error
                .downcast_ref::<opendal::Error>()
                .is_some_and(|error| error.kind() == opendal::ErrorKind::PermissionDenied)
        {
            return true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

fn safe_store_error(error: object_store::Error) -> object_store::Error {
    // LocalFileSystem and OpenDAL wrap permission errors in Generic. Inspect
    // typed sources before discarding their possibly sensitive diagnostics.
    if is_access_denied(&error) {
        return store_denied();
    }
    match error {
        object_store::Error::NotFound { .. } => object_store::Error::NotFound {
            path: String::new(),
            source: "lance-blob: not found".into(),
        },
        _ => object_store::Error::Generic {
            store: "lance-blob",
            source: "lance-blob: backing I/O failed".into(),
        },
    }
}

fn read_only() -> object_store::Error {
    object_store::Error::NotSupported {
        source: "Blob stores are read-only".into(),
    }
}

pub(super) fn normalize_uri(uri: &str) -> FfiResult<Url> {
    if uri.is_empty()
        || uri.trim() != uri
        || uri.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        || uri.contains(['?', '#'])
        || (uri.contains("://") && Url::parse(uri).is_err())
    {
        return Err(access_error("invalid or credential-bearing Blob target"));
    }
    let mut url = uri_to_url(uri).map_err(|_| access_error("invalid Blob target"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !SUPPORTED_SCHEMES.contains(&url.scheme())
        // Storage providers ignore URI ports when selecting buckets/repos.
        // Network endpoint ports belong in the resolved storage options.
        || url.port().is_some()
        // The local provider can ignore an authority while extracting its
        // filesystem path. Never authorize a different identity for that path.
        || (matches!(url.scheme(), "file" | "file-object-store" | "file+uring")
            && !url.authority().is_empty())
    {
        return Err(access_error(
            "unsupported or credential-bearing Blob target",
        ));
    }
    if url.scheme() == "hf" {
        // OpenDAL accepts singular/plural and case-insensitive repository types.
        // Authorize and cache every spelling under the same HF identity.
        let repo_type = match url
            .host_str()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str()
        {
            "model" | "models" => "models",
            "dataset" | "datasets" => "datasets",
            "space" | "spaces" => "spaces",
            "bucket" | "buckets" => "buckets",
            _ => return Err(access_error("unsupported HF repository type")),
        };
        url.set_host(Some(repo_type))
            .map_err(|_| access_error("invalid HF repository type"))?;
        // Lance passes the encoded owner/repository directly to the HF HTTP
        // client. Decode those identity components once, before callbacks and
        // client construction. Object keys keep their separate Path semantics.
        let mut segments = url.path().trim_start_matches('/').splitn(3, '/');
        let owner = hf_identity_component(segments.next().unwrap_or_default())?;
        let repo = hf_identity_component(segments.next().unwrap_or_default())?;
        let path = match segments.next() {
            Some(path) => format!("/{owner}/{repo}/{path}"),
            None => format!("/{owner}/{repo}"),
        };
        url.set_path(&path);
    }
    Ok(url)
}

fn hf_identity_component(encoded: &str) -> FfiResult<String> {
    // These are HF's repository-name characters, not arbitrary object-key
    // characters. Reject encoded delimiters and nested escapes instead of
    // allowing an HTTP route or another decoding pass to reinterpret them.
    if encoded.to_ascii_lowercase().contains("%2f") {
        return Err(access_error("invalid HF repository identity"));
    }
    let component =
        Path::from_url_path(encoded).map_err(|_| access_error("invalid HF repository identity"))?;
    if component.is_root()
        || !component
            .as_ref()
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(access_error("invalid HF repository identity"));
    }
    Ok(component.to_string())
}

// An empty registry, populated only with mediated providers, fails closed for
// unknown schemes. s3+ddb requires an unmediated commit handler and is excluded.
const SUPPORTED_SCHEMES: &[&str] = &[
    "file",
    "file-object-store",
    "file+uring",
    "s3",
    "gs",
    "az",
    "oss",
    "hf",
];

#[no_mangle]
pub unsafe extern "C" fn lance_blob_access_create(
    callbacks: *const LanceBlobAccessCallbacks,
    out_access: *mut *mut c_void,
) -> i32 {
    match access_create_inner(callbacks, out_access) {
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

fn access_create_inner(
    callbacks: *const LanceBlobAccessCallbacks,
    out_access: *mut *mut c_void,
) -> FfiResult<()> {
    if out_access.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "out_access is null",
        ));
    }
    // SAFETY: The caller supplies an aligned, writable output pointer.
    unsafe { *out_access = ptr::null_mut() };
    if callbacks.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "callbacks are null",
        ));
    }
    // SAFETY: The caller supplies a valid callback table for this call.
    let callbacks = unsafe { *callbacks };
    if callbacks.context.is_null()
        || callbacks.retain.is_none()
        || callbacks.release.is_none()
        || callbacks.resolve.is_none()
        || callbacks.release_result.is_none()
    {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "incomplete access callbacks",
        ));
    }
    // SAFETY: All callbacks were validated and the caller owns a live context.
    unsafe { (callbacks.retain.unwrap())(callbacks.context) };
    let access = Arc::new(BlobAccessContext {
        callbacks,
        callback_lock: Mutex::new(()),
    });
    // SAFETY: The output pointer is valid; ownership transfers to the caller.
    unsafe { *out_access = Box::into_raw(Box::new(access)).cast() };
    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn lance_blob_access_free(access: *mut c_void) {
    if !access.is_null() {
        // SAFETY: The caller consumes this access-handle box exactly once.
        drop(unsafe { Box::from_raw(access.cast::<Arc<BlobAccessContext>>()) });
    }
    // Cleanup must preserve a caller-thread read/open error.
}

pub(super) unsafe fn clone_access(access: *mut c_void) -> FfiResult<Arc<BlobAccessContext>> {
    if access.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "access context is null",
        ));
    }
    // SAFETY: The caller owns a live access handle created by this module.
    Ok(unsafe { &*access.cast::<Arc<BlobAccessContext>>() }.clone())
}

pub(super) fn session(access: Arc<BlobAccessContext>) -> Arc<Session> {
    let defaults = ObjectStoreRegistry::default();
    let registry = ObjectStoreRegistry::empty();
    for scheme in SUPPORTED_SCHEMES {
        if let Some(inner) = defaults.get_provider(scheme) {
            registry.insert(
                scheme,
                Arc::new(BlobStoreProvider {
                    inner,
                    access: access.clone(),
                    resolutions: Default::default(),
                }),
            );
        }
    }
    // In particular, the historical manifest cannot come from a shared cache.
    Arc::new(Session::new(0, 0, Arc::new(registry)))
}

struct ResolvedStore {
    params: ObjectStoreParams,
    prefix: String,
    store: OnceCell<ObjectStore>,
    opendal: bool,
}

#[derive(Clone)]
struct BlobStoreProvider {
    inner: Arc<dyn ObjectStoreProvider>,
    access: Arc<BlobAccessContext>,
    resolutions: Arc<Mutex<HashMap<String, Arc<ResolvedStore>>>>,
}

impl fmt::Debug for BlobStoreProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobStoreProvider")
    }
}

impl BlobStoreProvider {
    fn resolve(&self, target: &StoreTarget, operation: u32) -> lance::Result<Arc<ResolvedStore>> {
        let url = &target.url;
        // Authorization precedes the upstream cache lookup and any delegation.
        if operation != PROVIDER {
            self.access.resolve(url, PROVIDER).map_err(|_| denied())?;
        }
        let options = self.access.resolve(url, operation).map_err(|_| denied())?;
        let mut resolutions = self.resolutions.lock().map_err(|_| denied())?;
        if let Some(resolved) = resolutions.get(url.as_str()) {
            return Ok(resolved.clone());
        }
        let opendal = matches!(url.scheme(), "hf" | "oss")
            || (matches!(url.scheme(), "s3" | "gs" | "az")
                && options
                    .get("use_opendal")
                    .is_some_and(|value| value == "true"));
        let credentials = explicit_aws_credentials(&options)
            .map(|credentials| Arc::new(StaticCredentialProvider::new(credentials)) as _);
        let params = ObjectStoreParams {
            // Start from defaults: never inherit dataset AWS credentials,
            // accessors, endpoints or a store override for an external target.
            aws_credentials: credentials,
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                options,
            ))),
            ..Default::default()
        };
        // Complete target separation also isolates same-bucket secret scopes.
        // Options are frozen for this open only; raw secrets never enter keys.
        let resolved = Arc::new(ResolvedStore {
            params,
            prefix: format!("blob${:x}", Sha256::digest(url.as_str().as_bytes())),
            store: OnceCell::new(),
            opendal,
        });
        resolutions.insert(url.to_string(), resolved.clone());
        Ok(resolved)
    }

    async fn backing_store(
        &self,
        target: &StoreTarget,
        resolved: &ResolvedStore,
    ) -> lance::Result<(ObjectStore, Path)> {
        // Validate the backend's interpretation before construction or I/O,
        // including operations that reuse a cached client.
        let path = target.backing_path(resolved.opendal)?;
        // Upstream registries bind a store to a base URI. Child objects can have
        // different secret scopes, so select their own upstream clients here.
        // Cache only raw clients: retaining a policy wrapper would form a cycle
        // back to this provider and keep the access context alive indefinitely.
        let store = resolved
            .store
            .get_or_try_init(|| async {
                let mut store = self
                    .inner
                    .new_store(target.backing_url(resolved.opendal)?, &resolved.params)
                    .await
                    .map_err(|error| {
                        if is_access_denied(&error) {
                            denied()
                        } else {
                            lance::Error::io("lance-blob: provider setup failed")
                        }
                    })?;
                store.store_prefix = resolved.prefix.clone();
                Ok::<_, lance::Error>(store)
            })
            .await
            .cloned()?;
        Ok((store, path))
    }
}

#[async_trait]
impl ObjectStoreProvider for BlobStoreProvider {
    fn extract_path(&self, url: &Url) -> lance::Result<Path> {
        Ok(StoreTarget::new(url, self.inner.as_ref())?.path)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _inherited_options: Option<&HashMap<String, String>>,
    ) -> lance::Result<String> {
        let target = StoreTarget::new(url, self.inner.as_ref())?;
        Ok(self.resolve(&target, PROVIDER)?.prefix.clone())
    }

    async fn new_store(
        &self,
        url: Url,
        _inherited: &ObjectStoreParams,
    ) -> lance::Result<ObjectStore> {
        let target = StoreTarget::new(&url, self.inner.as_ref())?;
        let resolved = self.resolve(&target, PROVIDER)?;
        let (mut store, _) = self.backing_store(&target, &resolved).await?;
        store.inner = Arc::new(BlobPolicyStore {
            target,
            provider: self.clone(),
        });
        Ok(store)
    }
}

// One parsed identity drives callbacks, credentials, caches and backend access.
// Reuse Lance's extraction and URL/Path encoding instead of parsing object keys
// independently in each operation. Only backend-specific adaptations live here.
#[derive(Clone, Debug)]
struct StoreTarget {
    url: Url,
    root: Url,
    path: Path,
}

impl StoreTarget {
    fn new(url: &Url, provider: &dyn ObjectStoreProvider) -> lance::Result<Self> {
        let url = normalize_uri(url.as_str()).map_err(|_| denied())?;
        let path = provider.extract_path(&url).map_err(|_| denied())?;
        // Rebuild from the provider's canonical path when its root is the URI
        // origin. In particular, filesystem extraction normalizes repeated
        // separators that a generic URL-path decoder would reject.
        let mut root = url.clone();
        root.set_path("");
        let mut target = Self { url, root, path };
        let canonical = target.uri(&target.path)?;
        if provider
            .extract_path(&canonical)
            .is_ok_and(|candidate| candidate == target.path)
        {
            target.url = canonical;
            return Ok(target);
        }
        // Providers such as HF also need a repository prefix in their root.
        // Match the decoded object path at an encoded URL segment boundary.
        // Counting decoded components would over-trim repository prefixes when
        // one URL segment contains encoded separators such as `dir%2Ftable`.
        let url_path = target.url.path();
        let prefix_len = std::iter::once(0)
            .chain(url_path.match_indices('/').map(|(index, _)| index + 1))
            .chain(std::iter::once(url_path.len()))
            .find(|&start| {
                Path::from_url_path(&url_path[start..]).is_ok_and(|suffix| suffix == target.path)
            })
            .ok_or_else(denied)?;
        target.root.set_path(&url_path[..prefix_len]);
        // Reject a mapping that cannot faithfully reproduce the upstream path.
        target.url = target.uri(&target.path)?;
        if provider.extract_path(&target.url).map_err(|_| denied())? != target.path {
            return Err(denied());
        }
        Ok(target)
    }

    fn uri(&self, path: &Path) -> lance::Result<Url> {
        let mut url = self.root.clone();
        url.path_segments_mut()
            .map_err(|_| denied())?
            .pop_if_empty()
            .extend(path.parts().map(|part| part.as_ref().to_owned()));
        Ok(url)
    }

    fn with_path(&self, path: &Path) -> lance::Result<Self> {
        Ok(Self {
            url: self.uri(path)?,
            root: self.root.clone(),
            path: path.clone(),
        })
    }

    fn backing_url(&self, opendal: bool) -> lance::Result<Url> {
        let mut url = self.url.clone();
        if matches!(url.scheme(), "file" | "file+uring") {
            // Disable upstream local fast paths, which bypass the wrapper.
            url = Url::parse(&format!(
                "file-object-store{}",
                &url[url::Position::AfterScheme..]
            ))
            .map_err(|_| denied())?;
        }
        if opendal && matches!(url.scheme(), "az" | "gs") {
            // These providers turn the URL path into an OpenDAL root. The
            // operation already carries the complete bucket-relative path.
            url.set_path("");
        }
        Ok(url)
    }

    fn backing_path(&self, opendal: bool) -> lance::Result<Path> {
        if !opendal {
            return Ok(self.path.clone());
        }
        // OpenDAL trims Unicode whitespace and normalizes separators on every
        // operation. Reject a changed object identity instead of authorizing
        // one name and accessing another. The empty Path denotes the root.
        if !self.path.is_root()
            && opendal::raw::normalize_path(self.path.as_ref()) != self.path.as_ref()
        {
            return Err(denied());
        }
        // object_store_opendal 0.57 decodes its input Path again. Preserve a
        // literal percent sequence by encoding it at this single boundary.
        Ok(Path::from(self.path.as_ref()))
    }
}

#[derive(Clone)]
struct BlobPolicyStore {
    target: StoreTarget,
    provider: BlobStoreProvider,
}

impl fmt::Debug for BlobPolicyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobPolicyStore")
    }
}

impl fmt::Display for BlobPolicyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BlobPolicyStore")
    }
}

struct BackingStore {
    inner: Arc<dyn Store>,
    path: Path,
    opendal: bool,
}

impl BackingStore {
    fn listed_path(&self, path: Path) -> object_store::Result<Path> {
        if self.opendal {
            // OpenDAL listing entries go through Path::from, which encodes raw
            // names. Restore object_store's path semantics before Lance reuses
            // the entries for another authorized operation.
            Path::from_url_path(path.as_ref()).map_err(|_| store_denied())
        } else {
            Ok(path)
        }
    }

    fn listed_meta(&self, mut meta: ObjectMeta) -> object_store::Result<ObjectMeta> {
        meta.location = self.listed_path(meta.location)?;
        Ok(meta)
    }
}

impl BlobPolicyStore {
    async fn backing_store(
        &self,
        path: &Path,
        operation: u32,
    ) -> object_store::Result<BackingStore> {
        let target = self.target.with_path(path).map_err(|_| store_denied())?;
        let resolved = self
            .provider
            .resolve(&target, operation)
            .map_err(|_| store_denied())?;
        self.provider
            .backing_store(&target, &resolved)
            .await
            .map(|(store, path)| BackingStore {
                inner: store.inner,
                path,
                opendal: resolved.opendal,
            })
            .map_err(|error| {
                safe_store_error(object_store::Error::Generic {
                    store: "lance-blob",
                    source: Box::new(error),
                })
            })
    }
}

#[async_trait]
impl Store for BlobPolicyStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let store = self
            .backing_store(location, if options.head { METADATA } else { READ })
            .await?;
        let head = options.head;
        let requested_range = options.range.clone();
        let mut result = store
            .inner
            .get_opts(&store.path, options)
            .await
            .map_err(safe_store_error)?;
        result.meta.location = location.clone();
        if head {
            return Ok(result);
        }
        // A backend may legally clip a bounded GET at its current EOF. Lance's
        // scheduler expects the exact range from the pinned file description,
        // so a clipped response is a short read even if its body agrees with
        // the returned Content-Range. Do not let it reach the scheduler.
        if matches!(requested_range, Some(object_store::GetRange::Bounded(range)) if range != result.range)
        {
            return Err(object_store::Error::Generic {
                store: "lance-blob",
                source: "lance-blob: short backing read".into(),
            });
        }
        // Lance's scheduler slices successful responses assuming their promised
        // length. Reject a short transport body before it reaches that code (and
        // before any caller buffer is touched). Collect only the requested range,
        // just as the upstream Reader would, without another byte copy/cache.
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let expected = range
            .end
            .checked_sub(range.start)
            .ok_or_else(|| safe_store_error(read_only()))?;
        let bytes = result.bytes().await.map_err(safe_store_error)?;
        if bytes.len() as u64 != expected {
            return Err(object_store::Error::Generic {
                store: "lance-blob",
                source: "lance-blob: short backing read".into(),
            });
        }
        Ok(GetResult {
            payload: object_store::GetResultPayload::Stream(
                stream::once(async { Ok(bytes) }).boxed(),
            ),
            meta,
            range,
            attributes,
        })
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let policy = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move {
            let store = policy
                .backing_store(&prefix.clone().unwrap_or_default(), LIST)
                .await?;
            Ok::<_, object_store::Error>(
                store
                    .inner
                    .list(prefix.as_ref().map(|_| &store.path))
                    .map(move |result| {
                        // The stream can outlive its caller and the original store.
                        let _lease = &policy;
                        result
                            .map_err(safe_store_error)
                            .and_then(|meta| store.listed_meta(meta))
                    })
                    .boxed(),
            )
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let store = self
            .backing_store(&prefix.cloned().unwrap_or_default(), LIST)
            .await?;
        let result = store
            .inner
            .list_with_delimiter(prefix.map(|_| &store.path))
            .await
            .map_err(safe_store_error)?;
        Ok(ListResult {
            common_prefixes: result
                .common_prefixes
                .into_iter()
                .map(|path| store.listed_path(path))
                .collect::<object_store::Result<_>>()?,
            objects: result
                .objects
                .into_iter()
                .map(|meta| store.listed_meta(meta))
                .collect::<object_store::Result<_>>()?,
        })
    }

    async fn put_opts(
        &self,
        _: &Path,
        _: PutPayload,
        _: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(read_only())
    }

    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(read_only())
    }

    fn delete_stream(
        &self,
        _: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        stream::once(async { Err(read_only()) }).boxed()
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> object_store::Result<()> {
        Err(read_only())
    }
}

#[cfg(test)]
#[path = "blob_store_tests.rs"]
pub(super) mod tests;
