//! Test-only owned callback implementation. It deliberately uses the C ABI so
//! result allocation, retain/release, worker-thread errors and teardown are tested.

use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr, CString};
use std::path::PathBuf;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::blob_store::*;

#[derive(Default)]
pub(super) struct Policy {
    pub calls: Mutex<Vec<(String, u32, std::thread::ThreadId)>>,
    pub deny: Mutex<Vec<(String, Option<u32>)>>,
    pub scopes: Mutex<Vec<(String, HashMap<String, String>)>>,
    pub retained: AtomicUsize,
    pub released: AtomicUsize,
    pub results_released: AtomicUsize,
    pub option_requests: AtomicUsize,
    pub provider_options_only: AtomicBool,
}

struct ResultOwner {
    uri: CString,
    _keys: Vec<CString>,
    _values: Vec<CString>,
    keys: Vec<*const c_char>,
    values: Vec<*const c_char>,
}

unsafe extern "C" fn retain(context: *mut c_void) {
    // SAFETY: Tests pass Arc::as_ptr for a live Policy and obey the callback ABI.
    unsafe {
        Arc::increment_strong_count(context.cast::<Policy>());
        (*context.cast::<Policy>())
            .retained
            .fetch_add(1, Ordering::SeqCst);
    }
}

unsafe extern "C" fn release(context: *mut c_void) {
    // SAFETY: Exactly one retained Arc is consumed by the last access lease.
    unsafe {
        (*context.cast::<Policy>())
            .released
            .fetch_add(1, Ordering::SeqCst);
        Arc::decrement_strong_count(context.cast::<Policy>());
    }
}

unsafe extern "C" fn resolve(
    context: *mut c_void,
    uri: *const c_char,
    operation: u32,
    resolve_options: bool,
    result: *mut LanceBlobAccessResult,
) -> i32 {
    // SAFETY: Rust supplies live callback arguments for this synchronous call.
    let (policy, uri, result) = unsafe {
        (
            &*context.cast::<Policy>(),
            CStr::from_ptr(uri).to_str().unwrap(),
            &mut *result,
        )
    };
    if resolve_options {
        policy.option_requests.fetch_add(1, Ordering::SeqCst);
    }
    policy
        .calls
        .lock()
        .unwrap()
        .push((uri.to_owned(), operation, std::thread::current().id()));
    if policy
        .deny
        .lock()
        .unwrap()
        .iter()
        .any(|(prefix, op)| uri.contains(prefix) && op.is_none_or(|op| op == operation))
    {
        // Include a sentinel secret to ensure callback text never leaks to TLS.
        result.safe_error = c"DENIED secret-key-must-not-leak https://host/?sig=private".as_ptr();
        return -1;
    }
    let mut options = policy
        .scopes
        .lock()
        .unwrap()
        .iter()
        .find(|(prefix, _)| uri.contains(prefix))
        .map(|(_, options)| options.clone())
        .unwrap_or_default();
    if (!resolve_options || operation != PROVIDER)
        && policy.provider_options_only.load(Ordering::SeqCst)
    {
        options.clear();
    }
    let (keys, values): (Vec<_>, Vec<_>) = options
        .into_iter()
        .map(|(key, value)| (CString::new(key).unwrap(), CString::new(value).unwrap()))
        .unzip();
    let owner = Box::new(ResultOwner {
        uri: CString::new(uri).unwrap(),
        keys: keys.iter().map(|key| key.as_ptr()).collect(),
        values: values.iter().map(|value| value.as_ptr()).collect(),
        _keys: keys,
        _values: values,
    });
    result.normalized_uri = owner.uri.as_ptr();
    result.option_keys = owner.keys.as_ptr();
    result.option_values = owner.values.as_ptr();
    result.option_count = owner.keys.len();
    result.result_owner = Box::into_raw(owner).cast();
    0
}

unsafe extern "C" fn release_result(context: *mut c_void, result: *mut LanceBlobAccessResult) {
    // SAFETY: One release for the initialized result from resolve above.
    unsafe {
        (*context.cast::<Policy>())
            .results_released
            .fetch_add(1, Ordering::SeqCst);
        if !(*result).result_owner.is_null() {
            drop(Box::from_raw((*result).result_owner.cast::<ResultOwner>()));
        }
        *result = LanceBlobAccessResult::default();
    }
}

pub(super) struct Access {
    pub policy: Arc<Policy>,
    pub handle: *mut c_void,
}

impl Access {
    pub fn new() -> Self {
        let policy = Arc::new(Policy::default());
        let callbacks = LanceBlobAccessCallbacks {
            context: Arc::as_ptr(&policy).cast_mut().cast(),
            retain: Some(retain),
            release: Some(release),
            resolve: Some(resolve),
            release_result: Some(release_result),
        };
        let mut handle = ptr::null_mut();
        // SAFETY: Arc-backed Policy and all callbacks satisfy the access ABI.
        assert_eq!(
            unsafe { lance_blob_access_create(&callbacks, &mut handle) },
            0
        );
        Self { policy, handle }
    }

    pub fn context(&self) -> Arc<BlobAccessContext> {
        // SAFETY: This owner keeps its access box live.
        unsafe { clone_access(self.handle).unwrap() }
    }

    pub fn deny(&self, prefix: &str, operation: Option<u32>) {
        self.policy
            .deny
            .lock()
            .unwrap()
            .push((prefix.to_owned(), operation));
    }

    pub fn call_count(&self) -> usize {
        self.policy.calls.lock().unwrap().len()
    }
}

impl Drop for Access {
    fn drop(&mut self) {
        // SAFETY: This owner frees its box once; stores keep independent Arcs.
        unsafe { lance_blob_access_free(self.handle) };
    }
}

pub(super) struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "lance-blob-pr2-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn assert_released(policy: &Policy) {
    // Upstream schedulers can briefly retain the store after delivering bytes.
    for _ in 0..200 {
        if policy.released.load(Ordering::SeqCst) == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(policy.retained.load(Ordering::SeqCst), 1);
    assert_eq!(policy.released.load(Ordering::SeqCst), 1);
    assert_eq!(
        policy.results_released.load(Ordering::SeqCst),
        policy.calls.lock().unwrap().len()
    );
}
