use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

use lance::dataset::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE};
use lance::session::Session;
use lance_core::cache::{CacheBackend, MokaCacheBackend};

use crate::error::{clear_last_error, set_last_error, ErrorCode};
#[cfg(feature = "vane-distributed")]
use crate::runtime;

use super::types::SessionHandle;
use super::util::{optional_session_handle, u64_to_usize, FfiError, FfiResult};
#[cfg(feature = "vane-distributed")]
use super::vane_index_cache::VaneIndexCacheBackend;

static DATASET_OPEN_COUNT: AtomicU64 = AtomicU64::new(0);
static NAMESPACE_DESCRIBE_COUNT: AtomicU64 = AtomicU64::new(0);
static COMMIT_COUNT: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
pub(crate) static DEBUG_COUNTER_TEST_LOCK: Mutex<()> = Mutex::new(());

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct LanceSessionStats {
    pub size_bytes: u64,
    pub approx_num_items: u64,
}

#[cfg(feature = "vane-distributed")]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct LanceVaneSessionCacheStats {
    pub index_hits: u64,
    pub index_misses: u64,
    pub index_num_entries: u64,
    pub index_size_bytes: u64,
    pub metadata_hits: u64,
    pub metadata_misses: u64,
    pub metadata_num_entries: u64,
    pub metadata_size_bytes: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct LanceDebugCounters {
    pub dataset_open_count: u64,
    pub namespace_describe_count: u64,
    pub commit_count: u64,
}

pub(crate) fn record_dataset_open() {
    DATASET_OPEN_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_namespace_describe() {
    NAMESPACE_DESCRIBE_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_commit() {
    COMMIT_COUNT.fetch_add(1, Ordering::Relaxed);
}

#[no_mangle]
pub unsafe extern "C" fn lance_create_session(
    index_cache_size_bytes: u64,
    metadata_cache_size_bytes: u64,
) -> *mut c_void {
    match create_session_inner(index_cache_size_bytes, metadata_cache_size_bytes) {
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

#[cfg(feature = "vane-distributed")]
#[no_mangle]
pub extern "C" fn lance_vane_default_index_cache_size_bytes() -> u64 {
    DEFAULT_INDEX_CACHE_SIZE as u64
}

#[cfg(feature = "vane-distributed")]
#[no_mangle]
pub extern "C" fn lance_vane_default_metadata_cache_size_bytes() -> u64 {
    DEFAULT_METADATA_CACHE_SIZE as u64
}

fn create_session_inner(
    index_cache_size_bytes: u64,
    metadata_cache_size_bytes: u64,
) -> FfiResult<SessionHandle> {
    let (index_cache_size_bytes, metadata_cache_size_bytes) =
        if index_cache_size_bytes == 0 && metadata_cache_size_bytes == 0 {
            (DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE)
        } else {
            (
                u64_to_usize(index_cache_size_bytes, "index_cache_size_bytes")?,
                u64_to_usize(metadata_cache_size_bytes, "metadata_cache_size_bytes")?,
            )
        };
    let bounded_index_cache: Arc<dyn CacheBackend> =
        Arc::new(MokaCacheBackend::with_capacity(index_cache_size_bytes));
    #[cfg(feature = "vane-distributed")]
    let (index_cache, vane_index_cache): (Arc<dyn CacheBackend>, Arc<VaneIndexCacheBackend>) = {
        let vane_index_cache = Arc::new(VaneIndexCacheBackend::new(bounded_index_cache));
        (vane_index_cache.clone(), vane_index_cache)
    };
    #[cfg(not(feature = "vane-distributed"))]
    let index_cache = bounded_index_cache;
    let session = Arc::new(Session::with_index_cache_backend(
        index_cache.clone(),
        metadata_cache_size_bytes,
        Default::default(),
    ));
    Ok(SessionHandle {
        session,
        index_cache,
        #[cfg(feature = "vane-distributed")]
        metadata_cache_size_bytes,
        #[cfg(feature = "vane-distributed")]
        vane_index_cache,
    })
}

fn clear_session_caches(handle: &SessionHandle) {
    if let Some(runtime) = crate::runtime::initialized_runtime() {
        runtime.block_on(async {
            handle.index_cache.clear().await;
            handle.session.file_metadata_cache().clear().await;
        });
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_close_session(session: *mut c_void) {
    if !session.is_null() {
        let handle = unsafe { Box::from_raw(session as *mut SessionHandle) };
        clear_session_caches(&handle);
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_session_get_stats(
    session: *mut c_void,
    out_stats: *mut LanceSessionStats,
) -> i32 {
    if !out_stats.is_null() {
        unsafe {
            std::ptr::write_unaligned(out_stats, LanceSessionStats::default());
        }
    }

    match session_get_stats_inner(session) {
        Ok(stats) => {
            clear_last_error();
            if !out_stats.is_null() {
                unsafe {
                    std::ptr::write_unaligned(out_stats, stats);
                }
            }
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

fn session_get_stats_inner(session: *mut c_void) -> FfiResult<LanceSessionStats> {
    let Some(session) = (unsafe { optional_session_handle(session)? }) else {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "session is null"));
    };
    Ok(LanceSessionStats {
        size_bytes: session.size_bytes(),
        approx_num_items: session.approx_num_items() as u64,
    })
}

#[cfg(feature = "vane-distributed")]
#[no_mangle]
pub unsafe extern "C" fn lance_vane_session_get_cache_stats(
    session: *mut c_void,
    out_stats: *mut LanceVaneSessionCacheStats,
) -> i32 {
    if !out_stats.is_null() {
        // SAFETY: The caller provides writable storage for the output structure.
        unsafe {
            std::ptr::write_unaligned(out_stats, LanceVaneSessionCacheStats::default());
        }
    }

    match vane_session_get_cache_stats_inner(session) {
        Ok(stats) => {
            clear_last_error();
            if !out_stats.is_null() {
                // SAFETY: The caller provides writable storage for the output structure.
                unsafe {
                    std::ptr::write_unaligned(out_stats, stats);
                }
            }
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

#[cfg(feature = "vane-distributed")]
fn vane_session_get_cache_stats_inner(
    session: *mut c_void,
) -> FfiResult<LanceVaneSessionCacheStats> {
    // SAFETY: A non-null pointer is owned by this library and points to a SessionHandle.
    let Some(session) = (unsafe { optional_session_handle(session)? }) else {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "session is null"));
    };
    let (index, metadata) = runtime::block_on(async {
        let index = session.index_cache_stats().await;
        let metadata = session.metadata_cache_stats().await;
        (index, metadata)
    })
    .map_err(|err| FfiError::new(ErrorCode::Runtime, format!("runtime: {err}")))?;
    Ok(LanceVaneSessionCacheStats {
        index_hits: index.hits,
        index_misses: index.misses,
        index_num_entries: index.num_entries as u64,
        index_size_bytes: index.size_bytes as u64,
        metadata_hits: metadata.hits,
        metadata_misses: metadata.misses,
        metadata_num_entries: metadata.num_entries as u64,
        metadata_size_bytes: metadata.size_bytes as u64,
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_debug_get_counters(out_counters: *mut LanceDebugCounters) -> i32 {
    if out_counters.is_null() {
        set_last_error(ErrorCode::InvalidArgument, "out_counters is null");
        return -1;
    }

    clear_last_error();
    let counters = LanceDebugCounters {
        dataset_open_count: DATASET_OPEN_COUNT.load(Ordering::Relaxed),
        namespace_describe_count: NAMESPACE_DESCRIBE_COUNT.load(Ordering::Relaxed),
        commit_count: COMMIT_COUNT.load(Ordering::Relaxed),
    };
    unsafe {
        std::ptr::write_unaligned(out_counters, counters);
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn lance_debug_reset_counters() {
    DATASET_OPEN_COUNT.store(0, Ordering::Relaxed);
    NAMESPACE_DESCRIBE_COUNT.store(0, Ordering::Relaxed);
    COMMIT_COUNT.store(0, Ordering::Relaxed);
    clear_last_error();
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "vane-distributed")]
    use std::ffi::CStr;
    use std::ffi::CString;
    use std::fs;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::WriteParams;
    use lance::Dataset;

    use crate::runtime;

    use super::super::dataset::{lance_close_dataset, lance_open_dataset_with_session};
    use super::*;

    #[test]
    fn test_create_session_and_get_stats() {
        unsafe {
            let session = lance_create_session(0, 0);
            assert!(!session.is_null());

            let mut stats = LanceSessionStats::default();
            assert_eq!(lance_session_get_stats(session, &mut stats), 0);
            assert_eq!(stats.approx_num_items, 0);

            lance_close_session(session);
        }
    }

    #[test]
    fn test_open_dataset_with_session_records_debug_counters() {
        let _counter_guard = DEBUG_COUNTER_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let dataset_dir =
            std::env::temp_dir().join(format!("ffi-session-{}", rand::random::<u64>()));
        let uri = dataset_dir.to_string_lossy().to_string();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);

        unsafe {
            lance_debug_reset_counters();
            let session = lance_create_session(0, 0);
            assert!(!session.is_null());

            runtime::block_on(Dataset::write(reader, &uri, Some(WriteParams::default())))
                .unwrap()
                .unwrap();

            let uri_c = CString::new(uri.clone()).unwrap();
            let first = lance_open_dataset_with_session(uri_c.as_ptr(), session);
            assert!(!first.is_null());
            lance_close_dataset(first);

            let second = lance_open_dataset_with_session(uri_c.as_ptr(), session);
            assert!(!second.is_null());
            lance_close_dataset(second);

            let mut counters = LanceDebugCounters::default();
            assert_eq!(lance_debug_get_counters(&mut counters), 0);
            assert_eq!(counters.dataset_open_count, 2);

            let mut stats = LanceSessionStats::default();
            assert_eq!(lance_session_get_stats(session, &mut stats), 0);

            lance_close_session(session);
        }

        let _ = fs::remove_dir_all(dataset_dir);
    }

    #[cfg(feature = "vane-distributed")]
    #[test]
    fn test_versioned_open_revalidates_manifest_with_shared_session() {
        let _counter_guard = DEBUG_COUNTER_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let dataset_dir =
            std::env::temp_dir().join(format!("ffi-versioned-session-{}", rand::random::<u64>()));
        let uri = dataset_dir.to_string_lossy().to_string();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let first_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let first_reader =
            RecordBatchIterator::new(vec![Ok(first_batch)].into_iter(), schema.clone());
        let first = runtime::block_on(Dataset::write(
            first_reader,
            &uri,
            Some(WriteParams::default()),
        ))
        .unwrap()
        .unwrap();
        let frozen_version = first.version_id();
        assert_eq!(frozen_version, 1);
        drop(first);

        unsafe {
            lance_debug_reset_counters();
            let session = lance_create_session(0, 0);
            assert!(!session.is_null());
            assert!(lance_vane_default_index_cache_size_bytes() > 0);
            assert!(lance_vane_default_metadata_cache_size_bytes() > 0);

            let uri_c = CString::new(uri.clone()).unwrap();
            // Seed the persistent Session with dataset A's version-1 manifest.
            let first_open = lance_open_dataset_with_session(uri_c.as_ptr(), session);
            assert!(!first_open.is_null());
            assert_eq!(
                super::super::dataset::lance_dataset_version(first_open),
                frozen_version
            );
            let first_generation_ptr =
                super::super::dataset::lance_dataset_generation_id(first_open);
            assert!(!first_generation_ptr.is_null());
            let first_generation = CStr::from_ptr(first_generation_ptr)
                .to_str()
                .unwrap()
                .to_string();
            crate::error::lance_free_string(first_generation_ptr);
            lance_close_dataset(first_open);

            fs::remove_dir_all(&dataset_dir).unwrap();
            let replacement_batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![4, 5]))])
                    .unwrap();
            let replacement_reader =
                RecordBatchIterator::new(vec![Ok(replacement_batch)].into_iter(), schema);
            let replacement = runtime::block_on(Dataset::write(
                replacement_reader,
                &uri,
                Some(WriteParams::default()),
            ))
            .unwrap()
            .unwrap();
            assert_eq!(replacement.version_id(), frozen_version);
            drop(replacement);

            let replacement_open =
                super::super::dataset::lance_vane_open_dataset_version_with_session(
                    uri_c.as_ptr(),
                    frozen_version,
                    session,
                );
            assert!(!replacement_open.is_null());
            assert_eq!(
                super::super::dataset::lance_dataset_version(replacement_open),
                frozen_version
            );
            let replacement_generation_ptr =
                super::super::dataset::lance_dataset_generation_id(replacement_open);
            assert!(!replacement_generation_ptr.is_null());
            let replacement_generation = CStr::from_ptr(replacement_generation_ptr)
                .to_str()
                .unwrap()
                .to_string();
            crate::error::lance_free_string(replacement_generation_ptr);
            assert_ne!(replacement_generation, first_generation);

            let session_handle = &*(session as *const super::super::types::SessionHandle);
            let dataset_handle = &*(replacement_open as *const super::super::types::DatasetHandle);
            assert!(Arc::ptr_eq(
                &dataset_handle.dataset.session(),
                &session_handle.session
            ));

            let mut counters = LanceDebugCounters::default();
            assert_eq!(lance_debug_get_counters(&mut counters), 0);
            assert_eq!(counters.dataset_open_count, 2);

            lance_close_dataset(replacement_open);
            lance_close_session(session);
        }

        let _ = fs::remove_dir_all(dataset_dir);
    }
}
