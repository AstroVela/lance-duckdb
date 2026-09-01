use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use async_trait::async_trait;
use futures::Future;
use lance_core::cache::{
    CacheBackend, CacheCodec, CacheEntry, CacheKey, CacheKeyIterator, DeepSizeOf, InternalCacheKey,
};

#[derive(Clone, Debug)]
struct PinnedEntry {
    value: CacheEntry,
    size_bytes: usize,
    leases: usize,
}

/// Adds query-lifetime, non-evictable entries in front of Lance's configured
/// bounded index cache. Only coordinator-frozen snapshot metadata is pinned;
/// every other Lance cache entry remains subject to the configured capacity.
pub(crate) struct VaneIndexCacheBackend {
    bounded: Arc<dyn CacheBackend>,
    pinned: Mutex<HashMap<InternalCacheKey, PinnedEntry>>,
}

impl std::fmt::Debug for VaneIndexCacheBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VaneIndexCacheBackend")
            .field("bounded", &self.bounded)
            .field("pinned_entries", &self.lock_pinned().len())
            .finish()
    }
}

impl VaneIndexCacheBackend {
    pub(crate) fn new(bounded: Arc<dyn CacheBackend>) -> Self {
        Self {
            bounded,
            pinned: Mutex::new(HashMap::new()),
        }
    }

    fn lock_pinned(&self) -> MutexGuard<'_, HashMap<InternalCacheKey, PinnedEntry>> {
        self.pinned
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn internal_key<K>(prefix: &str, key: &K) -> InternalCacheKey
    where
        K: CacheKey,
    {
        InternalCacheKey::new(
            Arc::from(prefix),
            Arc::from(key.key().into_owned()),
            K::type_name(),
        )
    }

    fn entry_size<T>(value: &T) -> usize
    where
        T: DeepSizeOf + ?Sized,
    {
        value.deep_size_of() + std::mem::size_of::<std::sync::atomic::AtomicUsize>() * 2
    }

    pub(crate) fn get_pinned_with_key<K>(&self, prefix: &str, key: &K) -> Option<Arc<K::ValueType>>
    where
        K: CacheKey,
        K::ValueType: Send + Sync + 'static,
    {
        self.lock_pinned()
            .get(&Self::internal_key(prefix, key))
            .map(|entry| entry.value.clone())
            .and_then(|entry| entry.downcast::<K::ValueType>().ok())
    }

    pub(crate) fn pin_with_key<K>(
        self: &Arc<Self>,
        prefix: &str,
        key: &K,
        value: Arc<K::ValueType>,
    ) -> VaneIndexCacheLease
    where
        K: CacheKey,
        K::ValueType: DeepSizeOf + Send + Sync + 'static,
    {
        let key = Self::internal_key(prefix, key);
        let size_bytes = Self::entry_size(value.as_ref());
        let value: CacheEntry = value;
        let mut pinned = self.lock_pinned();
        pinned
            .entry(key.clone())
            .and_modify(|entry| {
                entry.value = value.clone();
                entry.size_bytes = size_bytes;
                entry.leases += 1;
            })
            .or_insert(PinnedEntry {
                value,
                size_bytes,
                leases: 1,
            });
        VaneIndexCacheLease {
            cache: Arc::downgrade(self),
            key,
        }
    }

    fn unpin(&self, key: &InternalCacheKey) {
        let mut pinned = self.lock_pinned();
        if let Some(entry) = pinned.get_mut(key) {
            entry.leases -= 1;
            if entry.leases == 0 {
                pinned.remove(key);
            }
        }
    }

    fn pinned_entry(&self, key: &InternalCacheKey) -> Option<CacheEntry> {
        self.lock_pinned().get(key).map(|entry| entry.value.clone())
    }

    fn contains_pinned(&self, key: &InternalCacheKey) -> bool {
        self.lock_pinned().contains_key(key)
    }

    #[cfg(test)]
    pub(crate) fn pinned_entry_count(&self) -> usize {
        self.lock_pinned().len()
    }
}

#[derive(Debug)]
pub(crate) struct VaneIndexCacheLease {
    cache: Weak<VaneIndexCacheBackend>,
    key: InternalCacheKey,
}

impl Drop for VaneIndexCacheLease {
    fn drop(&mut self) {
        if let Some(cache) = self.cache.upgrade() {
            cache.unpin(&self.key);
        }
    }
}

#[async_trait]
impl CacheBackend for VaneIndexCacheBackend {
    async fn get(&self, key: &InternalCacheKey, codec: Option<CacheCodec>) -> Option<CacheEntry> {
        if let Some(entry) = self.pinned_entry(key) {
            return Some(entry);
        }
        let bounded = self.bounded.get(key, codec).await;
        self.pinned_entry(key).or(bounded)
    }

    async fn insert(
        &self,
        key: &InternalCacheKey,
        entry: CacheEntry,
        size_bytes: usize,
        codec: Option<CacheCodec>,
    ) {
        if self.contains_pinned(key) {
            // Lance can write inferred legacy-index details back into the
            // cache. Keep the coordinator's canonical raw IndexSection pinned
            // so subsequent loads infer and apply fragment reuse exactly once.
            return;
        }
        self.bounded.insert(key, entry, size_bytes, codec).await;
    }

    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: Pin<Box<dyn Future<Output = lance_core::Result<(CacheEntry, usize)>> + Send + 'a>>,
        codec: Option<CacheCodec>,
    ) -> lance_core::Result<(CacheEntry, bool)> {
        if let Some(entry) = self.pinned_entry(key) {
            return Ok((entry, true));
        }
        let bounded = self.bounded.get_or_insert(key, loader, codec).await?;
        Ok(self
            .pinned_entry(key)
            .map(|entry| (entry, true))
            .unwrap_or(bounded))
    }

    async fn invalidate_prefix(&self, prefix: &str) {
        // Active frozen snapshots own their pins and must survive unrelated
        // cache invalidations. Dropping the DatasetHandle releases the pin.
        self.bounded.invalidate_prefix(prefix).await;
    }

    async fn clear(&self) {
        self.lock_pinned().clear();
        self.bounded.clear().await;
    }

    async fn keys(&self) -> Option<CacheKeyIterator<'_>> {
        let mut keys: HashSet<_> = self.lock_pinned().keys().cloned().collect();
        let bounded = self.bounded.keys().await?;
        keys.extend(bounded);
        Some(Box::new(keys.into_iter()))
    }

    async fn num_entries(&self) -> usize {
        self.bounded.num_entries().await + self.lock_pinned().len()
    }

    async fn size_bytes(&self) -> usize {
        self.bounded.size_bytes().await
            + self
                .lock_pinned()
                .values()
                .map(|entry| entry.size_bytes)
                .sum::<usize>()
    }

    fn approx_num_entries(&self) -> usize {
        self.bounded.approx_num_entries() + self.lock_pinned().len()
    }

    fn approx_size_bytes(&self) -> usize {
        self.bounded.approx_size_bytes()
            + self
                .lock_pinned()
                .values()
                .map(|entry| entry.size_bytes)
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use lance_core::cache::{CacheKey, LanceCache, MokaCacheBackend};

    use super::*;

    struct TestKey;

    impl CacheKey for TestKey {
        type ValueType = Vec<u8>;

        fn key(&self) -> Cow<'_, str> {
            Cow::Borrowed("metadata")
        }

        fn type_name() -> &'static str {
            "VanePinnedMetadataTest"
        }
    }

    #[tokio::test]
    async fn pin_survives_tiny_cache_and_releases_with_lease() {
        let bounded: Arc<dyn CacheBackend> = Arc::new(MokaCacheBackend::with_capacity(1));
        let backend = Arc::new(VaneIndexCacheBackend::new(bounded));
        let cache = LanceCache::with_backend_and_prefix(backend.clone(), "dataset/".to_string());
        let expected = Arc::new(vec![1_u8; 1024]);
        let lease = backend.pin_with_key("dataset/", &TestKey, expected.clone());

        assert_eq!(cache.get_with_key(&TestKey).await, Some(expected));
        cache
            .insert_with_key(&TestKey, Arc::new(vec![2_u8; 1024]))
            .await;
        assert_eq!(cache.get_with_key(&TestKey).await.unwrap()[0], 1);

        drop(lease);
        assert!(cache.get_with_key(&TestKey).await.is_none());
    }
}
