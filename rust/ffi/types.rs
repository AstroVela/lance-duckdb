use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use lance::session::Session;
use lance::Dataset;
use lance_core::cache::CacheBackend;

#[cfg(feature = "vane-distributed")]
use super::vane_distributed_search::VectorCandidateStream;
use crate::datafusion_stream::DataFusionStream;
use crate::scanner::{LanceStream, LanceTakeStream};

use super::projection;
#[cfg(feature = "vane-distributed")]
use super::vane_index_cache::{VaneIndexCacheBackend, VaneIndexCacheLease};

pub(crate) type SchemaHandle = Arc<Schema>;

pub(crate) struct SessionHandle {
    pub(crate) session: Arc<Session>,
    // Lance does not expose clearing its index cache through Session.
    pub(crate) index_cache: Arc<dyn CacheBackend>,
    #[cfg(feature = "vane-distributed")]
    pub(crate) index_metadata_seed_lock: Arc<tokio::sync::Mutex<()>>,
    #[cfg(feature = "vane-distributed")]
    pub(crate) vane_index_cache: Arc<VaneIndexCacheBackend>,
}

pub(crate) struct DatasetHandle {
    pub(crate) dataset: Arc<Dataset>,
    pub(crate) arrow_schema: SchemaHandle,
    pub(crate) base_projection: Arc<[String]>,
    pub(crate) fts_projection: Arc<[String]>,
    #[cfg(feature = "vane-distributed")]
    frozen_index_metadata_lease: std::sync::Mutex<Option<VaneIndexCacheLease>>,
}

impl DatasetHandle {
    pub(crate) fn new(dataset: Arc<Dataset>) -> Self {
        let arrow_schema: Schema = dataset.schema().into();
        let arrow_schema = Arc::new(arrow_schema);
        let base_projection = projection::build_base_projection(&arrow_schema);
        let fts_projection = projection::build_fts_projection(&base_projection);
        Self {
            dataset,
            arrow_schema,
            base_projection,
            fts_projection,
            #[cfg(feature = "vane-distributed")]
            frozen_index_metadata_lease: std::sync::Mutex::new(None),
        }
    }

    #[cfg(feature = "vane-distributed")]
    pub(crate) fn retain_frozen_index_metadata(&self, lease: VaneIndexCacheLease) {
        *self
            .frozen_index_metadata_lease
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(lease);
    }
}

pub(crate) enum StreamHandle {
    Lance(LanceStream),
    #[cfg(feature = "vane-distributed")]
    VectorCandidates(VectorCandidateStream),
    Take(LanceTakeStream),
    DataFusion(DataFusionStream),
    Batches(std::vec::IntoIter<RecordBatch>),
}

impl StreamHandle {
    pub(crate) fn next_batch(&mut self) -> Result<Option<RecordBatch>, anyhow::Error> {
        match self {
            StreamHandle::Lance(stream) => stream.next().map_err(anyhow::Error::new),
            #[cfg(feature = "vane-distributed")]
            StreamHandle::VectorCandidates(stream) => stream.next(),
            StreamHandle::Take(stream) => stream.next().map_err(anyhow::Error::new),
            StreamHandle::DataFusion(stream) => stream.next().map_err(anyhow::Error::new),
            StreamHandle::Batches(iter) => Ok(iter.next()),
        }
    }
}
