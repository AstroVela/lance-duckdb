mod arrow_export;
mod dataset;
mod dir_namespace;
#[cfg(feature = "vane-distributed")]
mod distributed_mutation;
#[cfg(feature = "vane-distributed")]
mod distributed_write;
mod exec;
mod index;
mod knn;
mod merge;
mod namespace;
mod projection;
mod query_table;
mod scan;
mod schema_evolution;
mod search;
mod session;
mod stream;
mod take;
mod types;
mod update;
mod util;
mod write;

#[cfg(feature = "vane-distributed")]
mod vane_distributed_search;
#[cfg(feature = "vane-distributed")]
mod vane_index_cache;
#[cfg(feature = "vane-distributed")]
mod vane_rest_resolution;
#[cfg(feature = "vane-distributed")]
mod vane_search_plan;
