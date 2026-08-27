use std::collections::{HashMap, HashSet};
use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::Arc;

use lance::dataset::ProjectionRequest;
use lance_core::utils::deletion::DeletionVector;

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;

use super::types::StreamHandle;
use super::util::{optional_cstr_array, slice_from_ptr, FfiError, FfiResult};

#[no_mangle]
pub unsafe extern "C" fn lance_create_dataset_take_stream(
    dataset: *mut c_void,
    row_ids: *const u64,
    row_ids_len: usize,
    columns: *const *const c_char,
    columns_len: usize,
) -> *mut c_void {
    match create_dataset_take_stream_inner(
        dataset,
        row_ids,
        row_ids_len,
        columns,
        columns_len,
        true,
    ) {
        Ok(stream) => {
            clear_last_error();
            Box::into_raw(Box::new(stream)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_create_dataset_take_stream_unfiltered(
    dataset: *mut c_void,
    row_ids: *const u64,
    row_ids_len: usize,
    columns: *const *const c_char,
    columns_len: usize,
) -> *mut c_void {
    match create_dataset_take_stream_inner(
        dataset,
        row_ids,
        row_ids_len,
        columns,
        columns_len,
        false,
    ) {
        Ok(stream) => {
            clear_last_error();
            Box::into_raw(Box::new(stream)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

fn create_dataset_take_stream_inner(
    dataset: *mut c_void,
    row_ids: *const u64,
    row_ids_len: usize,
    columns: *const *const c_char,
    columns_len: usize,
    filter_out_of_range: bool,
) -> FfiResult<StreamHandle> {
    let handle = unsafe { super::util::dataset_handle(dataset)? };

    let row_ids = if row_ids_len == 0 {
        &[][..]
    } else {
        unsafe { slice_from_ptr(row_ids, row_ids_len, "row_ids")? }
    };
    let row_ids_filtered;
    let row_ids = if !filter_out_of_range || row_ids.is_empty() {
        row_ids
    } else if handle.dataset.manifest.uses_stable_row_ids() {
        // Stable row IDs are monotonically allocated. Lance's row ID index
        // filters IDs that have been deleted, while next_row_id is the exact
        // upper bound for IDs that could exist in this snapshot.
        let max_row_id = handle.dataset.manifest.next_row_id;
        if row_ids.iter().all(|id| *id < max_row_id) {
            row_ids
        } else {
            row_ids_filtered = row_ids
                .iter()
                .copied()
                .filter(|id| *id < max_row_id)
                .collect::<Vec<_>>();
            row_ids_filtered.as_slice()
        }
    } else {
        // Address-style row IDs encode (fragment_id, row_offset), so a total
        // dataset row count is not a valid upper bound. Resolve only the
        // referenced fragments and reject missing, out-of-range, and deleted
        // addresses before calling Lance's lower-level take primitive.
        let fragment_state = match runtime::block_on(async {
            let mut state = HashMap::new();
            let fragment_ids = row_ids.iter().map(|id| *id >> 32).collect::<HashSet<_>>();
            for fragment_id in fragment_ids {
                let Ok(fragment_idx) = usize::try_from(fragment_id) else {
                    continue;
                };
                if let Some(fragment) = handle.dataset.get_fragment(fragment_idx) {
                    let physical_rows = fragment.physical_rows().await? as u64;
                    let deletions = fragment.get_deletion_vector().await?;
                    state.insert(fragment_id, (physical_rows, deletions));
                }
            }
            Ok::<HashMap<u64, (u64, Option<Arc<DeletionVector>>)>, lance::Error>(state)
        }) {
            Ok(Ok(state)) => state,
            Ok(Err(err)) => {
                return Err(FfiError::new(
                    ErrorCode::DatasetTake,
                    format!("dataset row address validation: {err}"),
                ))
            }
            Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
        };
        let is_live_address = |id: &u64| {
            let fragment_id = *id >> 32;
            let row_offset = *id as u32;
            fragment_state
                .get(&fragment_id)
                .is_some_and(|(physical_rows, deletions)| {
                    u64::from(row_offset) < *physical_rows
                        && !deletions
                            .as_ref()
                            .is_some_and(|deletion_vector| deletion_vector.contains(row_offset))
                })
        };
        if row_ids.iter().all(is_live_address) {
            row_ids
        } else {
            row_ids_filtered = row_ids
                .iter()
                .copied()
                .filter(is_live_address)
                .collect::<Vec<_>>();
            row_ids_filtered.as_slice()
        }
    };

    let projection_cols = unsafe { optional_cstr_array(columns, columns_len, "columns")? };
    let projection = if projection_cols.is_empty() {
        ProjectionRequest::from_schema(handle.dataset.schema().clone())
    } else {
        ProjectionRequest::from_columns(
            projection_cols.iter().map(|s| s.as_str()),
            handle.dataset.schema(),
        )
    };

    let batch = match runtime::block_on(handle.dataset.take_rows(row_ids, projection)) {
        Ok(Ok(batch)) => batch,
        Ok(Err(err)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetTake,
                format!("dataset take_rows: {err}"),
            ))
        }
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };

    Ok(StreamHandle::Batches(vec![batch].into_iter()))
}
