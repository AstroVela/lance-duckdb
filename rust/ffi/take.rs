use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::ptr;

use lance::dataset::ProjectionRequest;

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
    } else {
        let manifest = &handle.dataset.manifest;
        let uses_stable_row_ids = manifest.uses_stable_row_ids();
        let fragment_row_counts = (!uses_stable_row_ids).then(|| {
            manifest
                .fragments
                .iter()
                .map(|fragment| (fragment.id, fragment.physical_rows.unwrap_or_default()))
                .collect::<HashMap<_, _>>()
        });
        let row_id_is_in_range = |row_id: u64| {
            if uses_stable_row_ids {
                row_id < manifest.next_row_id
            } else {
                let fragment_id = row_id >> 32;
                let row_offset = row_id as u32 as usize;
                fragment_row_counts
                    .as_ref()
                    .and_then(|counts| counts.get(&fragment_id))
                    .is_some_and(|row_count| row_offset < *row_count)
            }
        };
        if row_ids.iter().all(|id| row_id_is_in_range(*id)) {
            row_ids
        } else {
            row_ids_filtered = row_ids
                .iter()
                .copied()
                .filter(|id| row_id_is_in_range(*id))
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
