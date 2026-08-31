use std::ffi::{c_char, CString};
use std::ptr;

use lance_namespace::models::{DescribeTableRequest, DescribeTableResponse};
use lance_namespace::LanceNamespace;
use lance_namespace_impls::RestNamespaceBuilder;

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;

use super::session::record_namespace_describe;
use super::util::{cstr_to_str, to_c_string, vane_rest_namespace_error, FfiError, FfiResult};

unsafe fn optional_string(value: *const c_char, what: &'static str) -> FfiResult<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    let value = unsafe { cstr_to_str(value, what)? };
    if value.is_empty() {
        Ok(None)
    } else {
        Ok(Some(value.to_string()))
    }
}

fn headers(value: Option<&str>) -> Vec<(String, String)> {
    value
        .into_iter()
        .flat_map(str::lines)
        .filter_map(|line| {
            let (name, value) = line.split_once('\t')?;
            (!name.is_empty()).then(|| (name.to_string(), value.to_string()))
        })
        .collect()
}

#[derive(Debug)]
struct RestPhysicalDescription {
    table_uri: String,
    schema_json: String,
    version: u64,
}

fn distributed_describe_request(id: Vec<String>, version: i64) -> DescribeTableRequest {
    let mut request = DescribeTableRequest::new();
    request.id = Some(id);
    request.with_table_uri = Some(true);
    request.load_detailed_metadata = Some(true);
    request.vend_credentials = Some(false);
    request.version = Some(version);
    // Deliberately leave identity, context, branch, and check_declared unset.
    // The version is the snapshot already bound by the coordinator.
    request
}

fn physical_description_from_response(
    response: DescribeTableResponse,
    expected_version: u64,
) -> FfiResult<RestPhysicalDescription> {
    if response.managed_versioning.unwrap_or(false) {
        return Err(FfiError::new(
            ErrorCode::NamespaceDescribeTable,
            "distributed REST reads do not support managed versioning",
        ));
    }
    if response.is_only_declared.unwrap_or(false) {
        return Err(FfiError::new(
            ErrorCode::NamespaceDescribeTable,
            "distributed REST reads require a materialized table",
        ));
    }
    let table_uri = response.table_uri.ok_or_else(|| {
        FfiError::new(
            ErrorCode::NamespaceDescribeTable,
            "distributed REST reads require table_uri",
        )
    })?;
    if table_uri.is_empty() || table_uri.contains('\0') {
        return Err(FfiError::new(
            ErrorCode::NamespaceDescribeTable,
            "distributed REST reads received an invalid table_uri",
        ));
    }
    let version = response.version.ok_or_else(|| {
        FfiError::new(
            ErrorCode::NamespaceDescribeTable,
            "distributed REST reads require detailed table version metadata",
        )
    })?;
    let version = u64::try_from(version)
        .ok()
        .filter(|version| *version != 0)
        .ok_or_else(|| {
            FfiError::new(
                ErrorCode::NamespaceDescribeTable,
                "distributed REST reads received an invalid table version",
            )
        })?;
    if version != expected_version {
        return Err(FfiError::new(
            ErrorCode::NamespaceDescribeTable,
            "distributed REST reads received a different table version than the bound snapshot",
        ));
    }
    let schema = response.schema.ok_or_else(|| {
        FfiError::new(
            ErrorCode::NamespaceDescribeTable,
            "distributed REST reads require detailed table schema metadata",
        )
    })?;
    let schema_json = serde_json::to_string(&schema).map_err(|_| {
        FfiError::new(
            ErrorCode::SchemaExport,
            "failed to encode the distributed REST table schema",
        )
    })?;

    // response.storage_options is intentionally ignored even if a server
    // returns it despite vend_credentials=false.
    Ok(RestPhysicalDescription {
        table_uri,
        schema_json,
        version,
    })
}

#[allow(clippy::too_many_arguments)]
unsafe fn resolve_inner(
    endpoint: *const c_char,
    table_id: *const c_char,
    bearer_token: *const c_char,
    api_key: *const c_char,
    delimiter: *const c_char,
    headers_tsv: *const c_char,
    expected_version: u64,
) -> FfiResult<RestPhysicalDescription> {
    let endpoint = unsafe { cstr_to_str(endpoint, "endpoint")? };
    let table_id = unsafe { cstr_to_str(table_id, "table_id")? };
    let bearer_token = unsafe { optional_string(bearer_token, "bearer_token")? };
    let api_key = unsafe { optional_string(api_key, "api_key")? };
    let delimiter =
        unsafe { optional_string(delimiter, "delimiter")? }.unwrap_or_else(|| "$".to_string());
    let headers_tsv = unsafe { optional_string(headers_tsv, "headers_tsv")? };
    let expected_version = i64::try_from(expected_version)
        .ok()
        .filter(|version| *version > 0)
        .ok_or_else(|| {
            FfiError::new(
                ErrorCode::InvalidArgument,
                "distributed REST resolution requires a valid bound table version",
            )
        })?;

    let mut builder = RestNamespaceBuilder::new(endpoint);
    if let Some(token) = bearer_token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    if let Some(key) = api_key {
        builder = builder.header("x-api-key", key);
    }
    for (name, value) in headers(headers_tsv.as_deref()) {
        builder = builder.header(name, value);
    }
    let namespace = builder.delimiter(delimiter.clone()).build();
    let id = table_id
        .split(delimiter.as_str())
        .map(str::to_string)
        .collect::<Vec<_>>();

    runtime::block_on(async move {
        record_namespace_describe();
        let request = distributed_describe_request(id, expected_version);
        let response = namespace.describe_table(request).await.map_err(|err| {
            vane_rest_namespace_error(
                ErrorCode::NamespaceDescribeTable,
                "distributed namespace describe_table",
                err,
            )
        })?;
        physical_description_from_response(response, expected_version as u64)
    })
    .map_err(|err| {
        FfiError::new(
            ErrorCode::Runtime,
            format!("REST resolution runtime: {err}"),
        )
    })?
}

#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub unsafe extern "C" fn lance_vane_resolve_rest_table(
    endpoint: *const c_char,
    table_id: *const c_char,
    bearer_token: *const c_char,
    api_key: *const c_char,
    delimiter: *const c_char,
    headers_tsv: *const c_char,
    expected_version: u64,
    out_table_uri: *mut *const c_char,
    out_schema_json: *mut *const c_char,
    out_version: *mut u64,
) -> i32 {
    if !out_table_uri.is_null() {
        unsafe { ptr::write_unaligned(out_table_uri, ptr::null()) };
    }
    if !out_schema_json.is_null() {
        unsafe { ptr::write_unaligned(out_schema_json, ptr::null()) };
    }
    if !out_version.is_null() {
        unsafe { ptr::write_unaligned(out_version, 0) };
    }
    if out_table_uri.is_null() || out_schema_json.is_null() || out_version.is_null() {
        set_last_error(
            ErrorCode::InvalidArgument,
            "distributed REST resolution output is null",
        );
        return -1;
    }

    match unsafe {
        resolve_inner(
            endpoint,
            table_id,
            bearer_token,
            api_key,
            delimiter,
            headers_tsv,
            expected_version,
        )
    } {
        Ok(result) => {
            let table_uri =
                CString::new(result.table_uri).unwrap_or_else(|_| to_c_string("invalid table uri"));
            let schema_json =
                CString::new(result.schema_json).unwrap_or_else(|_| to_c_string("invalid schema"));
            unsafe {
                ptr::write_unaligned(out_table_uri, table_uri.into_raw());
                ptr::write_unaligned(out_schema_json, schema_json.into_raw());
                ptr::write_unaligned(out_version, result.version);
            }
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use lance_namespace::models::JsonArrowSchema;

    use super::*;

    fn valid_response() -> DescribeTableResponse {
        let mut response = DescribeTableResponse::new();
        response.table_uri = Some("s3://bucket/table.lance".to_string());
        response.version = Some(7);
        response.schema = Some(Box::new(JsonArrowSchema::new(Vec::new())));
        response
    }

    #[test]
    fn header_parser_drops_malformed_lines() {
        assert_eq!(
            headers(Some("x-a\t1\nmalformed\nx-b\t2")),
            vec![
                ("x-a".to_string(), "1".to_string()),
                ("x-b".to_string(), "2".to_string())
            ]
        );
    }

    #[test]
    fn describe_request_uses_only_the_standard_resolution_fields() {
        let request = distributed_describe_request(vec!["db".into(), "table".into()], 7);
        assert_eq!(request.id, Some(vec!["db".into(), "table".into()]));
        assert_eq!(request.with_table_uri, Some(true));
        assert_eq!(request.load_detailed_metadata, Some(true));
        assert_eq!(request.vend_credentials, Some(false));
        assert!(request.identity.is_none());
        assert!(request.context.is_none());
        assert_eq!(request.version, Some(7));
        assert!(request.branch.is_none());
        assert!(request.check_declared.is_none());

        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["vend_credentials"], false);
        assert_eq!(json["with_table_uri"], true);
        assert_eq!(json["load_detailed_metadata"], true);
        assert!(json.get("context").is_none());
    }

    #[test]
    fn response_requires_standard_physical_identity_and_ignores_vended_options() {
        let mut response = valid_response();
        response.storage_options = Some(HashMap::from([(
            "secret_access_key".to_string(),
            "must-not-be-consumed".to_string(),
        )]));
        let description = physical_description_from_response(response, 7).unwrap();
        assert_eq!(description.table_uri, "s3://bucket/table.lance");
        assert_eq!(description.version, 7);
        assert!(!description.schema_json.contains("must-not-be-consumed"));

        let mut missing_uri = valid_response();
        missing_uri.location = Some("s3://bucket/location-only.lance".to_string());
        missing_uri.table_uri = None;
        assert!(physical_description_from_response(missing_uri, 7).is_err());

        let mut managed = valid_response();
        managed.managed_versioning = Some(true);
        assert!(physical_description_from_response(managed, 7).is_err());

        let mut declared = valid_response();
        declared.is_only_declared = Some(true);
        assert!(physical_description_from_response(declared, 7).is_err());

        let mut wrong_version = valid_response();
        wrong_version.version = Some(8);
        assert!(physical_description_from_response(wrong_version, 7).is_err());
    }
}
