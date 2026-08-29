use std::ffi::{c_char, c_void, CStr, CString};
use std::sync::Arc;

use anyhow::Context;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use datafusion_expr::Expr;
#[cfg(feature = "vane-distributed")]
use lance::dataset::builder::DatasetBuilder;
use lance::session::Session;
use lance_core::datatypes::Schema as LanceSchema;
#[cfg(feature = "vane-distributed")]
use lance_io::object_store::uri_to_url;
#[cfg(feature = "vane-distributed")]
use object_store::{aws::AwsCredential, StaticCredentialProvider};
#[cfg(feature = "vane-distributed")]
use url::Url;

use crate::error::ErrorCode;

use super::types::{DatasetHandle, SchemaHandle, SessionHandle, StreamHandle};

#[cfg(feature = "vane-distributed")]
const VANE_PATH_IS_URI: u8 = 1 << 0;
#[cfg(feature = "vane-distributed")]
const VANE_PATH_HAS_PRIVATE_COMPONENTS: u8 = 1 << 1;
#[cfg(feature = "vane-distributed")]
const VANE_PATH_IS_PROCESS_LOCAL: u8 = 1 << 2;
#[cfg(feature = "vane-distributed")]
const VANE_PATH_INVALID: u8 = 1 << 3;
#[cfg(feature = "vane-distributed")]
const VANE_PATH_IS_LANCE_DATASET: u8 = 1 << 4;
#[cfg(feature = "vane-distributed")]
const VANE_PATH_IS_REMOTE: u8 = 1 << 5;

#[cfg(feature = "vane-distributed")]
fn classify_vane_path(path: &str) -> u8 {
    // The remaining FFI opens are C-string based. An embedded NUL would make
    // them observe a different location from this length-aware classifier, so
    // fail closed before attempting URL normalization.
    if path.as_bytes().contains(&0) {
        return VANE_PATH_INVALID | VANE_PATH_HAS_PRIVATE_COMPONENTS;
    }

    // WHATWG URL parsing trims leading/trailing C0 controls and spaces and
    // ignores embedded tabs/newlines. Those rewrites must never turn an
    // apparently harmless coordinator string into a different worker URI.
    let has_unsafe_whitespace = path.as_bytes().iter().enumerate().any(|(index, byte)| {
        *byte < 0x20 || *byte == 0x7f || (*byte == b' ' && (index == 0 || index + 1 == path.len()))
    });

    // Preserve WHATWG's preprocessing even if Url::parse ultimately rejects a
    // malformed authority. Lance's generic URI helper can otherwise fall back
    // to a local path after the URI intent and its userinfo become invisible.
    let whatwg_probe: String = path
        .trim_matches(|character: char| character <= '\u{20}')
        .chars()
        .filter(|character| !matches!(character, '\t' | '\r' | '\n'))
        .collect();
    let direct_url = Url::parse(path).ok();
    let windows_drive = cfg!(windows)
        && direct_url
            .as_ref()
            .is_some_and(|url| url.scheme().len() == 1);
    let is_uri = direct_url.is_some() && !windows_drive;
    let mut flags = if is_uri { VANE_PATH_IS_URI } else { 0 };
    let malformed_authority_uri = direct_url.is_none()
        && whatwg_probe.find(':').is_some_and(|colon| {
            let scheme = &whatwg_probe[..colon];
            let valid_scheme = scheme.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_alphabetic()
                    || (index > 0
                        && (byte.is_ascii_digit() || byte == b'+' || byte == b'-' || byte == b'.'))
            }) && !scheme.is_empty();
            valid_scheme
                && !(cfg!(windows) && scheme.len() == 1)
                && whatwg_probe[colon + 1..].starts_with("//")
        });
    if malformed_authority_uri {
        // A malformed authority (for example an invalid port after userinfo)
        // makes url::Url reject the URI, while Lance's generic helper can
        // still fall back to a percent-encoded local path. Preserve the
        // caller's URI intent and fail closed instead of losing credentials.
        flags |= VANE_PATH_INVALID | VANE_PATH_HAS_PRIVATE_COMPONENTS;
    }
    let raw_private_offset = path.find(['?', '#']);
    if raw_private_offset.is_some() {
        // Lance's local-path fallback percent-encodes these delimiters instead
        // of exposing them as URL query/fragment components. DuckDB can also
        // preserve them while normalizing a file: URI to a literal path. Treat
        // the raw delimiters as private before either representation is lost.
        flags |= VANE_PATH_HAS_PRIVATE_COMPONENTS;
    }

    let Ok(url) = uri_to_url(path) else {
        return flags | VANE_PATH_INVALID | VANE_PATH_HAS_PRIVATE_COMPONENTS;
    };
    if (is_uri && has_unsafe_whitespace)
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        flags |= VANE_PATH_HAS_PRIVATE_COMPONENTS;
    }
    if url.scheme().eq_ignore_ascii_case("memory")
        || url.scheme().eq_ignore_ascii_case("shared-memory")
    {
        flags |= VANE_PATH_IS_PROCESS_LOCAL;
    }
    if is_uri
        && !url.scheme().eq_ignore_ascii_case("file")
        && !url.scheme().eq_ignore_ascii_case("memory")
        && !url.scheme().eq_ignore_ascii_case("shared-memory")
    {
        flags |= VANE_PATH_IS_REMOTE;
    }
    let dataset_probe = if malformed_authority_uri {
        whatwg_probe.as_str()
    } else {
        path
    };
    let dataset_private_offset = dataset_probe.find(['?', '#']);
    let raw_dataset_path = &dataset_probe[..dataset_private_offset.unwrap_or(dataset_probe.len())];
    if url.path().trim_end_matches(['/', '\\']).ends_with(".lance")
        || raw_dataset_path
            .trim_end_matches(['/', '\\'])
            .ends_with(".lance")
    {
        flags |= VANE_PATH_IS_LANCE_DATASET;
    }
    flags
}

#[cfg(feature = "vane-distributed")]
#[no_mangle]
pub unsafe extern "C" fn lance_vane_classify_path(path: *const u8, path_len: usize) -> u8 {
    if path.is_null() {
        return VANE_PATH_INVALID | VANE_PATH_HAS_PRIVATE_COMPONENTS;
    }
    let bytes = unsafe { std::slice::from_raw_parts(path, path_len) };
    let Ok(path) = std::str::from_utf8(bytes) else {
        return VANE_PATH_INVALID | VANE_PATH_HAS_PRIVATE_COMPONENTS;
    };
    classify_vane_path(path)
}

#[derive(Debug)]
pub(crate) struct FfiError {
    pub(crate) code: ErrorCode,
    pub(crate) message: String,
}

impl FfiError {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub(crate) type FfiResult<T> = Result<T, FfiError>;

pub(crate) fn to_c_string(s: impl AsRef<str>) -> CString {
    match CString::new(s.as_ref()) {
        Ok(v) => v,
        Err(_) => CString::new(s.as_ref().replace('\0', "\\0"))
            .unwrap_or_else(|_| CString::new("invalid string").unwrap()),
    }
}

#[cfg(feature = "vane-distributed")]
pub(crate) fn explicit_aws_credentials(
    storage_options: &std::collections::HashMap<String, String>,
) -> Option<AwsCredential> {
    let access_key_id = storage_options
        .get("aws_access_key_id")
        .or_else(|| storage_options.get("access_key_id"));
    let secret_access_key = storage_options
        .get("aws_secret_access_key")
        .or_else(|| storage_options.get("secret_access_key"));
    let (Some(access_key_id), Some(secret_access_key)) = (access_key_id, secret_access_key) else {
        return None;
    };
    if access_key_id.is_empty() || secret_access_key.is_empty() {
        return None;
    }

    let token = storage_options
        .get("aws_session_token")
        .or_else(|| storage_options.get("session_token"))
        .filter(|value| !value.is_empty())
        .cloned();
    Some(AwsCredential {
        key_id: access_key_id.clone(),
        secret_key: secret_access_key.clone(),
        token,
    })
}

#[cfg(feature = "vane-distributed")]
pub(crate) fn with_explicit_aws_credentials(
    builder: DatasetBuilder,
    storage_options: &std::collections::HashMap<String, String>,
) -> DatasetBuilder {
    let Some(credentials) = explicit_aws_credentials(storage_options) else {
        return builder;
    };
    builder.with_aws_credentials_provider(Arc::new(StaticCredentialProvider::new(credentials)))
}

#[cfg(feature = "vane-distributed")]
pub(crate) fn vane_external_location_error(
    code: ErrorCode,
    operation: &'static str,
    location: &str,
    error: impl std::fmt::Display,
) -> FfiError {
    // DuckDB may normalize file: URIs into literal local paths while retaining
    // their query/fragment text. Treat those delimiters as private even when
    // the normalized value no longer has a URI scheme.
    let classification = classify_vane_path(location);
    let has_private_components = location.contains('?')
        || location.contains('#')
        || classification
            & (VANE_PATH_HAS_PRIVATE_COMPONENTS | VANE_PATH_INVALID | VANE_PATH_IS_REMOTE)
            != 0;
    if has_private_components {
        return FfiError::new(code, format!("{operation} '<redacted-private-uri>' failed"));
    }
    FfiError::new(code, format!("{operation} '{location}': {error}"))
}

#[cfg(feature = "vane-distributed")]
pub(crate) fn vane_rest_namespace_error(
    code: ErrorCode,
    operation: &'static str,
    _error: impl std::fmt::Display,
) -> FfiError {
    // REST namespace implementations may relay opaque storage-service errors
    // containing vended credentials or presigned URLs. This remains possible
    // for a public endpoint without client-side authentication, so no remote
    // error detail is safe to expose in Vane diagnostics.
    FfiError::new(code, format!("{operation} failed: details redacted"))
}

pub(crate) fn canonicalize_lance_field_path(
    schema: &LanceSchema,
    column: &str,
    what: &'static str,
) -> FfiResult<String> {
    let column = column.trim();
    if column.is_empty() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} cannot be empty"),
        ));
    }

    let field = schema.field_case_insensitive(column).ok_or_else(|| {
        FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} not found: '{column}'"),
        )
    })?;

    schema.field_path(field.id).map_err(|err| {
        FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} path normalize: {err}"),
        )
    })
}

pub(crate) unsafe fn cstr_to_str<'a>(ptr: *const c_char, what: &'static str) -> FfiResult<&'a str> {
    if ptr.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} is null"),
        ));
    }
    let s = unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .context("utf8 decode")
        .map_err(|err| FfiError::new(ErrorCode::Utf8, format!("{what} utf8: {err}")))?;
    Ok(s)
}

pub(crate) unsafe fn slice_from_ptr<'a, T>(
    ptr: *const T,
    len: usize,
    what: &'static str,
) -> FfiResult<&'a [T]> {
    if ptr.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} is null"),
        ));
    }
    // SAFETY: Caller guarantees ptr points to at least len elements.
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

pub(crate) unsafe fn optional_cstr_array(
    ptr: *const *const c_char,
    len: usize,
    what: &'static str,
) -> FfiResult<Vec<String>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} is null with non-zero length"),
        ));
    }

    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    let mut out = Vec::with_capacity(len);
    for (idx, &item) in slice.iter().enumerate() {
        if item.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                format!("{what}[{idx}] is null"),
            ));
        }
        let s = unsafe { CStr::from_ptr(item) }
            .to_str()
            .context("utf8 decode")
            .map_err(|err| FfiError::new(ErrorCode::Utf8, format!("{what}[{idx}] utf8: {err}")))?;
        out.push(s.to_string());
    }
    Ok(out)
}

pub(crate) unsafe fn parse_optional_filter_ir(
    filter_ir: *const u8,
    filter_ir_len: usize,
    code: ErrorCode,
    what: &'static str,
) -> FfiResult<Option<Expr>> {
    if filter_ir_len == 0 {
        return Ok(None);
    }
    if filter_ir.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} is null with non-zero length"),
        ));
    }

    let bytes = unsafe { std::slice::from_raw_parts(filter_ir, filter_ir_len) };
    crate::filter_ir::parse_filter_ir(bytes)
        .map(Some)
        .map_err(|err| FfiError::new(code, format!("{what} parse: {err}")))
}

pub(crate) fn u64_to_usize(v: u64, what: &'static str) -> FfiResult<usize> {
    usize::try_from(v)
        .map_err(|err| FfiError::new(ErrorCode::InvalidArgument, format!("invalid {what}: {err}")))
}

pub(crate) fn nonzero_u64_to_usize(v: u64, what: &'static str) -> FfiResult<usize> {
    let v = u64_to_usize(v, what)?;
    if v == 0 {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} must be > 0"),
        ));
    }
    Ok(v)
}

pub(crate) fn nonzero_u64_to_i64(v: u64, what: &'static str) -> FfiResult<i64> {
    let v = i64::try_from(v).map_err(|err| {
        FfiError::new(ErrorCode::InvalidArgument, format!("invalid {what}: {err}"))
    })?;
    if v <= 0 {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} must be > 0"),
        ));
    }
    Ok(v)
}

pub(crate) unsafe fn dataset_handle<'a>(dataset: *mut c_void) -> FfiResult<&'a DatasetHandle> {
    if dataset.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "dataset is null"));
    }
    // SAFETY: Caller guarantees dataset points to a valid DatasetHandle.
    Ok(unsafe { &*(dataset as *const DatasetHandle) })
}

pub(crate) unsafe fn optional_session_handle(
    session: *mut c_void,
) -> FfiResult<Option<Arc<Session>>> {
    if session.is_null() {
        return Ok(None);
    }
    let handle = unsafe { &*(session as *const SessionHandle) };
    Ok(Some(handle.session.clone()))
}

pub(crate) unsafe fn stream_handle_mut<'a>(stream: *mut c_void) -> FfiResult<&'a mut StreamHandle> {
    if stream.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "stream is null"));
    }
    // SAFETY: Caller guarantees stream points to a valid StreamHandle.
    Ok(unsafe { &mut *(stream as *mut StreamHandle) })
}

pub(crate) unsafe fn schema_handle<'a>(schema: *mut c_void) -> FfiResult<&'a SchemaHandle> {
    if schema.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "schema is null"));
    }
    // SAFETY: Caller guarantees schema points to a valid SchemaHandle.
    Ok(unsafe { &*(schema as *const SchemaHandle) })
}

pub(crate) unsafe fn batch_handle<'a>(batch: *mut c_void) -> FfiResult<&'a RecordBatch> {
    if batch.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "batch is null"));
    }
    // SAFETY: Caller guarantees batch points to a valid RecordBatch.
    Ok(unsafe { &*(batch as *const RecordBatch) })
}

pub(crate) fn schema_to_ffi_arrow_schema(
    schema: &Schema,
) -> FfiResult<arrow::ffi::FFI_ArrowSchema> {
    let data_type = arrow::datatypes::DataType::Struct(schema.fields().clone());
    arrow::ffi::FFI_ArrowSchema::try_from(&data_type)
        .map_err(|err| FfiError::new(ErrorCode::SchemaExport, format!("schema export: {err}")))
}

#[cfg(all(test, feature = "vane-distributed"))]
mod tests {
    use std::collections::HashMap;

    use crate::error::ErrorCode;

    use super::{
        classify_vane_path, explicit_aws_credentials, lance_vane_classify_path,
        vane_external_location_error, vane_rest_namespace_error, VANE_PATH_HAS_PRIVATE_COMPONENTS,
        VANE_PATH_INVALID, VANE_PATH_IS_LANCE_DATASET, VANE_PATH_IS_PROCESS_LOCAL,
        VANE_PATH_IS_REMOTE, VANE_PATH_IS_URI,
    };

    #[test]
    fn vane_path_classification_matches_lance_url_semantics() {
        for path in [
            " file:/tmp/data.lance?leading-secret",
            "s3:\t//user:password@bucket/data.lance",
            "file://user:password@localhost/tmp/data.lance",
            "s3://user:password@bucket/data.lance ",
            "s3://user:password@host:notaport/data.lance",
            " s3://user:password@host:notaport/data.lance ",
            "s3:\t//user:password@host:notaport/data.lance",
        ] {
            let flags = classify_vane_path(path);
            assert_ne!(flags & VANE_PATH_HAS_PRIVATE_COMPONENTS, 0, "{path}");
            assert_ne!(flags & VANE_PATH_IS_LANCE_DATASET, 0, "{path}");
        }

        let malformed_authority = classify_vane_path("s3://user:password@host:notaport/data.lance");
        assert_ne!(malformed_authority & VANE_PATH_INVALID, 0);
        for path in [
            " s3://user:password@host:notaport/data.lance ",
            "s3:\t//user:password@host:notaport/data.lance",
        ] {
            let flags = classify_vane_path(path);
            assert_ne!(flags & VANE_PATH_INVALID, 0, "{path:?}");
            assert_ne!(flags & VANE_PATH_HAS_PRIVATE_COMPONENTS, 0, "{path:?}");
            assert_ne!(flags & VANE_PATH_IS_LANCE_DATASET, 0, "{path:?}");
        }

        for path in [
            "s3://user:password@bucket/data.lance/.",
            "file://user:password@localhost/tmp/data.lance/",
        ] {
            assert_ne!(
                classify_vane_path(path) & VANE_PATH_IS_LANCE_DATASET,
                0,
                "{path}"
            );
        }

        let safe_uri = classify_vane_path("s3://bucket/data.lance");
        assert_ne!(safe_uri & VANE_PATH_IS_URI, 0);
        assert_ne!(safe_uri & VANE_PATH_IS_REMOTE, 0);
        assert_eq!(safe_uri & VANE_PATH_HAS_PRIVATE_COMPONENTS, 0);
        assert_eq!(safe_uri & VANE_PATH_INVALID, 0);

        let local_path = classify_vane_path("relative/path/data.lance");
        assert_eq!(local_path & VANE_PATH_IS_URI, 0);
        assert_eq!(local_path & VANE_PATH_INVALID, 0);
        assert_ne!(local_path & VANE_PATH_IS_LANCE_DATASET, 0);

        for local_path in [
            "/tmp/data.lance?sig=literal-query-secret",
            "/tmp/data.lance#literal-fragment-secret",
        ] {
            let flags = classify_vane_path(local_path);
            assert_eq!(flags & VANE_PATH_IS_URI, 0, "{local_path}");
            assert_ne!(flags & VANE_PATH_HAS_PRIVATE_COMPONENTS, 0, "{local_path}");
            assert_ne!(flags & VANE_PATH_IS_LANCE_DATASET, 0, "{local_path}");
        }

        let file_uri = classify_vane_path("file:/tmp/data.lance");
        assert_eq!(file_uri & VANE_PATH_IS_REMOTE, 0);

        for local_path in [
            " relative/path/data.lance",
            "relative/path/data set.lance ",
            "relative/path/data\tset.lance",
            "relative/path/data\nset.lance",
        ] {
            let flags = classify_vane_path(local_path);
            assert_eq!(flags & VANE_PATH_IS_URI, 0, "{local_path:?}");
            assert_eq!(
                flags & VANE_PATH_HAS_PRIVATE_COMPONENTS,
                0,
                "{local_path:?}"
            );
            assert_eq!(flags & VANE_PATH_INVALID, 0, "{local_path:?}");
        }

        for path in ["memory:/dataset", "shared-memory://bucket/dataset"] {
            assert_ne!(
                classify_vane_path(path) & VANE_PATH_IS_PROCESS_LOCAL,
                0,
                "{path}"
            );
        }

        #[cfg(not(windows))]
        {
            let one_letter = classify_vane_path("x:/missing.lance?one-letter-secret");
            assert_ne!(one_letter & VANE_PATH_IS_URI, 0);
            assert_ne!(one_letter & VANE_PATH_HAS_PRIVATE_COMPONENTS, 0);
        }
    }

    #[test]
    fn vane_path_classifier_rejects_embedded_nul_without_c_string_truncation() {
        let path = b"/tmp/data.lance\0?token=embedded-nul-secret";
        // SAFETY: `path` is a valid byte slice for its complete length. The
        // classifier must inspect the bytes after NUL rather than treating the
        // input as a C string.
        let flags = unsafe { lance_vane_classify_path(path.as_ptr(), path.len()) };
        assert_ne!(flags & VANE_PATH_INVALID, 0);
        assert_ne!(flags & VANE_PATH_HAS_PRIVATE_COMPONENTS, 0);
    }

    #[test]
    fn external_location_errors_redact_private_uri_components() {
        for location in [
            "file:/tmp/data.lance?token=secret",
            "file://user:password@localhost/tmp/data.lance",
            "/tmp/directory#private-fragment",
        ] {
            let error = vane_external_location_error(
                ErrorCode::DatasetOpen,
                "dataset open",
                location,
                format!("backend repeated {location}"),
            );
            assert_eq!(
                error.message,
                "dataset open '<redacted-private-uri>' failed"
            );
            assert!(!error.message.contains("secret"));
            assert!(!error.message.contains("password"));
            assert!(!error.message.contains("private-fragment"));
        }
    }

    #[test]
    fn external_location_errors_keep_safe_local_diagnostics() {
        let error = vane_external_location_error(
            ErrorCode::DatasetOpen,
            "dataset open",
            "/srv/lance/data.lance",
            "not found",
        );
        assert_eq!(
            error.message,
            "dataset open '/srv/lance/data.lance': not found"
        );
    }

    #[test]
    fn external_location_errors_redact_remote_backend_details() {
        let error = vane_external_location_error(
            ErrorCode::DatasetOpen,
            "dataset open",
            "s3://bucket/data.lance",
            "backend echoed x-amz-security-token=remote-secret",
        );
        assert_eq!(
            error.message,
            "dataset open '<redacted-private-uri>' failed"
        );
        assert!(!error.message.contains("remote-secret"));
    }

    #[test]
    fn rest_namespace_errors_always_drop_remote_details() {
        let error = vane_rest_namespace_error(
            ErrorCode::NamespaceListTables,
            "namespace list_tables",
            "backend repeated https://storage/?token=remote-secret",
        );
        assert_eq!(
            error.message,
            "namespace list_tables failed: details redacted"
        );
        assert!(!error.message.contains("remote-secret"));
    }

    #[test]
    fn explicit_aws_credentials_treats_an_empty_token_as_absent() {
        let options = HashMap::from([
            ("aws_access_key_id".to_string(), "access".to_string()),
            ("aws_secret_access_key".to_string(), "secret".to_string()),
            ("aws_session_token".to_string(), String::new()),
        ]);

        let credentials = explicit_aws_credentials(&options).unwrap();
        assert_eq!(credentials.key_id, "access");
        assert_eq!(credentials.secret_key, "secret");
        assert_eq!(credentials.token, None);
    }

    #[test]
    fn explicit_aws_credentials_preserves_nonempty_tokens_and_aliases() {
        let options = HashMap::from([
            ("access_key_id".to_string(), "access".to_string()),
            ("secret_access_key".to_string(), "secret".to_string()),
            ("session_token".to_string(), "token".to_string()),
        ]);

        let credentials = explicit_aws_credentials(&options).unwrap();
        assert_eq!(credentials.token.as_deref(), Some("token"));
    }

    #[test]
    fn explicit_aws_credentials_requires_a_complete_nonempty_pair() {
        for options in [
            HashMap::new(),
            HashMap::from([("aws_access_key_id".to_string(), "access".to_string())]),
            HashMap::from([
                ("aws_access_key_id".to_string(), String::new()),
                ("aws_secret_access_key".to_string(), "secret".to_string()),
            ]),
            HashMap::from([("aws_profile".to_string(), "profile".to_string())]),
        ] {
            assert!(explicit_aws_credentials(&options).is_none());
        }
    }
}
