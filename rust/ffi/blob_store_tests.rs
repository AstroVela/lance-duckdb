use std::sync::atomic::{AtomicUsize, Ordering};

use object_store::{local::LocalFileSystem, memory::InMemory, ObjectStoreExt};

use super::*;
use crate::ffi::blob_test_support::{assert_released, Access, TempDir};

#[derive(Debug)]
struct RecordingProvider {
    inner: Arc<dyn Store>,
    created: Arc<AtomicUsize>,
    options: Arc<Mutex<Vec<HashMap<String, String>>>>,
}

#[async_trait]
impl ObjectStoreProvider for RecordingProvider {
    async fn new_store(&self, url: Url, params: &ObjectStoreParams) -> lance::Result<ObjectStore> {
        self.created.fetch_add(1, Ordering::SeqCst);
        self.options
            .lock()
            .unwrap()
            .push(params.storage_options().cloned().unwrap_or_default());
        Ok(ObjectStore::new(
            self.inner.clone(),
            url,
            Some(4096),
            None,
            false,
            true,
            2,
            0,
            params.storage_options(),
        ))
    }
}

#[derive(Debug)]
struct RecordingStore {
    inner: Arc<dyn Store>,
    reads: Arc<AtomicUsize>,
    short: bool,
    omit_etag: bool,
}

impl fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecordingStore")
    }
}

#[async_trait]
impl Store for RecordingStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let head = options.head;
        let mut result = self.inner.get_opts(location, options).await?;
        if self.omit_etag {
            result.meta.e_tag = None;
        }
        if self.short && !head {
            result.payload = object_store::GetResultPayload::Stream(
                stream::once(async { Ok(vec![1u8].into()) }).boxed(),
            );
        }
        Ok(result)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.list_with_delimiter(prefix).await
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

fn provider(access: &Access, inner: Arc<dyn ObjectStoreProvider>) -> Arc<BlobStoreProvider> {
    Arc::new(BlobStoreProvider {
        inner,
        access: access.context(),
        resolutions: Default::default(),
    })
}

#[test]
fn local_authorities_are_rejected_before_callbacks_and_provider_io() {
    crate::runtime::block_on(async {
        for scheme in ["file", "file-object-store", "file+uring"] {
            let access = Access::new();
            let created = Arc::new(AtomicUsize::new(0));
            let registry = ObjectStoreRegistry::empty();
            registry.insert(
                scheme,
                provider(
                    &access,
                    Arc::new(RecordingProvider {
                        inner: Arc::new(LocalFileSystem::new()),
                        created: created.clone(),
                        options: Default::default(),
                    }),
                ),
            );
            for host in ["host", "127.0.0.1", "[::1]"] {
                let url = Url::parse(&format!("{scheme}://{host}/tmp/private.bin")).unwrap();
                let error = registry
                    .get_store(url, &ObjectStoreParams::default())
                    .await
                    .unwrap_err();
                assert_eq!(
                    io_error(ErrorCode::BlobOpen, &error, "open").code as i32,
                    ErrorCode::BlobAccess as i32
                );
            }
            assert_eq!(created.load(Ordering::SeqCst), 0);
            assert_eq!(access.call_count(), 0);
        }
    })
    .unwrap();
}

#[test]
fn nested_store_permissions_keep_safe_error_categories() {
    for source in [
        Box::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "private-file secret-key",
        )) as Box<dyn std::error::Error + Send + Sync>,
        Box::new(object_store::Error::Unauthenticated {
            path: "private-file".to_owned(),
            source: "secret-key".into(),
        }),
    ] {
        let error = object_store::Error::Generic {
            store: "test-provider",
            source,
        };
        let safe = safe_store_error(error);
        assert!(matches!(safe, object_store::Error::PermissionDenied { .. }));
        let error = io_error(ErrorCode::BlobRead, &safe.into(), "read");
        assert_eq!(error.code as i32, ErrorCode::BlobAccess as i32);
        assert!(!error.message.contains("private-file"));
        assert!(!error.message.contains("secret-key"));
    }
    let safe = safe_store_error(object_store::Error::Generic {
        store: "test-provider",
        source: "permission denied: arbitrary provider text".into(),
    });
    let error = io_error(ErrorCode::BlobRead, &safe.into(), "read");
    assert_eq!(error.code as i32, ErrorCode::BlobRead as i32);
}

#[test]
fn provider_denial_precedes_construction_and_same_bucket_scopes_are_isolated() {
    crate::runtime::block_on(async {
        let access = Access::new();
        let created = Arc::new(AtomicUsize::new(0));
        let options = Arc::new(Mutex::new(Vec::new()));
        let inner = Arc::new(RecordingProvider {
            inner: Arc::new(InMemory::new()),
            created: created.clone(),
            options: options.clone(),
        });
        let registry = ObjectStoreRegistry::empty();
        registry.insert("s3", provider(&access, inner));
        access.deny("denied", None);
        let params = ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
                HashMap::from([("dataset_secret".to_owned(), "must-not-propagate".to_owned())]),
            ))),
            ..Default::default()
        };
        assert!(registry
            .get_store(Url::parse("s3://bucket/denied").unwrap(), &params)
            .await
            .is_err());
        assert_eq!(created.load(Ordering::SeqCst), 0);
        for name in ["first", "second"] {
            access.policy.scopes.lock().unwrap().push((
                name.to_owned(),
                HashMap::from([("scope".to_owned(), name.to_owned())]),
            ));
        }
        let first_url = Url::parse("s3://bucket/first").unwrap();
        let first = registry
            .get_store(first_url.clone(), &params)
            .await
            .unwrap();
        let second = registry
            .get_store(Url::parse("s3://bucket/second").unwrap(), &params)
            .await
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        let cached = registry.get_store(first_url, &params).await.unwrap();
        assert!(Arc::ptr_eq(&first, &cached));
        assert_eq!(created.load(Ordering::SeqCst), 2);
        let seen = options.lock().unwrap();
        assert_eq!(
            seen[0],
            HashMap::from([("scope".to_owned(), "first".to_owned())])
        );
        assert_eq!(
            seen[1],
            HashMap::from([("scope".to_owned(), "second".to_owned())])
        );
        assert!(!first.store_prefix.contains("first"));
        assert_ne!(first.store_prefix, second.store_prefix);
    })
    .unwrap();
}

#[derive(Debug)]
struct ScopedProvider {
    stores: HashMap<String, Arc<dyn Store>>,
}

#[async_trait]
impl ObjectStoreProvider for ScopedProvider {
    async fn new_store(&self, url: Url, params: &ObjectStoreParams) -> lance::Result<ObjectStore> {
        let scope = &params.storage_options().unwrap()["scope"];
        RecordingProvider {
            inner: self.stores[scope].clone(),
            created: Default::default(),
            options: Default::default(),
        }
        .new_store(url, params)
        .await
    }
}

#[test]
fn object_scopes_under_one_base_apply_to_metadata_reads_and_lists() {
    crate::runtime::block_on(async {
        let access = Access::new();
        let reads = Arc::new(AtomicUsize::new(0));
        let mut stores = HashMap::new();
        for (name, payload) in [("first", "one"), ("second", "second"), ("base", "base")] {
            let memory = Arc::new(InMemory::new());
            if name != "base" {
                memory
                    .put(
                        &Path::from(format!("media/{name}/object")),
                        payload.as_bytes().to_vec().into(),
                    )
                    .await
                    .unwrap();
            }
            stores.insert(
                name.to_owned(),
                Arc::new(RecordingStore {
                    inner: memory,
                    reads: reads.clone(),
                    short: false,
                    omit_etag: false,
                }) as Arc<dyn Store>,
            );
            // Put the default base scope last, after the more specific scopes.
            let scope = if name == "base" {
                "/media".to_owned()
            } else {
                format!("/media/{name}")
            };
            access.policy.scopes.lock().unwrap().push((
                scope,
                HashMap::from([("scope".to_owned(), name.to_owned())]),
            ));
        }
        let provider = provider(&access, Arc::new(ScopedProvider { stores }));
        let store = provider
            .new_store(
                Url::parse("s3://bucket/media").unwrap(),
                &ObjectStoreParams::default(),
            )
            .await
            .unwrap();
        for (name, payload) in [
            ("first", b"one".as_slice()),
            ("second", b"second".as_slice()),
        ] {
            let path = Path::from(format!("media/{name}/object"));
            assert_eq!(
                store.inner.head(&path).await.unwrap().size,
                payload.len() as u64
            );
            assert_eq!(
                store
                    .inner
                    .get_range(&path, 0..payload.len() as u64)
                    .await
                    .unwrap()
                    .as_ref(),
                payload
            );
            let prefix = Path::from(format!("media/{name}"));
            let listed = store
                .inner
                .list(Some(&prefix))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(listed.len(), 1);
            assert_eq!(listed[0].location, path);
            let listed = store
                .inner
                .list_with_delimiter(Some(&prefix))
                .await
                .unwrap();
            assert_eq!(listed.objects.len(), 1);
            assert_eq!(listed.objects[0].location, path);
        }
        let count = reads.load(Ordering::SeqCst);
        access.deny("/media/first", Some(PROVIDER));
        assert!(store
            .inner
            .head(&Path::from("media/first/object"))
            .await
            .is_err());
        assert_eq!(reads.load(Ordering::SeqCst), count);
    })
    .unwrap();
}

#[test]
fn s3_object_requests_use_the_object_scoped_signing_credentials() {
    use std::io::{Read, Write};
    for head in [true, false] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            let mut socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "no S3 request received"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                    Err(error) => panic!("S3 listener: {error}"),
                }
            };
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let count = socket.read(&mut buffer).unwrap();
                assert!(count != 0 && request.len() < 65536);
                request.extend_from_slice(&buffer[..count]);
            }
            let request = String::from_utf8(request).unwrap();
            let authorized = request.contains("Credential=OBJECT/");
            let status = if authorized {
                "200 OK"
            } else {
                "403 Forbidden"
            };
            write!(socket, "HTTP/1.1 {status}\r\nContent-Length: 5\r\nETag: \"test\"\r\nLast-Modified: Wed, 14 Oct 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n").unwrap();
            if !head {
                socket.write_all(b"bytes").unwrap();
            }
            authorized
        });
        let result = crate::runtime::block_on(async {
            let access = Access::new();
            for (scope, key) in [("/media/private/", "OBJECT"), ("/media", "BASE")] {
                access.policy.scopes.lock().unwrap().push((
                    scope.into(),
                    HashMap::from([
                        ("aws_access_key_id".into(), key.into()),
                        ("aws_secret_access_key".into(), "test-secret".into()),
                        ("aws_region".into(), "us-east-1".into()),
                        ("aws_endpoint".into(), endpoint.clone()),
                        ("aws_allow_http".into(), "true".into()),
                        ("client_max_retries".into(), "0".into()),
                    ]),
                ));
            }
            let session = session(access.context());
            let (store, _) = ObjectStore::from_uri_and_params(
                session.store_registry(),
                "s3://bucket/media",
                &ObjectStoreParams::default(),
            )
            .await
            .unwrap();
            store
                .inner
                .get_opts(
                    &Path::from("media/private/object"),
                    GetOptions {
                        head,
                        ..Default::default()
                    },
                )
                .await
        })
        .unwrap();
        let authorized = server.join().unwrap();
        assert!(authorized, "the S3 request used the base credential");
        let result = result.unwrap();
        assert_eq!(result.meta.size, 5);
        if !head {
            assert_eq!(
                crate::runtime::block_on(result.bytes())
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                b"bytes"
            );
        }
    }
}

#[test]
fn cached_store_checks_each_object_metadata_ranges_and_lists() {
    crate::runtime::block_on(async {
        let access = Access::new();
        let policy = access.policy.clone();
        let reads = Arc::new(AtomicUsize::new(0));
        let memory = Arc::new(InMemory::new());
        memory
            .put(&Path::from("allowed"), b"abcdef".to_vec().into())
            .await
            .unwrap();
        let inner = Arc::new(RecordingProvider {
            inner: Arc::new(RecordingStore {
                inner: memory,
                reads: reads.clone(),
                short: false,
                omit_etag: false,
            }),
            created: Arc::new(AtomicUsize::new(0)),
            options: Default::default(),
        });
        let registry = ObjectStoreRegistry::empty();
        registry.insert("s3", provider(&access, inner));
        let store = registry
            .get_store(
                Url::parse("s3://bucket/base").unwrap(),
                &ObjectStoreParams::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .inner
                .get_range(&Path::from("allowed"), 1..4)
                .await
                .unwrap()
                .as_ref(),
            b"bcd"
        );
        let count = reads.load(Ordering::SeqCst);
        access.deny("blocked", None);
        for head in [false, true] {
            assert!(store
                .inner
                .get_opts(
                    &Path::from("blocked"),
                    GetOptions {
                        head,
                        ..Default::default()
                    }
                )
                .await
                .is_err());
        }
        assert!(store
            .inner
            .get_ranges(&Path::from("blocked"), &[0..1, 3..4])
            .await
            .is_err());
        assert!(store
            .inner
            .list(Some(&Path::from("blocked")))
            .try_collect::<Vec<_>>()
            .await
            .is_err());
        assert!(store
            .inner
            .list_with_delimiter(Some(&Path::from("blocked")))
            .await
            .is_err());
        assert_eq!(reads.load(Ordering::SeqCst), count);
        assert!(store
            .inner
            .put(&Path::from("allowed"), vec![1].into())
            .await
            .is_err());
        assert!(store.inner.delete(&Path::from("allowed")).await.is_err());
        assert!(store
            .inner
            .copy(&Path::from("allowed"), &Path::from("copy"))
            .await
            .is_err());
        drop(access);
        assert_eq!(policy.released.load(Ordering::SeqCst), 0);
        assert!(store.inner.head(&Path::from("allowed")).await.is_ok());
        let mut listed = store.inner.list(None);
        drop(store);
        drop(registry);
        // Client selection is asynchronous, and listing may be polled after
        // every original owner is gone. Keep its access lease until teardown.
        assert_eq!(policy.released.load(Ordering::SeqCst), 0);
        assert_eq!(
            listed.next().await.unwrap().unwrap().location,
            Path::from("allowed")
        );
        assert_eq!(policy.released.load(Ordering::SeqCst), 0);
        assert!(listed.next().await.is_none());
        drop(listed);
        assert_released(&policy);
    })
    .unwrap();
}

#[test]
fn every_local_scheme_mediates_large_known_and_unknown_size_reads() {
    crate::runtime::block_on(async {
        let directory = TempDir::new();
        let file = directory.0.join("large object%name.bin");
        std::fs::write(&file, vec![42; 128 * 1024]).unwrap();
        for scheme in ["file", "file+uring", "file-object-store"] {
            let access = Access::new();
            let reads = Arc::new(AtomicUsize::new(0));
            let registry = Arc::new(ObjectStoreRegistry::empty());
            registry.insert(
                scheme,
                provider(
                    &access,
                    Arc::new(RecordingProvider {
                        inner: Arc::new(RecordingStore {
                            inner: Arc::new(LocalFileSystem::new()),
                            reads: reads.clone(),
                            short: false,
                            omit_etag: false,
                        }),
                        created: Default::default(),
                        options: Default::default(),
                    }),
                ),
            );
            let uri = Url::from_file_path(&file).unwrap().to_string().replacen(
                "file:",
                &format!("{scheme}:"),
                1,
            );
            let (store, path) =
                ObjectStore::from_uri_and_params(registry, &uri, &ObjectStoreParams::default())
                    .await
                    .unwrap();
            let reader = store.open_with_size(&path, 128 * 1024).await.unwrap();
            assert_eq!(reader.get_range(0..8192).await.unwrap().len(), 8192);
            let count = reads.load(Ordering::SeqCst);
            assert!(count > 0, "local fast path bypassed the recording store");
            access.deny("large%20object", Some(READ));
            assert!(reader.get_range(8192..16384).await.is_err());
            let unknown = store.open(&path).await.unwrap();
            assert!(unknown.get_range(0..8192).await.is_err());
            assert_eq!(reads.load(Ordering::SeqCst), count);
        }
    })
    .unwrap();
}

#[test]
fn provider_path_inverse_preserves_hf_repository_and_encoded_names() {
    let defaults = ObjectStoreRegistry::default();
    for uri in [
        "s3://bucket/dir/a%20b%25x",
        "gs://bucket/dir/a%20b",
        "az://container/dir/file",
        "oss://bucket/dir/file",
        "file:///tmp/a%20b%25x",
        "hf://datasets/owner/repo/table.lance",
        "hf://datasets/owner/repo/a%20b%25x",
    ] {
        let url = Url::parse(uri).unwrap();
        let provider = defaults.get_provider(url.scheme()).unwrap();
        let target = StoreTarget::new(&url, provider.as_ref()).unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(target.uri(&path).unwrap(), url);
        if url.scheme() == "hf" {
            assert_eq!(
                target
                    .uri(&Path::from("table.lance/_versions/1.manifest"))
                    .unwrap()
                    .as_str(),
                "hf://datasets/owner/repo/table.lance/_versions/1.manifest"
            );
        }
    }
}

#[test]
fn provider_path_inverse_preserves_encoded_separators() {
    let defaults = ObjectStoreRegistry::default();
    for root in [
        "s3://bucket",
        "gs://bucket",
        "az://container",
        "oss://bucket",
        "file://",
        "file-object-store://",
        "hf://datasets/owner/repo",
        "hf://datasets/owner%20name/repo%252Fname",
    ] {
        for (encoded, decoded, canonical) in [
            ("dir%2Ftable.lance", "dir/table.lance", "dir/table.lance"),
            (
                "dir%2ftable.lance/_versions/1.manifest",
                "dir/table.lance/_versions/1.manifest",
                "dir/table.lance/_versions/1.manifest",
            ),
            (
                "dir%2Fsubdir%2Ftable.lance/",
                "dir/subdir/table.lance",
                "dir/subdir/table.lance",
            ),
            (
                "dir%252Fname%2Ftable.lance",
                "dir%2Fname/table.lance",
                "dir%252Fname/table.lance",
            ),
        ] {
            let url = Url::parse(&format!("{root}/{encoded}")).unwrap();
            let provider = defaults.get_provider(url.scheme()).unwrap();
            let path = provider.extract_path(&url).unwrap();
            assert_eq!(path.as_ref(), decoded, "{url}");
            let target = StoreTarget::new(&url, provider.as_ref()).unwrap();
            let reconstructed = target.uri(&path).unwrap();
            assert_eq!(reconstructed.as_str(), format!("{root}/{canonical}"));
            assert_eq!(provider.extract_path(&reconstructed).unwrap(), path);
            assert_eq!(
                target
                    .uri(&Path::from("other/payload.bin"))
                    .unwrap()
                    .as_str(),
                format!("{root}/other/payload.bin")
            );
        }
    }
    // HF also accepts an encoded delimiter as an empty repository-relative path.
    for suffix in ["", "/", "/%2F"] {
        let url = Url::parse(&format!("hf://datasets/owner/repo{suffix}")).unwrap();
        let provider = defaults.get_provider("hf").unwrap();
        assert!(provider.extract_path(&url).unwrap().is_root());
        let target = StoreTarget::new(&url, provider.as_ref()).unwrap();
        assert_eq!(
            target.uri(&Path::from("payload.bin")).unwrap().as_str(),
            "hf://datasets/owner/repo/payload.bin"
        );
    }
}

#[test]
fn local_provider_path_inverse_uses_filesystem_normalization() {
    let defaults = ObjectStoreRegistry::default();
    let drive = if cfg!(windows) { "C:/" } else { "" };
    for scheme in ["file", "file-object-store", "file+uring"] {
        let Some(provider) = defaults.get_provider(scheme) else {
            continue;
        };
        for encoded in [
            "dir//table.lance",
            "dir///table.lance",
            "dir%2F%2Ftable.lance",
            "dir/%2Ftable.lance",
            "dir/table.lance//",
        ] {
            let url = Url::parse(&format!("{scheme}:///{drive}{encoded}")).unwrap();
            let path = provider.extract_path(&url).unwrap();
            assert_eq!(path.as_ref(), format!("{drive}dir/table.lance"));
            let target = StoreTarget::new(&url, provider.as_ref()).unwrap();
            let canonical = target.uri(&path).unwrap();
            assert_eq!(
                canonical.as_str(),
                format!("{scheme}:///{drive}dir/table.lance")
            );
            assert_eq!(provider.extract_path(&canonical).unwrap(), path);
            assert_eq!(
                target
                    .uri(&Path::from(format!("{drive}other/payload.bin")))
                    .unwrap()
                    .as_str(),
                format!("{scheme}:///{drive}other/payload.bin")
            );
        }
    }
}

#[test]
fn hf_construction_and_private_uri_rejection() {
    crate::runtime::block_on(async {
        let access = Access::new();
        let session = session(access.context());
        let registry = session.store_registry();
        let uri = Url::parse("hf://datasets/owner/repo/dir%2Ftable.lance").unwrap();
        // The pinned HF provider constructs an OpenDAL store without discovery.
        // No public dataset or network reads are required by this experiment.
        let store = registry
            .get_store(uri.clone(), &ObjectStoreParams::default())
            .await
            .unwrap();
        assert_eq!(
            access
                .policy
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, op, _)| *op != PROVIDER)
                .count(),
            0
        );
        access.deny("owner/repo", None);
        assert!(registry
            .get_store(uri, &ObjectStoreParams::default())
            .await
            .is_err());
        drop(store);
    })
    .unwrap();
    for uri in [
        "",
        "s3://user:password@bucket/path",
        "s3://bucket/path?sig=secret",
        "s3://bucket/path#secret",
        "memory://table",
        "s3+ddb://bucket/path",
        "https://host/path",
        "s3://user:pass@host:invalid/path",
        " s3://bucket/path",
    ] {
        assert!(normalize_uri(uri).is_err());
    }
}

#[test]
fn short_response_is_rejected_before_the_lance_scheduler_slices_it() {
    crate::runtime::block_on(async {
        for short_body in [true, false] {
            let access = Access::new();
            let memory = Arc::new(InMemory::new());
            let path = Path::from("payload");
            memory.put(&path, vec![42u8; 16384].into()).await.unwrap();
            let provider = provider(
                &access,
                Arc::new(RecordingProvider {
                    inner: Arc::new(RecordingStore {
                        inner: memory.clone(),
                        reads: Default::default(),
                        short: short_body,
                        omit_etag: false,
                    }),
                    created: Default::default(),
                    options: Default::default(),
                }),
            );
            let store = provider
                .new_store(
                    Url::parse("s3://bucket/payload").unwrap(),
                    &ObjectStoreParams::default(),
                )
                .await
                .unwrap();
            let blob = lance::dataset::BlobFile::new_packed(Arc::new(store), path.clone(), 7, 8000);
            if !short_body {
                // Cache the original file size, then let a compliant backend
                // clip a later GET at the replacement object's smaller EOF.
                assert_eq!(blob.read_range(0..1).await.unwrap().as_ref(), &[42]);
                memory.put(&path, vec![42u8; 32].into()).await.unwrap();
            }
            let error = blob.read_range(0..100).await.unwrap_err();
            assert!(error.to_string().contains("short backing read"));
        }
    })
    .unwrap();
}

pub(crate) async fn dataset_without_manifest_etag(uri: &str) -> lance::Dataset {
    let access = Access::new();
    let registry = Arc::new(ObjectStoreRegistry::empty());
    registry.insert(
        "file",
        provider(
            &access,
            Arc::new(RecordingProvider {
                inner: Arc::new(RecordingStore {
                    inner: Arc::new(LocalFileSystem::new()),
                    reads: Default::default(),
                    short: false,
                    omit_etag: true,
                }),
                created: Default::default(),
                options: Default::default(),
            }),
        ),
    );
    // Exact-version resolution uses HEAD, whose missing ETag is intentional.
    lance::dataset::builder::DatasetBuilder::from_uri(uri)
        .with_version(1)
        .with_session(Arc::new(Session::new(0, 0, registry)))
        .load()
        .await
        .unwrap()
}
