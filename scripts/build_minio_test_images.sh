#!/usr/bin/env bash
set -euo pipefail

# Keep the upstream releases pinned; their registry images are no longer public.
minio_release="${1:-RELEASE.2025-09-07T16-13-09Z}"
case "$minio_release" in
  RELEASE.2025-09-07T16-13-09Z)
    minio_sha256=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f
    ;;
  RELEASE.2025-07-23T15-54-02Z)
    minio_sha256=eef6581f6509f43ece007a6f2eb4c5e3ce41498c8956e919a7ac7b4b170fa431
    ;;
  *)
    echo "Unsupported MinIO test release: $minio_release" >&2
    exit 2
    ;;
esac
mc_release=RELEASE.2025-08-13T08-35-41Z
mc_sha256=01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891

build_dir="$(mktemp -d)"
trap 'rm -rf "$build_dir"' EXIT
for component in minio mc; do
  if [[ "$component" == minio ]]; then
    release="$minio_release"
    sha256="$minio_sha256"
  else
    release="$mc_release"
    sha256="$mc_sha256"
  fi
  curl --fail --location --silent --show-error --retry 3 \
    --connect-timeout 10 --max-time 180 \
    "https://github.com/minio/$component/releases/download/$release/$component.linux-amd64.$release" \
    --output "$build_dir/$component"
  printf '%s  %s\n' "$sha256" "$build_dir/$component" | sha256sum --check --strict
  chmod 0755 "$build_dir/$component"
done

# Both binaries are static; BusyBox supplies the shell used by bucket setup.
cat > "$build_dir/Dockerfile" <<'DOCKERFILE'
FROM busybox:1.37.0-musl@sha256:dc88b80842580654294472735332ec7c7789a4010ec9a68b69f9b88c19abede6 AS runtime
COPY minio mc /usr/bin/
WORKDIR /data

FROM runtime AS minio
ENTRYPOINT ["/usr/bin/minio"]

FROM runtime AS mc
ENTRYPOINT ["/usr/bin/mc"]
DOCKERFILE
docker build --platform linux/amd64 --target minio \
  --tag "astrovela-minio-test:$minio_release" "$build_dir"
docker build --platform linux/amd64 --target mc \
  --tag "astrovela-mc-test:$mc_release" "$build_dir"
