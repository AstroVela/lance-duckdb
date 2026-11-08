#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if [ "$#" -eq 0 ]; then
  set -- autoconf automake binutils ccache curl file flex gcc gcc-c++ git libtool m4 \
    make ninja-build openssl openssl-devel patch perl-IPC-Cmd perl-Time-Piece \
    pkgconf-pkg-config tar unzip xz zip
fi

for attempt in 1 2 3; do
  if dnf --refresh install -y "$@"; then
    exit 0
  fi
  if [ "$attempt" -eq 3 ]; then
    echo 'manylinux dependency installation failed after three attempts' >&2
    exit 1
  fi
  dnf clean all
  sleep 5
done
