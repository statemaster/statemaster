#!/usr/bin/env bash
#
# Build a Homebrew bottle tarball for the StateMaster binaries on the current
# platform. A bottle is just a gzipped tar of the keg laid out as
# `<name>/<version>/...` under the Cellar; because our binaries embed no install
# paths the bottle is relocation-free (`cellar :any_skip_relocation`).
#
# Env in:  VERSION (e.g. 0.1.0), BOTTLE_TAG (e.g. arm64_sonoma, ventura,
#          x86_64_linux). Optional TARGET (a Rust target triple) cross-compiles
#          — e.g. x86_64-apple-darwin on an Apple-Silicon runner so we never
#          depend on a scarce Intel macOS runner. Writes the tarball + a
#          `sha-<tag>.txt` checksum file and, under Actions, sets step outputs.
set -euo pipefail

VERSION="${VERSION:?VERSION not set}"
BOTTLE_TAG="${BOTTLE_TAG:?BOTTLE_TAG not set}"
TARGET="${TARGET:-}"
NAME="statemaster"
BINS=(smdbd smdbctl smash)

if [ -n "${TARGET}" ]; then
  rustup target add "${TARGET}"
  cargo build --release --target "${TARGET}" "${BINS[@]/#/--bin=}"
  bindir="target/${TARGET}/release"
else
  cargo build --release "${BINS[@]/#/--bin=}"
  bindir="target/release"
fi

keg="${NAME}/${VERSION}"
rm -rf "${NAME}"
mkdir -p "${keg}/bin" "${keg}/.brew"
for b in "${BINS[@]}"; do
  install -m 0755 "${bindir}/${b}" "${keg}/bin/${b}"
done
# Homebrew bottles carry a copy of the formula under .brew.
cp Formula/statemaster.rb "${keg}/.brew/statemaster.rb"

tarball="${NAME}-${VERSION}.${BOTTLE_TAG}.bottle.tar.gz"
# -n on gzip drops the timestamp so the tarball is reproducible across runs.
tar -cf - "${NAME}" | gzip -n -9 > "${tarball}"

if command -v sha256sum >/dev/null 2>&1; then
  sha="$(sha256sum "${tarball}" | awk '{print $1}')"
else
  sha="$(shasum -a 256 "${tarball}" | awk '{print $1}')"
fi

echo "built ${tarball}"
echo "sha256 ${sha}"
echo "${BOTTLE_TAG} ${sha}" > "sha-${BOTTLE_TAG}.txt"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  {
    echo "tarball=${tarball}"
    echo "sha256=${sha}"
  } >> "${GITHUB_OUTPUT}"
fi
