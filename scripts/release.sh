#!/usr/bin/env bash
# Release StateMaster: verify the tree, run checks, tag vX.Y.Z, push the tag.
# CI (.github/workflows/release.yml) then builds the Homebrew bottles and
# pushes the multi-arch Docker image to Docker Hub.
#
# Usage:  scripts/release.sh <version>      e.g. scripts/release.sh 0.1.0
#
# No credentials belong in this file or in the repo. CI reads them from
# GitHub Actions secrets (DOCKERHUB_USERNAME / DOCKERHUB_TOKEN). For local
# overrides you may create a .release.env file (gitignored) next to the
# repo root; it is sourced if present.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if [[ -f .release.env ]]; then
  # shellcheck disable=SC1091
  source .release.env
fi

VERSION="${1:?usage: scripts/release.sh <version>  (e.g. 0.1.0)}"
VERSION="${VERSION#v}"
TAG="v${VERSION}"

# --- sanity checks ----------------------------------------------------------

if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  echo "error: '$VERSION' is not a semver version" >&2; exit 1
fi

CARGO_VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
if [[ "$CARGO_VERSION" != "$VERSION" ]]; then
  echo "error: Cargo.toml workspace version is $CARGO_VERSION, not $VERSION." >&2
  echo "       Bump [workspace.package] version first and commit it." >&2
  exit 1
fi

BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$BRANCH" != "main" ]]; then
  echo "error: releases are cut from main (currently on '$BRANCH')" >&2; exit 1
fi

if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "error: working tree has uncommitted changes" >&2; exit 1
fi

git fetch origin
if [[ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]]; then
  echo "error: local main and origin/main differ; push or pull first" >&2; exit 1
fi

if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  echo "error: tag $TAG already exists" >&2; exit 1
fi

# --- quality gates ----------------------------------------------------------

echo "==> cargo fmt --check"
cargo fmt --check

echo "==> cargo clippy"
cargo clippy --workspace --all-targets -- -D warnings

echo "==> cargo test"
cargo test --workspace

# --- tag and push -----------------------------------------------------------

echo "==> tagging $TAG"
git tag -a "$TAG" -m "StateMaster $VERSION"
git push origin "$TAG"

echo
echo "Release $TAG pushed. CI will now:"
echo "  - build Homebrew bottles and attach them to the GitHub release"
echo "  - update Formula/statemaster.rb on main"
echo "  - push statemaster/statemaster:$VERSION and :latest to Docker Hub"
echo
echo "Watch it: $(git remote get-url origin | sed -e 's/\.git$//' -e 's#^git@github.com:#https://github.com/#')/actions"
