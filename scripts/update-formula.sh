#!/usr/bin/env bash
#
# Regenerate Formula/statemaster.rb for a release: pin the source tarball URL +
# sha256 for the tag, and write a `bottle do` block from the per-platform
# checksums collected in `shas/sha-*.txt` (produced by build-bottle.sh).
#
# Env in: VERSION (e.g. 0.1.0), TAG (e.g. v0.1.0).
set -euo pipefail

VERSION="${VERSION:?VERSION not set}"
TAG="${TAG:?TAG not set}"
REPO="statemaster/statemaster"
FORMULA="Formula/statemaster.rb"

src_url="https://github.com/${REPO}/archive/refs/tags/${TAG}.tar.gz"
src_sha="$(curl -fsSL "${src_url}" | sha256sum | awk '{print $1}')"
root_url="https://github.com/${REPO}/releases/download/${TAG}"

bottle_lines=""
for f in shas/sha-*.txt; do
  [ -e "${f}" ] || continue
  read -r btag bsha < "${f}"
  bottle_lines+="    sha256 cellar: :any_skip_relocation, ${btag}: \"${bsha}\"
"
done

cat > "${FORMULA}" <<EOF
class Statemaster < Formula
  desc "State-machine database: owns entity lifecycles, transitions, and a change stream"
  homepage "https://github.com/${REPO}"
  url "${src_url}"
  sha256 "${src_sha}"
  license "AGPL-3.0-only"
  head "https://github.com/${REPO}.git", branch: "main"

  depends_on "rust" => :build

  # This block is regenerated on each release by .github/workflows/release.yml.
  bottle do
    root_url "${root_url}"
${bottle_lines}  end

  def install
    system "cargo", "build", "--release", "--bin", "smdbd", "--bin", "smdbctl", "--bin", "smash"
    bin.install "target/release/smdbd"
    bin.install "target/release/smdbctl"
    bin.install "target/release/smash"
  end

  service do
    run [opt_bin/"smdbd", "--data-dir", var/"statemaster"]
    keep_alive true
    log_path var/"log/smdbd.log"
    error_log_path var/"log/smdbd.log"
  end

  test do
    assert_match "smdbd", shell_output("#{bin}/smdbd --help")
    assert_match "smdbctl", shell_output("#{bin}/smdbctl --help")
    assert_match "smash", shell_output("#{bin}/smash --help")
  end
end
EOF

echo "wrote ${FORMULA} (source sha256 ${src_sha})"
