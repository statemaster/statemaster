class Statemaster < Formula
  desc "State-machine database: owns entity lifecycles, transitions, and a change stream"
  homepage "https://github.com/statemaster/statemaster"
  url "https://github.com/statemaster/statemaster/archive/refs/tags/v0.1.0.tar.gz"
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  license "AGPL-3.0-only"
  head "https://github.com/statemaster/statemaster.git", branch: "main"

  depends_on "rust" => :build

  # Regenerated on each release by .github/workflows/release.yml. Until the first
  # tagged release exists, this block is empty and `brew install` builds from
  # source (still works, just slower).
  bottle do
    root_url "https://github.com/statemaster/statemaster/releases/download/v0.1.0"
  end

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
