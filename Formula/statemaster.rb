class Statemaster < Formula
  desc "State-machine database: owns entity lifecycles, transitions, and a change stream"
  homepage "https://github.com/statemaster/statemaster"
  url "https://github.com/statemaster/statemaster/archive/refs/tags/v0.1.0.tar.gz"
  sha256 "716397a286e197b7fb6978637f687f189abd45df26034c363185019addc35b86"
  license "AGPL-3.0-only"
  head "https://github.com/statemaster/statemaster.git", branch: "main"

  depends_on "rust" => :build

  # This block is regenerated on each release by .github/workflows/release.yml.
  bottle do
    root_url "https://github.com/statemaster/statemaster/releases/download/v0.1.0"
    sha256 cellar: :any_skip_relocation, arm64_sonoma: "8d02561455a35c5fbde78900d112f7e7849d27dab4bfdf54401f49c3fadf664b"
    sha256 cellar: :any_skip_relocation, ventura: "15494465ae225176189084481286d4dbaa70a4baf5435e9f19ab91cbdfc9b218"
    sha256 cellar: :any_skip_relocation, x86_64_linux: "b0c006d94fdcb01ebeda633ca0bc9a71fd749a0042d5aea9e11417c5cfd17c1a"
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
