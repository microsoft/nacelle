#!/usr/bin/env python3
"""Regression tests for the public API gate without Rust builds or checkouts."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check-public-api.sh").resolve()
REMOVALS = [
    f"pub nacelle_core::{module}{variant}"
    for module in ("error::", "")
    for variant in (
        "NacelleResourceLimitReason::MemoryBytes",
        "NacelleResourceLimitReason::MemoryLimitMismatch",
        "NacelleTimeoutReason::MemoryAllocation",
    )
]
MOCK_CARGO = r'''
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
with Path(os.environ["API_TEST_LOG"]).open("a") as log:
    log.write(json.dumps(args) + "\n")
fixture = json.loads(Path(os.environ["API_TEST_FIXTURE"]).read_text())
if args[args.index("-p") + 1] != fixture.get("package", "nacelle-core"):
    fixture = {}
if fixture.get("build_failure"):
    print("mock rustdoc build failed", file=sys.stderr)
    sys.exit(7)
if fixture.get("malformed"):
    print("unexpected output")
    sys.exit(0)
for title, key in (
    ("Removed items from the public API", "removed"),
    ("Changed items in the public API", "changed"),
    ("Added items to the public API", "added"),
):
    print(title)
    print("=" * len(title))
    lines = fixture.get(key, [])
    print("\n".join(lines) if lines else "(none)")
    print()
denied = [args[i + 1] for i, value in enumerate(args) if value == "--deny"]
if any(fixture.get(kind) for kind in denied):
    print("mock --deny compatibility failure", file=sys.stderr)
    sys.exit(1)
'''


class PublicApiGateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="nacelle-api-gate-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        (self.repo / ".gitignore").write_text("target/\n")
        self.git("init", "--quiet")
        self.git("add", ".gitignore")
        self.git(
            "-c", "user.name=API gate test",
            "-c", "user.email=api-gate-test@example.invalid",
            "-c", "commit.gpgsign=false",
            "-c", "core.hooksPath=/dev/null",
            "commit", "--quiet", "-m", "test fixture",
        )
        self.bin = self.root / "bin"
        self.bin.mkdir()
        for name in ("cargo", "cargo-public-api"):
            executable = self.bin / name
            executable.write_text(f"#!{sys.executable}\n{MOCK_CARGO}")
            executable.chmod(0o755)
        self.log = self.root / "calls.jsonl"
        self.fixture = self.root / "fixture.json"

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, text=True)

    def run_gate(self, **fixture):
        self.fixture.write_text(json.dumps(fixture))
        env = {
            **os.environ,
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "API_TEST_LOG": str(self.log),
            "API_TEST_FIXTURE": str(self.fixture),
        }
        return subprocess.run(
            ["bash", str(SCRIPT), "HEAD", "HEAD"],
            cwd=self.repo, env=env, text=True, capture_output=True,
        )

    def test_compatible_api(self):
        result = self.run_gate()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.repo / "target/public-api/revisions.txt").is_file())

    def test_exact_memory_removals_pass_both_core_lanes(self):
        result = self.run_gate(removed=[f"-{item}" for item in REMOVALS])
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertEqual(len(calls), 10)
        for args in calls:
            denied = [args[i + 1] for i, value in enumerate(args) if value == "--deny"]
            self.assertIn("changed", denied)
            self.assertEqual("removed" in denied, args[args.index("-p") + 1] != "nacelle-core")
        for provider in ("rustls", "openssl"):
            report = self.repo / f"target/public-api/nacelle-core-{provider}.diff"
            for item in REMOVALS:
                self.assertIn(f"-{item}\n", report.read_text())

    def test_unrelated_core_removals_fail(self):
        for module in ("error::", ""):
            with self.subTest(module=module):
                removal = f"-pub nacelle_core::{module}NacelleTimeoutReason::TlsHandshake"
                result = self.run_gate(removed=[removal])
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(removal, result.stderr)

    def test_mixed_accepted_and_unrelated_removals_fail(self):
        result = self.run_gate(removed=[
            f"-{item}" for item in REMOVALS
        ] + ["-pub fn nacelle_core::NacelleLimits::new() -> Self"])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unaccepted public API removal", result.stderr)

    def test_similar_names_are_not_allowlisted(self):
        result = self.run_gate(removed=[f"-{REMOVALS[0]}Extra"])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unaccepted public API removal", result.stderr)

    def test_other_packages_still_deny_removals(self):
        result = self.run_gate(package="nacelle-codec", removed=[f"-{REMOVALS[0]}"])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("mock --deny compatibility failure", result.stderr)

    def test_changed_items_remain_denied(self):
        result = self.run_gate(
            removed=[f"-{item}" for item in REMOVALS],
            changed=["-pub fn nacelle_core::existing()", "+pub fn nacelle_core::existing(u8)"],
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("mock --deny compatibility failure", result.stderr)

    def test_build_failures_remain_failures(self):
        result = self.run_gate(build_failure=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("mock rustdoc build failed", result.stderr)

    def test_unrecognized_report_format_fails(self):
        result = self.run_gate(malformed=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unexpected public API report header", result.stderr)


if __name__ == "__main__":
    unittest.main()
