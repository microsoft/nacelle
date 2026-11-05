#!/usr/bin/env python3
"""Exercise launcher ownership and failure handling without Cargo builds or sockets."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


class StressLauncherTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="nacelle-launcher-")
        self.root = Path(self.temporary.name)
        self.bin = self.root / "bin"
        self.release = self.root / "target" / "test-host" / "release"
        self.bin.mkdir()
        self.release.mkdir(parents=True)
        self.script(self.bin / "rustc", "print('host: test-host')")
        self.env = {
            **os.environ,
            "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
            "CARGO_TARGET_DIR": str(self.root / "target"),
            "TEST_ROOT": str(self.root),
        }
        self.script(self.bin / "cargo", """
import json, os, sys
from pathlib import Path
Path(os.environ["TEST_ROOT"], "build.json").write_text(json.dumps(sys.argv[1:]))
if sys.argv[1] != "build":
    sys.exit(19)
""")
        self.script(self.bin / "pkill", """
import os
from pathlib import Path
Path(os.environ["TEST_ROOT"], "name-kill").touch()
""")
        self.script(self.bin / "ss", """
import os
from pathlib import Path
root = Path(os.environ["TEST_ROOT"])
if os.environ.get("TEST_BUSY") == "1" or (
    os.environ.get("TEST_BUSY_AFTER_BUILD") == "1" and (root / "build.json").exists()
):
    print('LISTEN 0 128 127.0.0.1:17882 *:* users:(("other",pid=1,fd=3))')
elif (root / "server.pid").exists():
    pid = (root / "server.pid").read_text()
    print(f'LISTEN 0 128 127.0.0.1:17882 *:* users:(("server",pid={pid},fd=3))')
""")
        self.script(self.release / "nacelle-stress-server", """
import json, os, signal, sys
from pathlib import Path
root = Path(os.environ["TEST_ROOT"])
if os.environ.get("TEST_SERVER_FAIL") == "1":
    sys.exit(23)
assert (root / "build.json").exists()
root.joinpath("server.json").write_text(json.dumps(sys.argv[1:]))
def stop(*_):
    root.joinpath("server.stopped").touch()
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
root.joinpath("server.pid").write_text(str(os.getpid()))
while True:
    signal.pause()
""")
        self.script(self.release / "nacelle-stress-test", """
import json, os, sys
from pathlib import Path
root = Path(os.environ["TEST_ROOT"])
root.joinpath("client.json").write_text(json.dumps(sys.argv[1:]))
sys.exit(int(os.environ.get("TEST_CLIENT_STATUS", "0")))
""")

    def script(self, path, body):
        path.write_text(f"#!{sys.executable}\n{body}")
        path.chmod(0o755)

    def tearDown(self):
        pid_file = self.root / "server.pid"
        if pid_file.exists():
            try:
                os.kill(int(pid_file.read_text()), signal.SIGTERM)
            except ProcessLookupError:
                pass
        self.temporary.cleanup()

    def run_launcher(self, **overrides):
        return subprocess.run(
            [
                "bash", str(ROOT / "examples/run-stress-test.sh"),
                "--config", "examples/nacelle-stress-server/configs/tcp.toml",
                "--bind", "127.0.0.1:17882", "--server-threads", "1",
                "--connections", "1", "--pipeline", "1", "--duration-secs", "1",
            ],
            cwd=ROOT,
            env={**self.env, **overrides},
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )

    def test_builds_before_starting_and_reaps_only_its_server(self):
        result = self.run_launcher()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads((self.root / "build.json").read_text())[0], "build")
        self.assertIn("--config", json.loads((self.root / "server.json").read_text()))
        self.assertTrue((self.root / "client.json").exists())
        self.assertTrue((self.root / "server.stopped").exists())
        self.assertFalse((self.root / "name-kill").exists())
        with self.assertRaises(ProcessLookupError):
            os.kill(int((self.root / "server.pid").read_text()), 0)

    def test_occupied_port_is_rejected_without_building_or_killing(self):
        result = self.run_launcher(TEST_BUSY="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already in use", result.stderr)
        self.assertFalse((self.root / "name-kill").exists())
        self.assertFalse((self.root / "build.json").exists())
        self.assertFalse((self.root / "client.json").exists())

    def test_server_startup_failure_does_not_launch_client(self):
        result = self.run_launcher(TEST_SERVER_FAIL="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("exited before listening", result.stderr)
        self.assertFalse((self.root / "client.json").exists())

    def test_port_taken_during_build_does_not_start_another_server(self):
        result = self.run_launcher(TEST_BUSY_AFTER_BUILD="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("already in use", result.stderr)
        self.assertTrue((self.root / "build.json").exists())
        self.assertFalse((self.root / "server.pid").exists())

    def test_client_failure_is_propagated_after_server_cleanup(self):
        result = self.run_launcher(TEST_CLIENT_STATUS="17")
        self.assertEqual(result.returncode, 17, result.stderr)
        self.assertTrue((self.root / "server.stopped").exists())


if __name__ == "__main__":
    unittest.main()
