#!/usr/bin/env python3
"""Regression tests for the architecture dependency gate."""

import importlib.util
from pathlib import Path
import sys
import unittest


sys.dont_write_bytecode = True
SCRIPT = Path(__file__).with_name("check-architecture.py")
SPEC = importlib.util.spec_from_file_location("check_architecture", SCRIPT)
CHECK = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(CHECK)


def metadata(*dependencies: str) -> dict:
    return {
        "packages": [
            {
                "name": "nacelle-core",
                "dependencies": [{"name": dependency} for dependency in dependencies],
            }
        ]
    }


class ArchitectureGateTests(unittest.TestCase):
    def test_current_layer_dependencies_are_allowed(self):
        self.assertEqual(CHECK.violations(metadata("bytes", "metrics", "tokio")), [])

    def test_transport_dependency_is_rejected(self):
        self.assertEqual(CHECK.violations(metadata("bytes", "nacelle-tcp")), ["nacelle-tcp"])

    def test_missing_core_package_fails_closed(self):
        self.assertTrue(CHECK.violations({"packages": []}))


if __name__ == "__main__":
    unittest.main()
