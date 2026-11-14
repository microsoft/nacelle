#!/usr/bin/env python3
"""Enforce the workspace's core-to-transport dependency boundary."""

import argparse
import json
from pathlib import Path
import subprocess
import sys


FORBIDDEN_CORE_DEPENDENCIES = {
    "nacelle",
    "nacelle-http",
    "nacelle-openssl",
    "nacelle-rustls",
    "nacelle-tcp",
}


def load_metadata(path: Path | None) -> dict:
    if path is not None:
        return json.loads(path.read_text())
    result = subprocess.run(
        ["cargo", "metadata", "--format-version=1", "--no-deps"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def violations(metadata: dict) -> list[str]:
    packages = metadata.get("packages", [])
    core = next((package for package in packages if package.get("name") == "nacelle-core"), None)
    if core is None:
        return ["cargo metadata does not contain the nacelle-core package"]
    return sorted(
        dependency["name"]
        for dependency in core.get("dependencies", [])
        if dependency.get("name") in FORBIDDEN_CORE_DEPENDENCIES
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", type=Path, help="read cargo metadata JSON from a fixture")
    args = parser.parse_args()
    found = violations(load_metadata(args.metadata))
    if found:
        print(
            "nacelle-core must not depend on facade, transport, or TLS-provider crates: "
            + ", ".join(found),
            file=sys.stderr,
        )
        return 1
    print("core dependency direction is valid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
