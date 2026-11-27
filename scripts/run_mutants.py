"""Run the mutants (roadmap R2): each breaks one guarantee; the test suite must catch every one.

    python scripts/run_mutants.py [--jobs 3] [NAME ...]          (from the repository root)

Each mutant is applied to its own copy of the committed tree (`git archive HEAD`), whose code is
imported first (PYTHONPATH), and the suite runs until its first failure. A mutant is CAUGHT when a
test fails, SURVIVED when the suite passes. The mutants are `scripts/mutants.json`: a title and
exact (file, old, new) edits, each `old` found once.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGES = ["agentic", "mcp", "console", "opensandbox", "postgres", "coding"]


def run(name: str, mutant: dict, base: Path) -> tuple[str, str, str]:
    work = Path(tempfile.mkdtemp(prefix=f"mutant-{name}-", dir=base))
    try:
        archive = subprocess.run(
            ["git", "archive", "HEAD"], cwd=ROOT, capture_output=True, check=True
        )
        subprocess.run(["tar", "-x", "-C", str(work)], input=archive.stdout, check=True)
        for file, old, new in mutant["edits"]:
            path = work / file
            text = path.read_text()
            if text.count(old) != 1:
                return name, "STALE", f"{file}: the text to change is not there once"
            path.write_text(text.replace(old, new))
        paths = [str(work), *(str(work / "packages" / f"statecraft-{p}") for p in PACKAGES)]
        paths.append(str(work / "examples" / "notes_plugin"))
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(paths), "PYTHONDONTWRITEBYTECODE": "1"}
        out = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-q",
                "-x",
                "-p",
                "no:cacheprovider",
                "--timeout",
                "90",
            ],
            cwd=work,
            env=env,
            capture_output=True,
            text=True,
            timeout=1200,
        ).stdout
        failed = [line for line in out.splitlines() if line.startswith(("FAILED", "ERROR"))]
        if failed:
            return name, "CAUGHT", failed[0][:160]
        summary = [line for line in out.splitlines() if " passed" in line or " error" in line]
        return name, "SURVIVED", (summary or ["?"])[-1][:160]
    except subprocess.TimeoutExpired:
        return name, "CAUGHT", "the suite hung (timeout)"
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("names", nargs="*")
    parser.add_argument("--jobs", type=int, default=3)
    args = parser.parse_args()
    mutants = json.loads((ROOT / "scripts" / "mutants.json").read_text())
    chosen = {n: m for n, m in mutants.items() if not args.names or n in args.names}
    base = Path(tempfile.mkdtemp(prefix="statecraft-mutants-"))
    results = []
    with ThreadPoolExecutor(args.jobs) as pool:
        for name, verdict, detail in pool.map(lambda kv: run(kv[0], kv[1], base), chosen.items()):
            print(f"{verdict:9} {name:28} {chosen[name]['title'][:50]:50} {detail}", flush=True)
            results.append(verdict)
    shutil.rmtree(base, ignore_errors=True)
    caught = results.count("CAUGHT")
    print(f"caught {caught}/{len(results)}")
    return 0 if caught == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
