"""Local CLI + part parser checks. No AWS needed.

    python3 tests/test_cli.py

The end-to-end search runs only if `diamond` is on PATH (it is in the Docker
image); otherwise those checks are skipped, not failed.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

import aggregator  # noqa: E402

FAILURES = []


def check(name, cond, detail=""):
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"  [{detail}]" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


print("parse_part")
rows = aggregator.parse_part(
    "t1\t1\t100\t5\t104\t100\t87.5\t1e-50\t200.0\tMKLV\t40AG59\n"
    "t3\t1\t10\t1\t10\t10\t100.0\t1e-3\t20.0\tMKLVAAAAAA\n"   # 10-column part (pre-1.6.0 worker)
    "t2\t1\t50\t1\t50\t50\t40.0\t1e-5\t60.0\n"   # 9-column part from an older worker
    "\n")
check("three rows parsed", len(rows) == 3)
check("pident passed through un-scaled", rows[0]["percent_identity"] == 87.5)
check("full_sseq -> target_sequence", rows[0]["target_sequence"] == "MKLV")
check("9-column row -> target_sequence None", rows[2]["target_sequence"] is None)
check("btop column -> btop", rows[0]["btop"] == "40AG59")
check("10-column row -> btop None", rows[1]["btop"] is None and rows[1]["target_sequence"] == "MKLVAAAAAA")
try:
    aggregator.parse_part("t\t1\t2\t1\t2\t2\t150\t1\t1\n")
    check("pident > 100 rejected", False)
except AssertionError:
    check("pident > 100 rejected", True)

print("cli (end to end)")
if shutil.which("diamond") is None:
    print("  SKIP  diamond not on PATH")
else:
    examples = os.path.join(ROOT, "data", "example-sequences.txt")
    with tempfile.TemporaryDirectory() as d:
        subprocess.run(["diamond", "makedb", "--in", examples, "-d", os.path.join(d, "ex"), "--quiet"],
                       check=True)
        with open(examples) as f:
            ispetase = "".join(f.read().split(">")[1].splitlines()[1:])
        p = subprocess.run([sys.executable, os.path.join(ROOT, "src", "cli.py"),
                            "--db", os.path.join(d, "ex.dmnd"), ispetase, "--max-results", "2"],
                           capture_output=True, text=True)
        check("cli exits 0", p.returncode == 0, p.stderr[-300:])
        if p.returncode == 0:
            out = json.loads(p.stdout)
            top = out["results"][0]
            check("result shape", {"query_length", "num_results", "results", "engine"} <= out.keys())
            check("top hit is the self-hit", top["target_id"] == "IsPETase" and top["percent_identity"] == 100.0)
            check("max-results respected", out["num_results"] == 2)
            check("ranked by bitscore", out["results"][0]["bitscore"] >= out["results"][1]["bitscore"])
        bad = subprocess.run([sys.executable, os.path.join(ROOT, "src", "cli.py"),
                              "--db", os.path.join(d, "ex.dmnd"), "MKX"], capture_output=True, text=True)
        check("short query rejected cleanly", bad.returncode != 0 and "too short" in bad.stderr)

print()
print("FAILED: " + ", ".join(FAILURES) if FAILURES else "ALL CHECKS PASSED")
sys.exit(1 if FAILURES else 0)
