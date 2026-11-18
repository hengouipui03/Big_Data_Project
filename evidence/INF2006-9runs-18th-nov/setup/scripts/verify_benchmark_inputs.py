"""Verify the entire staged HDFS input file set against recorded SHA-256 values."""
import csv
import hashlib
import json
import posixpath
import subprocess
import sys

root = sys.argv[1].rstrip("/")
groups = [
    ("evidence/local/q1_2016/inventory.csv", "prices", root + "/inputs/full/prices", 1),
    ("evidence/local/q1_2016/inventory.csv", "gdelt", root + "/inputs/full/gdelt", 91),
    ("evidence/local/inventory.csv", "gdelt", root + "/inputs/dev/gdelt", 7),
]
result = []
for manifest, source, directory, expected_count in groups:
    with open(manifest, newline="", encoding="utf-8") as handle:
        rows = [r for r in csv.DictReader(handle) if r["source"] == source]
    if len(rows) != expected_count:
        raise RuntimeError(f"{manifest}: expected {expected_count} {source} files, got {len(rows)}")
    listing = subprocess.check_output(["hdfs", "dfs", "-ls", directory], text=True)
    actual = {line.split()[-1].rsplit("/", 1)[-1] for line in listing.splitlines() if line.startswith("-")}
    expected = {r["file"] for r in rows}
    if actual != expected:
        raise RuntimeError(f"{directory}: missing={sorted(expected-actual)} extra={sorted(actual-expected)}")
    for row in rows:
        path = posixpath.join(directory, row["file"])
        proc = subprocess.Popen(["hdfs", "dfs", "-cat", path], stdout=subprocess.PIPE)
        sha, size = hashlib.sha256(), 0
        while True:
            chunk = proc.stdout.read(1024 * 1024)
            if not chunk:
                break
            sha.update(chunk)
            size += len(chunk)
        if proc.wait() != 0:
            raise RuntimeError(f"Could not read {path}")
        if sha.hexdigest() != row["sha256"].lower() or size != int(row["bytes"]):
            raise RuntimeError(f"Checksum or size mismatch: {path}")
        result.append({"hdfs_path": path, "bytes": size, "sha256": sha.hexdigest(),
                       "expected_rows": int(row["rows"]), "source_inventory": manifest})
print(json.dumps({"all_verified": True, "files": result}, indent=2))
