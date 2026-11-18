"""Summarize the nine actual end-to-end wall-clock observations."""
import json
from pathlib import Path
import statistics
import sys

root = Path(sys.argv[1])
runs = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(root.glob("run*/timing.json"))]
if len(runs) != 9 or any(r["exit_status"] != 0 for r in runs):
    raise SystemExit("All nine runs must exist and pass before producing a comparison")
out = {}
for profile, config in (("full", "reference"), ("7d", "reference"), ("full", "alternative")):
    group = [r for r in runs if r["profile"] == profile and r["config"] == config]
    if len(group) != 3:
        raise SystemExit(f"Expected exactly three runs for {profile}/{config}")
    seconds = [r["seconds"] for r in group]
    out[f"{config}_{profile}"] = {"runs": [r["run_id"] for r in group], "seconds": seconds,
                                   "median_seconds": statistics.median(seconds),
                                   "minimum_seconds": min(seconds), "maximum_seconds": max(seconds)}
print(json.dumps(out, indent=2))
