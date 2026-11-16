"""Build notebooks/inf2006_pipeline.ipynb from notebooks/cells/*.cells (no dependencies).

Cell format: a line '#@@ md' or '#@@ code' starts a cell; everything until the next marker is its source.
Usage: python -I scripts/build_notebook.py
"""
import glob
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
cells = []
for path in sorted(glob.glob(os.path.join(ROOT, "notebooks", "cells", "*.cells"))):
    kind, buf = None, []

    def flush():
        if kind is None:
            return
        src = "\n".join(buf).strip("\n")
        lines = src.split("\n")
        cell = {"cell_type": "markdown" if kind == "md" else "code", "id": f"cell-{len(cells):03d}", "metadata": {},
                "source": [l + "\n" for l in lines[:-1]] + [lines[-1]]}
        if kind == "code":
            cell.update({"execution_count": None, "outputs": []})
        cells.append(cell)

    for line in open(path, encoding="utf-8").read().split("\n"):
        if line.startswith("#@@ "):
            flush()
            kind, buf = line[4:].strip(), []
        else:
            buf.append(line)
    flush()

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "PySpark", "language": "python", "name": "pysparkkernel"},
        "language_info": {"codemirror_mode": {"name": "python", "version": 3}, "mimetype": "text/x-python",
                          "name": "pyspark", "pygments_lexer": "python3"},
    },
    "nbformat": 4, "nbformat_minor": 5,
}
out = os.path.join(ROOT, "notebooks", "inf2006_pipeline.ipynb")
with open(out, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1)
print(f"wrote {out}: {len(cells)} cells ({sum(c['cell_type']=='code' for c in cells)} code)")
