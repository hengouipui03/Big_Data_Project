"""Build an unexecuted source copy; never overwrite the submitted EMR export."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_submission_zip import source_cells, ROOT


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="notebooks/inf2006_pipeline_source.ipynb")
    args = parser.parse_args()
    output = (ROOT / args.output).resolve()
    if output == (ROOT / "notebooks/inf2006_pipeline.ipynb").resolve():
        raise SystemExit("Refusing to overwrite the executed submission notebook")
    if not output.is_relative_to(ROOT):
        raise SystemExit("Output must be inside the project")
    cells = []
    for kind, source in source_cells():
        cell = {"cell_type": kind, "id": f"source-{len(cells):03d}", "metadata": {}, "source": source.splitlines(keepends=True)}
        if kind == "code":
            cell.update(execution_count=None, outputs=[])
        cells.append(cell)
    nb = {"cells": cells, "metadata": {"kernelspec": {"display_name": "PySpark", "language": "python", "name": "pysparkkernel"}}, "nbformat": 4, "nbformat_minor": 5}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(nb, indent=1) + "\n", encoding="utf8")
    print(f"Wrote {len(cells)} unexecuted cells to {output.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
