# INF2006 Team 07 - submission project

This folder contains the completed cloud handover, curated for submission. **The report is still missing: add `report.pdf` before submitting.**

The project uses the supplied price snapshot and 91 GDELT daily archives for 2016 Q1. Hadoop Streaming audits GDELT; Hive registers and transforms the data; PySpark cleans prices, prepares chronological features, trains two Spark ML models and answers three analytical questions.

| Deliverable | Location |
|---|---|
| Actual executed EMR notebook, unchanged | [notebooks/inf2006_pipeline.ipynb](notebooks/inf2006_pipeline.ipynb) |
| Complete saved models | [models/v2026-15th-nov-2016/](models/v2026-15th-nov-2016/) |
| Prepared test data and original predictions for reload | [prepared/v2026-15th-nov-2016/reference/](prepared/v2026-15th-nov-2016/reference/) |
| Numbered setup and rerun guide; Appendix A source | [docs/RUN_ON_EMR.md](docs/RUN_ON_EMR.md) |
| Evidence index and known limitations | [docs/EVIDENCE_INDEX.md](docs/EVIDENCE_INDEX.md) |
| Corpus definitions | [docs/data_dictionary.md](docs/data_dictionary.md) |
| Input profiles and measured performance | [docs/input_profiles.md](docs/input_profiles.md) |
| Final shell execution evidence and notebook transcription | [evidence/final_run/](evidence/final_run/) |
| Figures extracted from this notebook | [report/figures/](report/figures/) |
| Handover provenance and local preparation changes | [docs/HANDOVER_PROVENANCE.md](docs/HANDOVER_PROVENANCE.md) |

The notebook records training application `application_1794811368122_0006`, fresh reload `application_1794811368122_0007`, 34/34 verification checks, zero probability differences within `1e-9`, and zero changed classes. Its execution counts reset at the fresh reload. Original output and source are preserved, including documented limitations and older path labels.

## Prepare the delivery

Use Python 3.10+ locally; the checker and builder require only the standard library. The unchanged notebook ran on EMR's Python 3.9.25.

```bash
python -I scripts/build_submission_zip.py --check # END
```

While the report is missing, explicitly prepare a review folder and ZIP:

```bash
python -I scripts/build_submission_zip.py --prepare # END
```

This writes `submission/` and `submission-review.zip`, labelled **REVIEW ONLY**. After adding `report.pdf`, run the builder without flags to produce `submission.zip`. ZIP paths are relative to its root. A SHA-256 manifest and ZIP integrity verification are included. Review the report, evidence limitations and all required appendices before uploading.

The delivery excludes raw datasets, obsolete runs, scratch files, redundant benchmark copies, full YARN dumps, Git internals and the local recovery archive. `prepared/` contains derived test features and predictions, not raw source datasets. The local `_recovery/` directory preserves the pre-cleanup workspace and must not be submitted.

## Notebook sources

The `.cells` files match the executed notebook's exact sources. `scripts/build_notebook.py` builds a separate unexecuted `inf2006_pipeline_source.ipynb` for development and refuses to overwrite the executed export. Consult the rerun guide before changing notebook resource parameters or reusing a completed output version.

## Report completion

Include the required sections 1-7, a labelled data-flow diagram, results and limitations, references, setup guide, supporting evidence, AI disclosure, and agreed team contribution percentages totalling 100%. Keep main text within 3,000 words and state its word count on the cover. The teaching brief has inconsistent appendix lettering; include all required content under explicit titles. No report or team contribution claims have been fabricated during cleanup.
