These fixtures were created on 2026-11-28 with the existing Trivium CLI built
against Polars 0.40.0, before upgrading the dependency to 0.55.2.

`source.jsonl` is the imported evidence, `evidence.parquet` is its unchanged
project storage, and `expected.json` contains the column metadata and row DTOs
returned by the old CLI. The compatibility test reads the frozen Parquet with
both readers, gathers reordered and duplicate rows, and queries it as a project.
It compares exact original text, row IDs, source locations, large integers,
nanosecond timestamps, and null versus missing cells.

Keep these files frozen. Regenerating them with the current writer would stop
testing compatibility with the old storage.
