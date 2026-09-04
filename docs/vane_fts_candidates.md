# Vane FTS Candidate Contract

This document defines the full-text candidate phase of
[Issue #9](https://github.com/AstroVela/lance-duckdb/issues/9). It applies only
to eligible indexed `lance_fts` queries in the Vane build. Every query outside
the admission boundary below continues to use one `FINAL_SEARCH` assignment.

## Why a shared BM25 scorer is required

An inverted-index segment can compute local top-k efficiently, but BM25 scores
are not segment-local values. Inverse document frequency and document-length
normalization depend on corpus-wide token counts, document counts, and per-term
document frequencies. Independently building a scorer in each worker can change
both scores and the final ranking.

Before executing segment work, every worker opens the complete frozen segment
set and uses Lance's native global BM25 scorer builder. It then injects that one
scorer into the native Lance search for the segments assigned to the worker.
The worker returns at most local k rows as `(_rowid, _score)`. Vane applies the
deterministic global ordering `(_score DESC, _rowid ASC)` and retains k rows.
Because physical segment coverage is disjoint and every worker uses the same
scoring function, the union of segment-local top-k sets contains the global
top-k.

## Admission boundary

FTS candidates admit a query only when all of these conditions hold:

- the source is a direct, replayable Lance dataset rather than a namespace;
- Lance selected an inverted index for the text column;
- at least two physical index segments are present;
- the selected segments have pairwise-disjoint fragment coverage;
- their union covers every fragment in the frozen snapshot;
- the segments use the same tokenizer and scoring configuration;
- no named, pushed-down, outer, computed-score, pre-, or post-filter is present;
- fragment row counts are known and total at least 4096; and
- the frozen state can be reproduced exactly by every worker.

A single segment does not provide parallel work. Partial index coverage is not
combined with a flat FTS scan in this phase because the uncovered documents do
not have the same indexed corpus statistics. Small datasets remain singleton to
avoid scheduling overhead. Namespace-backed FTS and hybrid search also remain
singleton.

Once admitted, each assignment names one frozen physical index UUID. Workers
validate that UUID and its metadata against the coordinator-frozen
`SearchIndexPlan`, derive its fragment set from that plan, and reject unknown,
overlapping, missing, or changed work. There is no execution fallback after
candidate admission.

## Global reduction and materialization

The candidate scan exposes only `_rowid` and `_score`. Vane's native `TopN`
operator performs the global reduction; the extension does not add a custom
shuffle or reducer. The internal table-in/table-out materializer then sends the
winning row IDs to one batched Lance `take_rows` call and appends the preserved
scores. Arrow converters are built from the actual returned batch schema, so
physical LargeUtf8, LargeBinary, LargeList, and dictionary layouts are not
reconstructed from DuckDB logical types.

## Required validation

Integration tests must demonstrate all of the following:

- one full-coverage segment remains `FINAL_SEARCH`;
- a newly appended, uncovered fragment remains `FINAL_SEARCH`;
- two complete disjoint segments produce two `FTS_CANDIDATES` assignments;
- segment-local execution with a corpus-wide scorer equals native Lance FTS;
- a corpus constructed to rank differently with per-segment BM25 still matches
  the native result;
- equal scores are reduced with `_rowid` as the secondary key;
- the final projected columns are materialized correctly from their physical
  Arrow schema;
- prefilters, post-filters, namespace searches, and hybrid searches remain
  singleton; and
- the two assignments execute on two Ray worker nodes.

## Build and protocol constraints

All FTS candidate code is Vane-specific and remains behind
`LANCE_VANE_DISTRIBUTED` and the `vane-distributed` Cargo feature. The ordinary
DuckDB extension keeps its native `lance_fts` implementation and does not
compile this path. This phase does not change the `lance.search-task` protocol
version; version 1 remains the only accepted version.
