# Vane Indexed Vector Candidate Contract

This document defines the semantic boundary for the indexed-vector phase of
[Issue #9](https://github.com/AstroVela/lance-duckdb/issues/9). Indexed vector
candidate execution is enabled only for the admission boundary below. Every
other indexed vector search continues to use one `FINAL_SEARCH` assignment.

## Why fragment-local index searches are not generally equivalent

For an exact flat search, each fragment has the same complete candidate space.
Taking local k from every disjoint fragment and then taking global k therefore
preserves the dataset-wide result.

An indexed search has an additional candidate-selection stage. In the pinned
Lance release, omitting `nprobs` leaves a minimum of one probe and no fixed
maximum. Lance can probe additional IVF partitions when the first partitions
do not yield k rows after the scan's prefilter, including a fragment
restriction. Independent fragment-local scans can therefore stop after
different partition sets. Their local-k union may contain rows that the single
dataset-wide native search never considered.

The Rust regression
`fragment_local_default_probes_change_indexed_vector_semantics` constructs this
case with two fragments and two fixed IVF centroids. The dataset-wide search
finds k rows in its first partition and stops. One fragment-local search finds
no rows in that partition and adaptively probes the next one, changing the
global result. This test is the executable reason that default-probe indexed
searches must remain singleton.

An explicit positive `nprobs` fixes the partition count. The same regression
checks that fragment-local union matches the dataset-wide result for two fixed
probe counts in the constructed dataset. This is a necessary condition for
distributed indexed candidates, but it is not sufficient by itself.

## Admission boundary

Indexed vector candidates admit a query only when all of these conditions hold:

- `use_index = true` and `nprobs` is explicitly set to a positive value;
- `refine_factor` is absent;
- there is no post-filter, and any filter is applied as a prefilter;
- the coordinator has frozen and validated the exact vector index segment
  identities and their fragment coverage for the query snapshot;
- work assignments are disjoint across selected index segments and uncovered
  flat fragments;
- at least two useful work assignments exist and the existing scheduling
  threshold is met; and
- every admitted worker can execute the assigned native Lance operation
  without reconstructing Lance's index plan in `lance-duckdb`.

Any query outside that boundary remains one `FINAL_SEARCH` assignment. Once a
query is admitted, malformed state, changed index metadata, overlapping work,
or an execution failure must fail closed. It must not silently rerun as a
singleton or a flat search.

The coordinator reuses the existing frozen `SearchIndexPlan`. Covered work is
identified by physical index segment UUID, while fragments outside the
selected segments form separate flat work. Pairwise overlapping physical
segment coverage is not admitted. A worker combines only the tasks assigned to
that worker, passes the selected UUIDs through Lance's native
`with_index_segments` API, and returns at most local k rows as
`(_rowid, _distance)`. Vane then applies the existing deterministic global
ordering `(_distance ASC, _rowid ASC)` and the existing batched Lance take
materializer.

The current Lance scanner can select frozen index segments and restrict
fragments. It does not expose a public hook for injecting one
coordinator-selected set of IVF partitions, so the extension deliberately
keeps partition selection inside Lance and requires an explicit fixed
`nprobs`. The extension does not copy or recreate Lance's filter, deletion, or
index execution pipeline. If disjoint segment work is not available for a
particular index layout, that layout remains singleton.

## Refinement is a separate phase

`refine_factor` expands approximate candidates and reranks them with exact
distances. Applying that operation independently on workers changes where the
candidate cutoff occurs. Distributed refinement therefore needs its own
contract: workers must return a proven sufficient candidate budget, followed
by one global exact rerank before the final k is selected. Until that contract
is implemented, every indexed query with refinement remains singleton.

## Required validation matrix

Integration tests compare the distributed result with the same snapshot's
native Lance result for:

- fixed `nprobs` values below, equal to, and above the available partition
  count;
- one and multiple physical index segments;
- full and partial index coverage, including newly appended fragments;
- deleted rows and stale index entries;
- absent, empty, and selective prefilters;
- equal-distance rows with deterministic `_rowid` tie-breaking;
- fewer than k qualifying rows and empty datasets; and
- index layouts that cannot produce at least two disjoint assignments.

Default probes, post-filters, refinement, missing indexes, unsupported index
types, and any ambiguous coverage must each have a routing test that confirms
they remain `FINAL_SEARCH`.

## Build and protocol constraints

All indexed candidate code is Vane-specific and must remain behind
`LANCE_VANE_DISTRIBUTED` and the `vane-distributed` Cargo feature. The ordinary
DuckDB extension build and native SQL execution path must not depend on it.
This phase does not change the `lance.search-task` protocol version; version 1
remains the only accepted version.
