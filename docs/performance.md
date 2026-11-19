# Performance

## Design choices

- **Event-driven wake-ups.** Each schedule compiles to a step function over the
  10 080 minutes of a week. Reconciliation requeues exactly at the next floor
  change (or the next DST transition), bounded by `--resync-interval`. Nothing
  polls schedules.
- **O(log n) evaluation.** The floor at an instant and the next change are two
  binary searches over the compiled breakpoints.
- **No self-triggered reconciles.** The watch is filtered on
  `metadata.generation`, so the controller's own status writes do not trigger
  reconciliations.
- **No redundant writes.** The target is patched only when the floor requires a
  change, and the status only when it differs from the stored one.
- **Cached discovery.** The API resource of each `apiVersion`/kind is resolved
  once per process.
- **Single-threaded runtime.** The controller is I/O bound; a current-thread
  Tokio runtime avoids worker threads and cross-thread synchronisation.
- **Release profile.** Fat LTO, one codegen unit, `panic = "abort"`, stripped.

## Profiling

`cargo build --profile profiling` builds release code with line tables. The
hot paths were profiled with [samply](https://github.com/mstange/samply) on
`tba evaluate` over 10 000 generated objects (`scripts/gen-fixtures.sh`), which
runs the same compile/evaluate code the controller runs per reconciliation.

| Finding | Change | Effect |
|---|---|---|
| 94% of YAML evaluation time is spent in the YAML parser (`serde-saphyr`/`granit-parser`). | JSON object streams (`kubectl get -o json`) are decoded with `serde_json`. | 10 000 objects: 327 ms (YAML) vs 26 ms (JSON). |
| `CompiledSchedule::compile` was 30% of the JSON path: a `BTreeMap` per compile, cloned schedule names, a day-dedup `Vec` per schedule and shrink-reallocations from `into_boxed_slice`. | Floor counts in a small vector, names borrowed from the spec, a day bitmask, pre-sized vectors, unsorted-key event sort. | Compile share 30% → 16%; JSON path 31.7 ms → 26.4 ms. |

Remaining time on the JSON path is `serde` decoding of the spec (≈ 52%),
dominated by strings and the `Vec<Schedule>` growth that JSON cannot pre-size.
Per-object evaluation itself is below 0.5% of samples.

## Benchmarks

`scripts/bench.sh` runs hyperfine (`--shell=none`, warm-up, ≥ 20 runs) and
writes [benchmarks.md](benchmarks.md). Apple M-series, macOS:

| Command | Mean |
|---|---:|
| `tba crd` (process start-up) | 3.3 ms |
| `tba evaluate`, 1 000 objects, YAML | 36.9 ms |
| `tba evaluate`, 10 000 objects, YAML | 327.1 ms |
| `tba evaluate`, 1 000 objects, JSON | 5.9 ms |
| `tba evaluate`, 10 000 objects, JSON | 26.4 ms |

## In-cluster load

`scripts/load.sh 1000` against a single-node kind cluster running Kubernetes
v1.38.0-beta.0 (Docker on an Apple M-series host): 1 000 HPAs, each with a
`TimeBasedAutoscaler`, and `TBA_RESYNC_INTERVAL=5s` to stress the steady state.

| Measure | Result |
|---|---:|
| Apply to all 1 000 objects `Ready` | 6 s |
| Reconciliations in the measurement window | 15 418 (≈ 200/s) |
| Mean reconciliation latency (API round trips included) | 1.3 ms |
| Leader memory (`crictl stats`) | 16.8 MB |
| Leader CPU at ≈ 200 reconciliations/s | 3.2% of one core |
| Standby replica memory | 0.7 MB |

With the default 60 s resync the same 1 000 objects need about 17
reconciliations per second.
