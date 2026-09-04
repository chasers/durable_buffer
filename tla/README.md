# TLA+ models

Formal models of the safety-critical distributed algorithms in
durable_buffer. Each spec models one algorithm. TLC, the TLA+ model checker,
explores every reachable state and reports the first invariant violation.

[`FINDINGS.md`](FINDINGS.md) is the full log: what each spec models, what TLC
found, and the algorithms not modeled yet. Read it before you add or change a
spec.

## How to run

```sh
./tla/run check                     # every config vs expected.tsv (CI's gate)
./tla/run all                       # every config, full TLC output
./tla/run Replication_core          # one config
```

The script downloads `tla2tools.jar` into `tla/` on first use. The jar and the
`tla/states/` scratch directory are git-ignored. The script runs TLC with
`-deadlock` because these specs have valid terminal states — a settled state
is not a bug.

CI runs `./tla/run check` on every push and pull request (the `tla` job in
`.github/workflows/ci.yml`). Each config's PASS/VIOLATED result must match
[`expected.tsv`](expected.tsv). A VIOLATED row there is intentional: a
negative control, or a documented finding on code as it stands today. A new
config must be added to `expected.tsv`, and a fix that changes a result must
update it in the same change.

The whole gate takes about a minute. `Replication_quorum` is most of it.

## Layout

- `<Module>.tla` — the spec.
- `<Module>[_variant].cfg` — one TLC configuration per scenario. The runner
  derives the module from the text before the first `_` in the config name.

## Current matrix

A VIOLATED row is intentional. It either demonstrates a finding or proves a
property is load-bearing.

| config | checks | result |
|---|---|---|
| `Replication_core` | crash + message loss, `fsync: true`, one replica | PASS |
| `Replication_quorum` | same with two replicas and a majority ack policy | PASS |
| `Replication_loss` | dropped batches alone; the tail check heals them | PASS |
| `Replication_nocrash` | truncate + loss, `fsync: false`, no crash | PASS |
| `Replication_adopt` | the adopted-epoch report across a failed truncate `:erpc` | PASS |
| `Replication_heal` | `fsync: false` + crash, with the open-time heal, the attach reference and the reconcile fix | PASS |
| `Replication` | `fsync: false` + primary crash, `AckedInPrimary` | VIOLATED — F-1 |
| `Replication_ahead` | same, `AckedSomewhere`, heal off | VIOLATED — F-2 |
| `Replication_staletruncate` | truncate whose `:erpc` to the replica fails | VIOLATED — F-3 |
| `Replication_dirtyread` | reads gated on the ack policy | VIOLATED — F-4 |
| `Replication_notailcheck` | the writer's tail rule turned off | VIOLATED (control) |
| `Replication_noepoch` | the epoch fence turned off in `reconcile/2` | VIOLATED (control) |

Invariants:

- `NoConflict` — the primary WAL and each replica WAL agree wherever both
  hold a byte.
- `AckedInPrimary` — an acked batch is still readable from the primary. Reads
  are local-only, so this is what a caller actually gets.
- `AckedSomewhere` — an acked batch still exists on some member.
- `ReadsAreAckDurable` — every readable byte met the ack policy.
- `AdoptedIsHonest` — the primary never claims an epoch a replica has not
  persisted.
- `PromotableIsClean` — a replica reported as promotable holds no
  pre-truncate data.

## Conventions

- Keep a spec's header comment pointing at the exact Elixir functions it
  models. When the code moves, update the spec or mark it stale.
- Every claimed property gets a negative config that breaks it. A spec that
  only passes proves little.
- When TLC confirms a bug, replicate it in a real Elixir test before you
  trust a fix.
- Do not add a constant that models a fix TLC has not confirmed. An
  `AttachFence` knob was written and removed for exactly that reason — see
  F-3.
- Log findings and results in `FINDINGS.md`. Track fixes and new specs in
  `.plans/`.
