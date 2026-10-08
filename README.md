# RunLedger

**A durable local background-job service that survives a worker crash and prevents stale attempts from overwriting results.** Its example workload evaluates recorded retrieval/AI outputs, with an inspection dashboard and a deterministic regression guard.

This project demonstrates backend reliability and evaluation infrastructure. The bundled candidate outputs are **hand-authored demo fixtures**, not evidence of live LLM performance. No model account or API key is needed.

## Run the app

Python 3.10+; runtime and tests use only the standard library.

```bash
make test
python3 -m runledger demo
```

Open **http://127.0.0.1:8765**. The demo starts two workers, submits baseline and regressed examples, and persists the runs. Inspect an attempt history or select the baseline and regressed candidate to compare them. Ctrl-C stops the server and worker processes; successful results remain in `data/runledger.sqlite`.

Start components separately when investigating recovery:

```bash
python3 -m runledger --db data/jobs.sqlite serve
# A separate terminal:
python3 -m runledger --db data/jobs.sqlite worker
# Another terminal:
python3 -m runledger --db data/jobs.sqlite submit examples/baseline.json --key baseline-v1
```

Submitting the same key with the same payload and attempt budget returns the same job. Changing the payload under that key is a conflict, not a silent new run.

## Verified crash demonstration

```bash
python3 tools/recovery_demo.py --output reports/my-recovery-run
```

This submits 40 evaluation jobs, launches a worker that calls `os._exit(73)` immediately after claiming one, and then starts four worker processes. The included measured run completed **40 jobs with 41 attempts, one expired lease and 40 committed results**. Read [the recorded outcome](reports/recovery-20261008/report.json). The 50ms delay per attempt is a deliberate demo workload, not model inference latency.

## Reliability contract

The job states are `queued → running → succeeded`, or `running → retry_wait → running`, ending in `dead` when the attempt budget is exhausted. SQLite WAL stores jobs, attempt history, append-only transition events and results. Claims and transitions use `BEGIN IMMEDIATE`, keeping ownership changes within one write transaction. SQLite supports concurrent readers but one writer; see its [transaction documentation](https://www.sqlite.org/lang_transaction.html).

- **Atomic claim:** concurrent workers cannot both claim the same ready job.
- **Fenced attempts:** every claim receives a new opaque lease token. Complete, heartbeat and fail operations must match the current running attempt and an unexpired lease.
- **Lease renewal:** a worker renews its lease during work. After a crash, another claim pass reaps the expired attempt and schedules a retry with bounded exponential backoff and deterministic jitter.
- **Atomic result:** writing the result and transitioning to succeeded happen in the same transaction. A stale worker or a repeated completion cannot replace it.
- **Durable history:** the attempt timeline records failures and expiry rather than hiding retries. Results include a SHA-256 content fingerprint.

The contract is **at-least-once execution with at most one committed result per job**. It is not exactly-once execution, and it does not make an external email, payment, model request, or object-store write exactly once. An external side effect needs its own idempotency or transaction/outbox design. Lease deadlines use wall-clock time; clock jumps and severe scheduling stalls are operational limits.

This is a single-host service. It is not a replicated scheduler, a multi-node consensus system, or a production deployment. The server binds to loopback, requires JSON submissions, checks browser origin, caps bodies at 1MB, and executes only the defined evaluation workload. It has no user authentication. Those choices are suitable for the local demo; a hosted service would need a separate deployment design.

## Evaluation workload

Each query supplies a question ID, reference relevant document IDs, recorded retrieved IDs, and recorded citation IDs. Results include Hit@3, Recall@3, mean reciprocal rank, invalid citation-ID rate and per-query failures. Optional **observed** model latency is accepted only when the submitter actually measured it; the demo does not invent it. Worker runtime is measured separately.

The benchmark fingerprint includes the suite, corpus IDs, question text and reference labels. It excludes the candidate's output, letting different runs be compared against identical references. Changing the questions, labels, corpus or provenance makes comparison invalid. The regression guard flags an MRR drop greater than .05 or an invalid citation rate increase greater than .01. These are fixed example tolerances, not a statistical significance test.

**A citation ID existing in the corpus does not establish factual grounding or entailment.** The project evaluates a narrow observable property, rather than calling it a complete hallucination detector.

To use your own real evaluation:

1. Copy the schema in `examples/baseline.json`.
2. Set `provenance` to `user_recorded` and record real retrieved/citation IDs against independently chosen labels.
3. Submit baseline and changed outputs using different keys, but the same questions, corpus IDs and reference labels.
4. Inspect per-query failures, not only the average score. Explain the labeling method and preserve a held-out set if you make model-quality claims.

Your existing FilingLens evaluation is a natural first real workload. The demo fixture is not a substitute for running that integration.

## API

| Route | Purpose |
|---|---|
| `POST /api/jobs` | Submit `{"payload": ...}` with `Idempotency-Key`; optional `max_attempts` |
| `GET /api/jobs` | Most recent 100 jobs |
| `GET /api/jobs/{id}` | Result, attempts and transition events |
| `GET /api/metrics` | States, attempts, committed results, lease expirations |
| `GET /api/compare?baseline={id}&candidate={id}` | Compare completed evaluation runs |

## Tests and useful extensions

The 15 tests cover concurrent idempotent submission, exclusive claim, expired-token fencing, renewal, bounded retries/dead letters, committed-result reopening, non-finite input rejection, hand-calculated evaluation scores, changed-reference rejection, an actual subprocess crash, and HTTP behavior.

Before adding more infrastructure, run your own recorded evaluation and have someone else use the app. The next engineering questions are result/artifact storage, database migrations, cancellation semantics, measured contention, worker resource limits, and a real deployment. A PostgreSQL or cloud migration would need new evidence; none is claimed here.
