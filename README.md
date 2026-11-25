# BatchCall

A lightweight Python library for concurrent batch calls, with independent adaptive concurrency, round-based retries, JSONL persistence and resume support. No runtime dependencies. Python 3.9+.

Designed for LLM APIs, agents, workflows and other I/O-bound calls. Bring your own callable; BatchCall handles scheduling, not business logic.

[中文文档](README_zh.md)

## Installation

From PyPI (once the first release is published):

```bash
python -m pip install batchcall
```

Or from a local checkout:

```bash
python -m pip install .
```

## Quick start

```python
import json
from batchcall import run, StorageConfig

class MyCaller:
    concurrency = 10
    adaptive = True
    max_concurrency = 50

    def __call__(self, input_data: str) -> dict:
        # Replace this example with your thread-safe API call.
        return {"status": "success", "output": input_data}

caller = MyCaller()
data = {str(i): json.dumps({"query": f"Question {i}"}) for i in range(100)}
summary = run(data, caller, storage_config=StorageConfig(output_path="experiment.jsonl"))
print(summary)
```

## Multiple callers

Each object represents an independent rate-limit unit. Callers must be interchangeable for the same workload. Store credentials in your own caller; do not commit secrets.

```python
caller1 = MyCaller()
caller1.concurrency = 10
caller1.max_concurrency = 30
caller2 = MyCaller()
caller2.concurrency = 20
caller2.max_concurrency = 100

from batchcall import LogConfig, RetryConfig

run(data, [caller1, caller2],
    storage_config=StorageConfig(output_path="experiment.jsonl", save_every_n=50),
    retry_config=RetryConfig(max_retries=3),
    log_config=LogConfig(log_path="runtime.log"))
```

### Caller protocol

A callable accepts one JSON string and returns a dict. The library does not parse or validate the business JSON inside the string.

| Field | Required | Meaning |
| --- | --- | --- |
| `status` | Yes | `success`, `rate_limited` (HTTP 429), or any other failure string |
| `output` | Yes | JSON string; may be `None` on failure |
| `duration_s` | No | Finite, nonnegative seconds; measured by BatchCall if omitted |
| `error` | No | Human-readable error or `None` |

Regular exceptions and invalid return contracts become `exception` failures. All failures are retried. The caller must manage API timeouts, credentials, thread safety and classify HTTP 429 as `rate_limited`. Hidden SDK retries are not separately observed by BatchCall.

Optional caller attributes (read at the start of each run):

| Attribute | Default | Meaning |
| --- | --- | --- |
| `name` | `caller_0`, etc. | Unique name for logs and records |
| `concurrency` | 10 | Fixed or initial concurrency |
| `adaptive` | False | Enable adaptive concurrency |
| `max_concurrency` | 50 | Upper bound in adaptive mode |
| `adjust_every_n` | 100 | Completed attempts per adjustment window |

Each caller independently adjusts its concurrency after a full window of completed attempts (including errors and retries). At most 1% rate-limited: increase by 1; over 5%: halve, rounding down; otherwise hold. The minimum is 1. Running calls are not cancelled. Configuration attributes are never modified. RPM is measured over the last 60 seconds of actual callable starts, not actively limited. One callable invocation counts as one attempt regardless of internal HTTP requests.

## Configuration

`run(data, callers, *, storage_config=None, retry_config=None, log_config=None)` accepts one caller or a list/tuple. All configuration objects are optional.

| Class | Parameters (defaults) |
| --- | --- |
| `StorageConfig` | `output_path="results.jsonl"`, `save_every_n=50`, `resume=True` |
| `RetryConfig` | `max_retries=3` (additional rounds, after initial round 0) |
| `LogConfig` | `log_every_n=100`, `console=True`, `log_path=None` |

Logs include total and per-caller RPM, target concurrency, in-flight requests, progress, success/failure counts, 429s, average elapsed and persisted records. Event logs also cover round boundaries, adjustments and tail repair. Console logs go to stderr. No raw input/output or credentials are logged by the framework.

## Persistence and resume

Each completed attempt is appended to JSONL, including failures and retries. A record has this shape:

```json
{"id":"case_001","input":"{\"query\":\"hello\"}","output":{"response":"{\"answer\":\"hi\"}","status":"success","elapsed":0.123},"error_msg":null,"caller_name":"caller_0","round":0}
```

The main thread flushes every `save_every_n` completions and at round boundaries and exit. The initial round runs all pending IDs; later rounds run only failures from the previous round. Each run gets its own retry budget. Caller concurrency state persists across rounds, not across runs.

On startup, BatchCall scans the history line by line, keeping only successful IDs. Any ID that succeeded at least once is skipped when `resume=True`. A valid last record without a newline is preserved and given a newline; an invalid, unterminated last line is truncated. Other invalid lines or structurally invalid records raise an error. `resume=False` still appends and checks the file.

Use a new file or new IDs whenever input, model, or experiment settings change. Do not write to the same result file from multiple processes/runs. Flush is not fsync: a crash can lose unflushed or OS-buffered records. Calls may be repeated after a crash; this is not exactly-once execution. Side-effecting calls should be idempotent. Ordinary interruption flushes collected results where possible; running threads cannot be forcibly stopped, so set caller timeouts. Results can contain sensitive data: choose an appropriate storage location.

```python
from batchcall import load_success_ids, iter_results

success_ids = load_success_ids("experiment.jsonl")
for record in iter_results("experiment.jsonl"):
    if record["output"]["status"] == "success":
        pass  # process a single successful record
```

The readers never modify files; `iter_results` yields every attempt, not deduplicated final results. `load_success_ids` returns an empty set for a missing file. Both reject corrupt records. Memory is bounded by input data, task IDs, active futures, the recent RPM window and the write buffer, not by the full historical result payloads.

`run` returns a dict with `total`, `skipped`, `executed`, `success`, `failed`, `attempts`, `rate_limited`, `elapsed` and `output_path`. Success/failure counts refer only to the current run's pending IDs.

## Development

```bash
python -m pip install -e .
python -m unittest discover -s tests -v
```

MIT license.
