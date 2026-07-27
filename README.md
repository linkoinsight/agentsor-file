# Parquet Guard reliability demo

This repository is an **owned engineering demonstration**, not client work and
not evidence that it has processed a customer's data. It was built to show a
specific reliability boundary for a filesystem Parquet pipeline:

- discover current `.parquet` inputs;
- validate configured Arrow column names and types before transformation;
- process valid files independently;
- quarantine corrupt or schema-drifted files without stopping the batch;
- externalize paths plus demonstrated transform and compression settings;
- write outputs and state atomically;
- key state by source name, source bytes, and processing contract;
- verify completed output bytes before skipping a rerun; and
- emit timestamped, run-correlated JSON logs plus content-free result counts.

The example transform uses decimal arithmetic to multiply `amount` by the
configured value, applies the configured rounding rule, and writes integer
`amount_cents`. The sample uses `half_even`, so midpoint values round to the
nearest even integer. A real delivery would replace that hook only after the
buyer supplies transformation fixtures and the destination contract. The demo
does not guess customer rules, hold credentials, watch partially written files,
or include a production target adapter.

## Run it

Python 3.11 or newer is required.

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
pytest
python examples/generate_sample.py
parquet-guard --config examples/config.toml
```

Paths in `examples/config.toml` are resolved relative to the configuration
file. The generator creates a deterministic synthetic fixture in `data/inbox`;
successful outputs appear in `data/output`, while invalid inputs and small
reason manifests appear in `data/quarantine`. Successful inputs stay in the
inbox: the same name and bytes are skipped after output-integrity verification,
while byte-identical inputs with different names are distinct work items.

The CLI writes JSON logs to stderr and one summary to stdout. It exits `0` when
the scan has no quarantines or unexpected failures, `1` when the batch completed
with either condition, and `2` for invalid configuration.

## Acceptance evidence

The tests exercise:

1. a valid end-to-end file and deterministic decimal transform;
2. corrupt and schema-drifted files quarantined without stopping good work;
3. a same-name/content rerun skipped only through verified durable state;
4. configuration changes and damaged outputs forcing safe reprocessing;
5. distinct identity for same-content files with different names;
6. cross-filesystem quarantine and unexpected per-file failure isolation;
7. external path and transform configuration; and
8. a clean-clone sample generator plus invalid-config rejection.

## Deliberate production boundary

Before a customer deployment, the contract still needs measured file volume,
an arrival-completion signal, exact input/output fixtures, full schema rules
(including order and nullability if required), transformation rules, one
destination protocol, rerun semantics, retention, observability, deployment
topology, and credential handoff. The demo assumes one process runs at a time.
A production implementation would also add a stable-file/producer handshake,
destination-specific idempotency, a run lock, backpressure, retention, metrics,
and integration tests against the authorized target.

License: MIT.
