from __future__ import annotations

from dataclasses import replace
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from parquet_guard import PipelineConfig, load_config, run_pipeline
import parquet_guard.pipeline as pipeline_module
from parquet_guard.cli import JsonFormatter


def config_for(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        input_dir=tmp_path / "inbox",
        output_dir=tmp_path / "output",
        quarantine_dir=tmp_path / "quarantine",
        state_dir=tmp_path / "state",
        required_schema={
            "event_id": "int64",
            "amount": "double",
            "category": "string",
        },
        amount_column="amount",
    ).validate()


def valid_table() -> pa.Table:
    return pa.table(
        {
            "event_id": pa.array([1, 2], type=pa.int64()),
            "amount": pa.array([10.25, 2.34], type=pa.float64()),
            "category": pa.array(["alpha", "beta"], type=pa.string()),
        }
    )


def write_input(config: PipelineConfig, name: str, table: pa.Table) -> Path:
    config.input_dir.mkdir(parents=True, exist_ok=True)
    target = config.input_dir / name
    pq.write_table(table, target)
    return target


def test_happy_path_writes_atomic_transformed_output(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    write_input(config, "batch.parquet", valid_table())

    summary = run_pipeline(config)

    assert summary.discovered == 1
    assert summary.succeeded == 1
    assert summary.quarantined == 0
    outputs = list(config.output_dir.glob("*.parquet"))
    assert len(outputs) == 1
    result = pq.read_table(outputs[0])
    assert result.column("amount_cents").to_pylist() == [1025, 234]
    assert not list(config.output_dir.glob("*.tmp"))


def test_corrupt_input_is_quarantined_without_stopping_batch(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    config.input_dir.mkdir(parents=True)
    (config.input_dir / "broken.parquet").write_bytes(b"not parquet")
    write_input(config, "good.parquet", valid_table())

    summary = run_pipeline(config)

    assert summary.discovered == 2
    assert summary.succeeded == 1
    assert summary.quarantined == 1
    manifests = list(config.quarantine_dir.glob("*.quarantine.json"))
    assert len(manifests) == 1
    manifest = json.loads(manifests[0].read_text())
    assert manifest["reason"] == "unreadable_parquet"
    assert "not parquet" not in manifests[0].read_text()


def test_schema_drift_is_quarantined_with_machine_readable_reason(
    tmp_path: Path,
) -> None:
    config = config_for(tmp_path)
    drifted = pa.table(
        {
            "event_id": pa.array([1], type=pa.int64()),
            "amount": pa.array(["10.25"], type=pa.string()),
            "category": pa.array(["alpha"], type=pa.string()),
        }
    )
    write_input(config, "drift.parquet", drifted)

    summary = run_pipeline(config)

    assert summary.quarantined == 1
    manifest_path = next(config.quarantine_dir.glob("*.quarantine.json"))
    manifest = json.loads(manifest_path.read_text())
    assert manifest["reason"] == "schema_drift"
    assert any(item.startswith("type:amount:") for item in manifest["details"])


def test_rerun_is_idempotent_when_output_and_state_exist(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    write_input(config, "batch.parquet", valid_table())

    first = run_pipeline(config)
    output = next(config.output_dir.glob("*.parquet"))
    initial_bytes = output.read_bytes()
    second = run_pipeline(config)

    assert first.succeeded == 1
    assert second.skipped == 1
    assert second.succeeded == 0
    assert output.read_bytes() == initial_bytes
    assert len(list(config.output_dir.glob("*.parquet"))) == 1


def test_changed_processing_contract_does_not_serve_stale_output(
    tmp_path: Path,
) -> None:
    config = config_for(tmp_path)
    write_input(config, "batch.parquet", valid_table())
    assert run_pipeline(config).succeeded == 1

    changed = replace(config, amount_multiplier="10").validate()
    second = run_pipeline(changed)

    assert second.succeeded == 1
    assert second.skipped == 0
    outputs = sorted(config.output_dir.glob("*.parquet"))
    assert len(outputs) == 2
    transformed_values = {
        tuple(pq.read_table(output)["amount_cents"].to_pylist()) for output in outputs
    }
    assert transformed_values == {(1025, 234), (102, 23)}


def test_damaged_output_is_rebuilt_instead_of_skipped(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    write_input(config, "batch.parquet", valid_table())
    assert run_pipeline(config).succeeded == 1
    output = next(config.output_dir.glob("*.parquet"))
    output.write_bytes(b"truncated")

    second = run_pipeline(config)

    assert second.succeeded == 1
    assert second.skipped == 0
    assert pq.read_table(output)["amount_cents"].to_pylist() == [1025, 234]


def test_state_cannot_redirect_output_outside_output_directory(
    tmp_path: Path,
) -> None:
    config = config_for(tmp_path)
    write_input(config, "batch.parquet", valid_table())
    assert run_pipeline(config).succeeded == 1
    state_path = next(config.state_dir.glob("*.json"))
    state = json.loads(state_path.read_text())
    state["output_name"] = "../redirected.parquet"
    state_path.write_text(json.dumps(state), encoding="utf-8")

    second = run_pipeline(config)

    assert second.succeeded == 1
    assert second.skipped == 0
    assert not (config.output_dir.parent / "redirected.parquet").exists()


def test_symlink_output_is_replaced_instead_of_trusted(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    write_input(config, "batch.parquet", valid_table())
    assert run_pipeline(config).succeeded == 1
    output = next(config.output_dir.glob("*.parquet"))
    external = tmp_path / "external.parquet"
    external.write_bytes(output.read_bytes())
    output.unlink()
    output.symlink_to(external)

    second = run_pipeline(config)

    assert second.succeeded == 1
    assert second.skipped == 0
    assert output.is_file()
    assert not output.is_symlink()
    assert external.is_file()


def test_identical_bytes_with_different_names_have_distinct_identity(
    tmp_path: Path,
) -> None:
    config = config_for(tmp_path)
    first = write_input(config, "first.parquet", valid_table())
    config.input_dir.mkdir(parents=True, exist_ok=True)
    second = config.input_dir / "second.parquet"
    second.write_bytes(first.read_bytes())

    summary = run_pipeline(config)

    assert summary.succeeded == 2
    outputs = sorted(config.output_dir.glob("*.parquet"))
    assert len(outputs) == 2
    assert outputs[0].name != outputs[1].name
    assert len(list(config.state_dir.glob("*.json"))) == 2


def test_cross_filesystem_quarantine_does_not_abort_batch(tmp_path: Path) -> None:
    shared_memory = Path("/dev/shm")
    if not shared_memory.is_dir() or not os.access(shared_memory, os.W_OK):
        pytest.skip("/dev/shm is not writable")
    if tmp_path.stat().st_dev == shared_memory.stat().st_dev:
        pytest.skip("no second writable filesystem is available")

    with tempfile.TemporaryDirectory(
        prefix="parquet-guard-test-", dir=shared_memory
    ) as quarantine:
        base = config_for(tmp_path)
        config = replace(base, quarantine_dir=Path(quarantine)).validate()
        config.input_dir.mkdir(parents=True)
        broken = config.input_dir / "broken.parquet"
        broken.write_bytes(b"not parquet")
        write_input(config, "good.parquet", valid_table())

        summary = run_pipeline(config)

        assert summary.quarantined == 1
        assert summary.succeeded == 1
        assert summary.failed == 0
        assert not broken.exists()
        assert len(list(config.quarantine_dir.glob("*.parquet"))) == 1


def test_unexpected_input_failure_does_not_stop_remaining_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = config_for(tmp_path)
    config.input_dir.mkdir(parents=True)
    bad = config.input_dir / "bad.parquet"
    bad.write_bytes(b"not parquet")
    write_input(config, "good.parquet", valid_table())
    original = pipeline_module._copy_atomically

    def copy_with_one_failure(
        source: Path, destination: Path, expected_digest: str
    ) -> None:
        if source.name == "bad.parquet":
            raise OSError("simulated per-file failure")
        original(source, destination, expected_digest)

    monkeypatch.setattr(pipeline_module, "_copy_atomically", copy_with_one_failure)

    summary = run_pipeline(config)

    assert summary.failed == 1
    assert summary.succeeded == 1
    assert bad.exists()


def test_external_toml_paths_are_relative_to_config(tmp_path: Path) -> None:
    config_path = tmp_path / "config" / "pipeline.toml"
    config_path.parent.mkdir()
    config_path.write_text(
        """
[pipeline]
input_dir = "../inbox"
output_dir = "../output"
quarantine_dir = "../quarantine"
state_dir = "../state"
amount_column = "amount"
amount_multiplier = "10"
amount_output_column = "scaled_amount"
amount_rounding = "half_up"

[schema]
event_id = "int64"
amount = "double"
category = "string"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.input_dir == (tmp_path / "inbox").resolve()
    assert config.output_dir == (tmp_path / "output").resolve()
    assert config.amount_multiplier == "10"
    assert config.amount_output_column == "scaled_amount"
    assert config.amount_rounding == "half_up"


def test_invalid_transform_configuration_fails_before_processing(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="floating-point"):
        PipelineConfig(
            input_dir=tmp_path / "inbox",
            output_dir=tmp_path / "output",
            quarantine_dir=tmp_path / "quarantine",
            state_dir=tmp_path / "state",
            required_schema={"amount": "string"},
            amount_column="amount",
        ).validate()


def test_decimal_rounding_rule_is_explicit_and_deterministic(tmp_path: Path) -> None:
    config = config_for(tmp_path)
    table = pa.table(
        {
            "event_id": pa.array([1, 2], type=pa.int64()),
            "amount": pa.array([1.005, 1.015], type=pa.float64()),
            "category": pa.array(["alpha", "beta"], type=pa.string()),
        }
    )
    write_input(config, "rounding.parquet", table)

    assert run_pipeline(config).succeeded == 1
    output = next(config.output_dir.glob("*.parquet"))
    assert pq.read_table(output)["amount_cents"].to_pylist() == [100, 102]


def test_sample_generator_creates_a_processable_input(tmp_path: Path) -> None:
    output = tmp_path / "sample.parquet"
    script = Path(__file__).parents[1] / "examples" / "generate_sample.py"

    subprocess.run(
        [sys.executable, str(script), "--output", str(output)],
        check=True,
        capture_output=True,
        text=True,
    )

    assert output.is_file()
    assert pq.read_table(output).schema.names == ["event_id", "amount", "category"]


def test_logs_are_timestamped_and_correlated_per_run(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config = config_for(tmp_path)
    write_input(config, "batch.parquet", valid_table())

    with caplog.at_level(logging.INFO, logger="parquet_guard"):
        run_pipeline(config)

    records = [record for record in caplog.records if hasattr(record, "run_id")]
    assert {record.run_id for record in records} == {records[0].run_id}
    assert {record.event for record in records} >= {
        "batch_started",
        "input_succeeded",
        "batch_completed",
    }
    rendered = json.loads(JsonFormatter().format(records[0]))
    assert rendered["run_id"] == records[0].run_id
    assert rendered["timestamp"].endswith("+00:00")
