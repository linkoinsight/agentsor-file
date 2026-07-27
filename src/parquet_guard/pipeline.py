"""A small, fail-closed and idempotent Parquet batch pipeline."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from decimal import (
    Decimal,
    ROUND_DOWN,
    ROUND_HALF_DOWN,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
    ROUND_UP,
)
from hashlib import sha256
import json
import logging
import os
from pathlib import Path
import shutil
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from .config import PipelineConfig


LOGGER = logging.getLogger("parquet_guard")
TRANSFORM_VERSION = "decimal_amount_scale/v1"
ROUNDING_RULES = {
    "half_even": ROUND_HALF_EVEN,
    "half_up": ROUND_HALF_UP,
    "half_down": ROUND_HALF_DOWN,
    "down": ROUND_DOWN,
    "up": ROUND_UP,
}


@dataclass(frozen=True)
class PipelineSummary:
    """Content-free result counters for one scan."""

    discovered: int = 0
    succeeded: int = 0
    quarantined: int = 0
    skipped: int = 0
    failed: int = 0


def _digest_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _contract_digest(config: PipelineConfig) -> str:
    """Hash every setting that can affect validation or output bytes."""

    contract = {
        "schema": "agentsor.parquet-contract/v1",
        "transform_version": TRANSFORM_VERSION,
        "required_schema": sorted(
            (name, str(pa.type_for_alias(alias)))
            for name, alias in config.required_schema.items()
        ),
        "allow_extra_columns": config.allow_extra_columns,
        "output_compression": config.output_compression,
        "amount_column": config.amount_column,
        "amount_multiplier": format(Decimal(config.amount_multiplier).normalize(), "f"),
        "amount_output_column": config.amount_output_column,
        "amount_rounding": config.amount_rounding,
    }
    payload = json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
    return sha256(payload).hexdigest()


def _identity_digest(source_name: str, source_digest: str, contract_digest: str) -> str:
    payload = json.dumps(
        {
            "source_name": source_name,
            "source_sha256": source_digest,
            "contract_sha256": contract_digest,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return sha256(payload).hexdigest()


def _schema_errors(table: pa.Table, config: PipelineConfig) -> list[str]:
    errors: list[str] = []
    for column, count in sorted(Counter(table.schema.names).items()):
        if count > 1:
            errors.append(f"duplicate:{column}:count={count}")
    actual_names = set(table.schema.names)
    expected_names = set(config.required_schema)
    for column, expected_alias in config.required_schema.items():
        if column not in actual_names:
            errors.append(f"missing:{column}")
            continue
        expected = pa.type_for_alias(expected_alias)
        actual = table.schema.field(column).type
        if actual != expected:
            errors.append(f"type:{column}:expected={expected}:actual={actual}")
    if not config.allow_extra_columns:
        for column in sorted(actual_names - expected_names):
            errors.append(f"unexpected:{column}")
    return errors


def transform(table: pa.Table, config: PipelineConfig) -> pa.Table:
    """Apply one deterministic example transform.

    The public buyer brief does not disclose its business transformations.
    This hook deliberately demonstrates the boundary without guessing them:
    when ``amount_column`` is configured, it appends a configured integer
    column using deterministic decimal multiplication and rounding.
    """

    if config.amount_column is None:
        return table
    amount = table[config.amount_column]
    multiplier = Decimal(config.amount_multiplier)
    rounding = ROUNDING_RULES[config.amount_rounding]
    scaled_values = [
        None
        if value is None
        else int(
            (Decimal(str(value)) * multiplier).quantize(Decimal("1"), rounding=rounding)
        )
        for value in amount.to_pylist()
    ]
    scaled = pa.array(scaled_values, type=pa.int64())
    if config.amount_output_column in table.schema.names:
        return table.set_column(
            table.schema.get_field_index(config.amount_output_column),
            config.amount_output_column,
            scaled,
        )
    return table.append_column(config.amount_output_column, scaled)


def _copy_atomically(source: Path, destination: Path, expected_digest: str) -> None:
    """Copy into the destination filesystem, then publish with one rename."""

    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
    try:
        with source.open("rb") as reader, temporary.open("xb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
            writer.flush()
            os.fsync(writer.fileno())
        if _digest_file(temporary) != expected_digest:
            raise OSError("source changed while it was copied to quarantine")
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _quarantine(
    source: Path,
    *,
    digest: str,
    reason: str,
    details: list[str],
    config: PipelineConfig,
) -> None:
    config.quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine_identity = _identity_digest(source.name, digest, "quarantine/v1")
    destination = (
        config.quarantine_dir / f"quarantine-{quarantine_identity[:24]}.parquet"
    )
    if not source.exists():
        raise FileNotFoundError(source)
    _copy_atomically(source, destination, digest)
    _atomic_json(
        destination.with_suffix(".quarantine.json"),
        {
            "schema": "agentsor.parquet-quarantine/v1",
            "source_name": source.name,
            "sha256": digest,
            "reason": reason,
            "details": details,
        },
    )
    source.unlink()
    _fsync_directory(source.parent)


def _process_one(source: Path, config: PipelineConfig, run_id: str) -> str:
    digest = _digest_file(source)
    contract_digest = _contract_digest(config)
    identity_digest = _identity_digest(source.name, digest, contract_digest)
    state_path = config.state_dir / f"{identity_digest}.json"
    expected_output_name = f"output-{identity_digest[:24]}.parquet"
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            output_name = state["output_name"]
            output_digest = state["output_sha256"]
        except (OSError, json.JSONDecodeError, KeyError, TypeError):
            output_name = None
            output_digest = None
        if isinstance(output_name, str) and isinstance(output_digest, str):
            output_path = config.output_dir / output_name
            try:
                output_matches = (
                    output_name == expected_output_name
                    and Path(output_name).name == output_name
                    and output_path.is_file()
                    and not output_path.is_symlink()
                    and _digest_file(output_path) == output_digest
                    and state.get("schema") == "agentsor.parquet-state/v2"
                    and state.get("contract_sha256") == contract_digest
                    and state.get("source_sha256") == digest
                    and state.get("source_name") == source.name
                )
            except OSError:
                output_matches = False
            if output_matches:
                LOGGER.info(
                    "input_skipped",
                    extra={
                        "event": "input_skipped",
                        "file": source.name,
                        "run_id": run_id,
                    },
                )
                return "skipped"
            LOGGER.warning(
                "state_invalidated",
                extra={
                    "event": "state_invalidated",
                    "file": source.name,
                    "reason": "state_or_output_integrity_mismatch",
                    "run_id": run_id,
                },
            )

    try:
        table = pq.read_table(source)
    except Exception as exc:  # PyArrow raises several format/IO-specific subclasses.
        _quarantine(
            source,
            digest=digest,
            reason="unreadable_parquet",
            details=[type(exc).__name__],
            config=config,
        )
        LOGGER.warning(
            "input_quarantined",
            extra={
                "event": "input_quarantined",
                "file": source.name,
                "reason": "unreadable_parquet",
                "run_id": run_id,
            },
        )
        return "quarantined"

    errors = _schema_errors(table, config)
    if errors:
        _quarantine(
            source,
            digest=digest,
            reason="schema_drift",
            details=errors,
            config=config,
        )
        LOGGER.warning(
            "input_quarantined",
            extra={
                "event": "input_quarantined",
                "file": source.name,
                "reason": "schema_drift",
                "run_id": run_id,
            },
        )
        return "quarantined"

    try:
        transformed = transform(table, config)
    except Exception as exc:
        _quarantine(
            source,
            digest=digest,
            reason="transform_error",
            details=[type(exc).__name__],
            config=config,
        )
        LOGGER.warning(
            "input_quarantined",
            extra={
                "event": "input_quarantined",
                "file": source.name,
                "reason": "transform_error",
                "run_id": run_id,
            },
        )
        return "quarantined"

    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.state_dir.mkdir(parents=True, exist_ok=True)
    if _digest_file(source) != digest:
        raise OSError("source changed while it was being processed")
    output_name = expected_output_name
    output_path = config.output_dir / output_name
    temporary = config.output_dir / f".{output_name}.{uuid4().hex}.tmp"
    compression = (
        None if config.output_compression == "none" else config.output_compression
    )
    try:
        pq.write_table(transformed, temporary, compression=compression)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, output_path)
        _fsync_directory(config.output_dir)
    finally:
        temporary.unlink(missing_ok=True)

    _atomic_json(
        state_path,
        {
            "schema": "agentsor.parquet-state/v2",
            "source_name": source.name,
            "source_sha256": digest,
            "contract_sha256": contract_digest,
            "output_name": output_name,
            "output_sha256": _digest_file(output_path),
            "rows": transformed.num_rows,
        },
    )
    LOGGER.info(
        "input_succeeded",
        extra={
            "event": "input_succeeded",
            "file": source.name,
            "rows": transformed.num_rows,
            "run_id": run_id,
        },
    )
    return "succeeded"


def run_pipeline(config: PipelineConfig) -> PipelineSummary:
    """Process every current input independently and return aggregate counts."""

    config.validate()
    run_id = uuid4().hex
    config.input_dir.mkdir(parents=True, exist_ok=True)
    sources = sorted(
        path
        for path in config.input_dir.iterdir()
        if path.is_file()
        and not path.is_symlink()
        and path.suffix.lower() == ".parquet"
    )
    counts = {"succeeded": 0, "quarantined": 0, "skipped": 0, "failed": 0}
    LOGGER.info(
        "batch_started",
        extra={
            "event": "batch_started",
            "run_id": run_id,
            "discovered": len(sources),
        },
    )
    for source in sources:
        try:
            outcome = _process_one(source, config, run_id)
        except Exception as exc:
            counts["failed"] += 1
            LOGGER.exception(
                "input_failed",
                extra={
                    "event": "input_failed",
                    "file": source.name,
                    "reason": type(exc).__name__,
                    "run_id": run_id,
                },
            )
        else:
            counts[outcome] += 1
    summary = PipelineSummary(discovered=len(sources), **counts)
    LOGGER.info(
        "batch_completed",
        extra={
            "event": "batch_completed",
            "run_id": run_id,
            **summary_dict(summary),
        },
    )
    return summary


def summary_dict(summary: PipelineSummary) -> dict[str, int]:
    """Return a serialization-friendly copy of a summary."""

    return asdict(summary)
