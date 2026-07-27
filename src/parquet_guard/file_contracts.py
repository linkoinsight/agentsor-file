"""Offline, project-keyed CSV and Parquet contract checks."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .contracts import (
    MAX_REPORTED_BYTES,
    MAX_REPORTED_ROWS,
    FileContract,
)
from .fingerprints import new_project_hmac, project_fingerprint


RESULT_SCHEMA_VERSION = 1
STATE_SCHEMA = "agentsor.file-contract-state/v1"
MAX_STATE_DIGESTS = 4096
_HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_CHECK_NAMES = (
    "readability",
    "schema",
    "rowBounds",
    "byteBounds",
    "eventFreshness",
    "duplicate",
)
_CHECK_STATUSES = {
    "passed",
    "failed",
    "inconclusive",
    "not_configured",
}
_REASON_CODES = {
    "file_missing",
    "file_unreadable",
    "format_invalid",
    "schema_drift",
    "row_count_below_min",
    "row_count_above_max",
    "byte_count_below_min",
    "byte_count_above_max",
    "event_too_old",
    "event_time_missing",
    "duplicate_output",
    "fingerprint_unavailable",
    "check_error",
}


class FileContractInputError(ValueError):
    """Raised when an input cannot be checked safely."""


class DuplicateStateError(ValueError):
    """Raised when duplicate state would be unsafe to update."""


@dataclass(frozen=True)
class ContractResult:
    """The strict result envelope shared with the hosted product."""

    overall: str
    file_format: str
    run_id: str
    started_at: datetime
    finished_at: datetime
    contract_fingerprint: str
    output_fingerprint: str | None
    schema_fingerprint: str | None
    row_count: int | None
    byte_count: int | None
    checks: dict[str, str]
    reason_codes: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        """Return the exact JSON-serializable v1 envelope."""

        return {
            "schemaVersion": RESULT_SCHEMA_VERSION,
            "runId": self.run_id,
            "contractFingerprint": self.contract_fingerprint,
            "startedAt": _timestamp(self.started_at),
            "finishedAt": _timestamp(self.finished_at),
            "overall": self.overall,
            "fileFormat": self.file_format,
            "rowCount": self.row_count,
            "byteCount": self.byte_count,
            "schemaFingerprint": self.schema_fingerprint,
            "outputFingerprint": self.output_fingerprint,
            "checks": {name: self.checks[name] for name in _CHECK_NAMES},
            "reasonCodes": list(self.reason_codes),
        }


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _clock_value(fixed: datetime | None) -> datetime:
    return fixed or datetime.now(timezone.utc)


def _stat_input(path: Path) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise FileContractInputError("file_missing") from exc
    except OSError as exc:
        raise FileContractInputError("file_unreadable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise FileContractInputError("file_unreadable")
    return metadata


def _file_fingerprint(
    path: Path,
    key: bytes,
    expected_metadata: os.stat_result,
) -> str:
    fingerprint = new_project_hmac(key, "output")
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if (
                opened.st_dev != expected_metadata.st_dev
                or opened.st_ino != expected_metadata.st_ino
                or opened.st_size != expected_metadata.st_size
            ):
                raise FileContractInputError("check_error")
            while chunk := handle.read(1024 * 1024):
                fingerprint.update(chunk)
    except FileContractInputError:
        raise
    except OSError as exc:
        raise FileContractInputError("file_unreadable") from exc
    return fingerprint.hexdigest()


def _bound_reason(
    value: int, minimum: int | None, maximum: int | None, kind: str
) -> str | None:
    if minimum is not None and value < minimum:
        return f"{kind}_below_min"
    if maximum is not None and value > maximum:
        return f"{kind}_above_max"
    return None


def _schema_fingerprint(schema: pa.Schema, key: bytes) -> str:
    canonical = [
        {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
        }
        for field in sorted(schema, key=lambda item: item.name)
    ]
    payload = json.dumps(
        {"schema": "agentsor.arrow-schema/v1", "fields": canonical},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return project_fingerprint(key, "schema", payload)


def _read_table(path: Path, file_format: str, contract: FileContract) -> pa.Table:
    if file_format == "parquet":
        return pq.read_table(path)
    return pacsv.read_csv(
        path,
        read_options=pacsv.ReadOptions(use_threads=False),
        parse_options=pacsv.ParseOptions(delimiter=contract.csv_delimiter),
    )


def _inspect_schema(
    table: pa.Table,
    file_format: str,
    contract: FileContract,
) -> tuple[pa.Table, str]:
    counts = Counter(table.schema.names)
    if any(count > 1 for count in counts.values()):
        return table, "failed"
    if any(counts.get(name, 0) != 1 for name in contract.required_schema):
        return table, "failed"

    normalized = table
    types_match = True
    for name, type_name in contract.required_schema.items():
        expected = pa.type_for_alias(type_name)
        index = normalized.schema.get_field_index(name)
        actual = normalized.schema.field(index).type
        if file_format == "parquet":
            if actual != expected:
                types_match = False
            continue
        try:
            converted = pc.cast(normalized.column(index), expected, safe=True)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError):
            types_match = False
        else:
            normalized = normalized.set_column(index, name, converted)
    if not types_match:
        return normalized, "failed"

    expected_names = set(contract.required_schema)
    has_extra = bool(set(table.schema.names) - expected_names)
    if has_extra and not contract.allow_extra_columns:
        return normalized, "failed"
    return normalized, "passed"


def _event_freshness(
    table: pa.Table,
    contract: FileContract,
    checked_at: datetime,
) -> tuple[str, str | None]:
    if contract.event_time_column is None:
        return "not_configured", None
    try:
        latest = pc.max(table.column(contract.event_time_column)).as_py()
    except (KeyError, pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return "failed", "event_time_missing"
    if latest is None or not isinstance(latest, datetime):
        return "failed", "event_time_missing"
    if latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    else:
        latest = latest.astimezone(timezone.utc)
    age = (checked_at - latest).total_seconds()
    if age > contract.max_age_seconds:
        return "failed", "event_too_old"
    return "passed", None


def _load_state(path: Path) -> list[str]:
    if path.is_symlink():
        raise DuplicateStateError("state_not_regular")
    if not path.exists():
        return []
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode):
            raise DuplicateStateError("state_not_regular")
        if metadata.st_size > 1024 * 1024:
            raise DuplicateStateError("state_too_large")
        value = json.loads(path.read_text(encoding="utf-8"))
    except DuplicateStateError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DuplicateStateError("state_unreadable") from exc
    if (
        not isinstance(value, dict)
        or set(value) != {"schema", "fingerprints"}
        or value.get("schema") != STATE_SCHEMA
        or not isinstance(value.get("fingerprints"), list)
    ):
        raise DuplicateStateError("state_invalid")
    fingerprints = value["fingerprints"]
    if (
        len(fingerprints) > MAX_STATE_DIGESTS
        or not all(
            isinstance(item, str) and _HEX_DIGEST.fullmatch(item)
            for item in fingerprints
        )
        or len(set(fingerprints)) != len(fingerprints)
    ):
        raise DuplicateStateError("state_invalid")
    return fingerprints


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_state(path: Path, fingerprints: list[str]) -> None:
    if path.is_symlink():
        raise DuplicateStateError("state_not_regular")
    temporary: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        payload = json.dumps(
            {"schema": STATE_SCHEMA, "fingerprints": fingerprints},
            sort_keys=True,
            separators=(",", ":"),
        )
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(payload + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except OSError as exc:
        raise DuplicateStateError("state_write_failed") from exc
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _duplicate_check(
    *,
    state_path: Path | None,
    source_path: Path,
    output_fingerprint: str | None,
    reject_duplicates: bool,
) -> tuple[str, str | None]:
    if state_path is None:
        if reject_duplicates:
            return "inconclusive", "fingerprint_unavailable"
        return "not_configured", None
    if state_path.resolve(strict=False) == source_path.resolve(strict=False):
        raise DuplicateStateError("state_target_conflict")
    if output_fingerprint is None:
        return "inconclusive", "fingerprint_unavailable"
    try:
        fingerprints = _load_state(state_path)
        if output_fingerprint in fingerprints:
            if reject_duplicates:
                return "failed", "duplicate_output"
            return "passed", None
        updated = [*fingerprints, output_fingerprint][-MAX_STATE_DIGESTS:]
        _atomic_state(state_path, updated)
    except DuplicateStateError:
        return "inconclusive", "check_error"
    return "passed", None


def _overall(checks: dict[str, str]) -> str:
    if any(status == "failed" for status in checks.values()):
        return "failed"
    if any(status == "inconclusive" for status in checks.values()):
        return "inconclusive"
    return "passed"


def _result(
    *,
    file_format: str,
    run_id: str,
    started_at: datetime,
    fixed_now: datetime | None,
    contract_fingerprint: str,
    output_fingerprint: str | None,
    schema_fingerprint: str | None,
    row_count: int | None,
    byte_count: int | None,
    checks: dict[str, str],
    reason_codes: list[str],
) -> ContractResult:
    if set(checks) != set(_CHECK_NAMES):
        raise AssertionError("result checks are not the fixed v1 set")
    if not all(status in _CHECK_STATUSES for status in checks.values()):
        raise AssertionError("result contains an unsupported check status")
    if any(
        status == "not_configured" and name not in {"eventFreshness", "duplicate"}
        for name, status in checks.items()
    ):
        raise AssertionError("required checks cannot be not_configured")
    reasons = tuple(dict.fromkeys(reason_codes))
    if not all(reason in _REASON_CODES for reason in reasons):
        raise AssertionError("result contains an unsupported reason code")
    overall = _overall(checks)
    if (overall == "passed") != (not reasons):
        raise AssertionError("result status and reason codes disagree")
    return ContractResult(
        overall=overall,
        file_format=file_format,
        run_id=run_id,
        started_at=started_at,
        finished_at=_clock_value(fixed_now),
        contract_fingerprint=contract_fingerprint,
        output_fingerprint=output_fingerprint,
        schema_fingerprint=schema_fingerprint,
        row_count=row_count,
        byte_count=byte_count,
        checks=checks,
        reason_codes=reasons,
    )


def _unavailable_result(
    *,
    file_format: str,
    run_id: str,
    started_at: datetime,
    fixed_now: datetime | None,
    contract: FileContract,
    contract_fingerprint: str,
    source_path: Path,
    state_path: Path | None,
    readability_status: str,
    root_reason: str,
) -> ContractResult:
    duplicate_status, duplicate_reason = _duplicate_check(
        state_path=state_path,
        source_path=source_path,
        output_fingerprint=None,
        reject_duplicates=contract.reject_duplicates,
    )
    reasons = [root_reason]
    if duplicate_reason is not None:
        reasons.append(duplicate_reason)
    checks = {
        "readability": readability_status,
        "schema": "inconclusive",
        "rowBounds": "inconclusive",
        "byteBounds": "inconclusive",
        "eventFreshness": (
            "inconclusive"
            if contract.event_time_column is not None
            else "not_configured"
        ),
        "duplicate": duplicate_status,
    }
    return _result(
        file_format=file_format,
        run_id=run_id,
        started_at=started_at,
        fixed_now=fixed_now,
        contract_fingerprint=contract_fingerprint,
        output_fingerprint=None,
        schema_fingerprint=None,
        row_count=None,
        byte_count=None,
        checks=checks,
        reason_codes=reasons,
    )


def check_file(
    path: Path | str,
    contract: FileContract,
    *,
    fingerprint_key: bytes,
    state_path: Path | str | None = None,
    now: datetime | None = None,
) -> ContractResult:
    """Check one local CSV or Parquet file without exposing its identity."""

    contract.validate()
    contract_fingerprint = contract.fingerprint(fingerprint_key)
    source_path = Path(path)
    suffix = source_path.suffix.lower()
    if suffix not in {".csv", ".parquet"}:
        raise FileContractInputError("unsupported_format")
    file_format = suffix.removeprefix(".")
    fixed_now = now
    if fixed_now is not None:
        if fixed_now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        fixed_now = fixed_now.astimezone(timezone.utc)
    started_at = _clock_value(fixed_now)
    run_id = str(uuid4())
    duplicate_path = Path(state_path) if state_path is not None else None

    if contract.file_format is not None and contract.file_format != file_format:
        return _unavailable_result(
            file_format=file_format,
            run_id=run_id,
            started_at=started_at,
            fixed_now=fixed_now,
            contract=contract,
            contract_fingerprint=contract_fingerprint,
            source_path=source_path,
            state_path=duplicate_path,
            readability_status="failed",
            root_reason="format_invalid",
        )

    try:
        metadata = _stat_input(source_path)
    except FileContractInputError as exc:
        return _unavailable_result(
            file_format=file_format,
            run_id=run_id,
            started_at=started_at,
            fixed_now=fixed_now,
            contract=contract,
            contract_fingerprint=contract_fingerprint,
            source_path=source_path,
            state_path=duplicate_path,
            readability_status="failed",
            root_reason=str(exc),
        )

    byte_count = metadata.st_size
    byte_reason = (
        "byte_count_above_max"
        if byte_count > MAX_REPORTED_BYTES
        else _bound_reason(
            byte_count, contract.min_bytes, contract.max_bytes, "byte_count"
        )
    )
    if byte_reason is not None:
        duplicate_status, duplicate_reason = _duplicate_check(
            state_path=duplicate_path,
            source_path=source_path,
            output_fingerprint=None,
            reject_duplicates=contract.reject_duplicates,
        )
        reasons = [byte_reason]
        if duplicate_reason is not None:
            reasons.append(duplicate_reason)
        return _result(
            file_format=file_format,
            run_id=run_id,
            started_at=started_at,
            fixed_now=fixed_now,
            contract_fingerprint=contract_fingerprint,
            output_fingerprint=None,
            schema_fingerprint=None,
            row_count=None,
            byte_count=(byte_count if byte_count <= MAX_REPORTED_BYTES else None),
            checks={
                "readability": "inconclusive",
                "schema": "inconclusive",
                "rowBounds": "inconclusive",
                "byteBounds": "failed",
                "eventFreshness": (
                    "inconclusive"
                    if contract.event_time_column is not None
                    else "not_configured"
                ),
                "duplicate": duplicate_status,
            },
            reason_codes=reasons,
        )

    try:
        output_fingerprint = _file_fingerprint(source_path, fingerprint_key, metadata)
    except FileContractInputError as exc:
        return _unavailable_result(
            file_format=file_format,
            run_id=run_id,
            started_at=started_at,
            fixed_now=fixed_now,
            contract=contract,
            contract_fingerprint=contract_fingerprint,
            source_path=source_path,
            state_path=duplicate_path,
            readability_status=(
                "inconclusive" if str(exc) == "check_error" else "failed"
            ),
            root_reason=str(exc),
        )

    try:
        table = _read_table(source_path, file_format, contract)
    except Exception:
        duplicate_status, duplicate_reason = _duplicate_check(
            state_path=duplicate_path,
            source_path=source_path,
            output_fingerprint=output_fingerprint,
            reject_duplicates=contract.reject_duplicates,
        )
        reasons = ["format_invalid"]
        if duplicate_reason is not None:
            reasons.append(duplicate_reason)
        return _result(
            file_format=file_format,
            run_id=run_id,
            started_at=started_at,
            fixed_now=fixed_now,
            contract_fingerprint=contract_fingerprint,
            output_fingerprint=output_fingerprint,
            schema_fingerprint=None,
            row_count=None,
            byte_count=byte_count,
            checks={
                "readability": "failed",
                "schema": "inconclusive",
                "rowBounds": "inconclusive",
                "byteBounds": "passed",
                "eventFreshness": (
                    "inconclusive"
                    if contract.event_time_column is not None
                    else "not_configured"
                ),
                "duplicate": duplicate_status,
            },
            reason_codes=reasons,
        )

    try:
        second_metadata = _stat_input(source_path)
        second_fingerprint = _file_fingerprint(
            source_path, fingerprint_key, second_metadata
        )
    except FileContractInputError:
        second_metadata = None
        second_fingerprint = None
    if (
        second_metadata is None
        or second_metadata.st_dev != metadata.st_dev
        or second_metadata.st_ino != metadata.st_ino
        or second_metadata.st_size != metadata.st_size
        or second_fingerprint != output_fingerprint
    ):
        return _unavailable_result(
            file_format=file_format,
            run_id=run_id,
            started_at=started_at,
            fixed_now=fixed_now,
            contract=contract,
            contract_fingerprint=contract_fingerprint,
            source_path=source_path,
            state_path=None,
            readability_status="inconclusive",
            root_reason="check_error",
        )

    normalized, schema_status = _inspect_schema(table, file_format, contract)
    reasons: list[str] = []
    if schema_status == "failed":
        reasons.append("schema_drift")

    actual_rows = table.num_rows
    row_reason = (
        "row_count_above_max"
        if actual_rows > MAX_REPORTED_ROWS
        else _bound_reason(
            actual_rows, contract.min_rows, contract.max_rows, "row_count"
        )
    )
    row_status = "failed" if row_reason is not None else "passed"
    if row_reason is not None:
        reasons.append(row_reason)

    if schema_status == "passed":
        event_status, event_reason = _event_freshness(
            normalized, contract, _clock_value(fixed_now)
        )
    elif contract.event_time_column is None:
        event_status, event_reason = "not_configured", None
    else:
        event_status, event_reason = "inconclusive", None
    if event_reason is not None:
        reasons.append(event_reason)

    duplicate_status, duplicate_reason = _duplicate_check(
        state_path=duplicate_path,
        source_path=source_path,
        output_fingerprint=output_fingerprint,
        reject_duplicates=contract.reject_duplicates,
    )
    if duplicate_reason is not None:
        reasons.append(duplicate_reason)
    return _result(
        file_format=file_format,
        run_id=run_id,
        started_at=started_at,
        fixed_now=fixed_now,
        contract_fingerprint=contract_fingerprint,
        output_fingerprint=output_fingerprint,
        schema_fingerprint=_schema_fingerprint(normalized.schema, fingerprint_key),
        row_count=(actual_rows if actual_rows <= MAX_REPORTED_ROWS else None),
        byte_count=byte_count,
        checks={
            "readability": "passed",
            "schema": schema_status,
            "rowBounds": row_status,
            "byteBounds": "passed",
            "eventFreshness": event_status,
            "duplicate": duplicate_status,
        },
        reason_codes=reasons,
    )
