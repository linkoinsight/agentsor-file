"""Narrow, secret-safe reporting to the Agentsor hosted collector.

The reporter intentionally has no configurable destination, proxy support,
redirect support, or token-from-environment fallback.  A caller can submit only
the fixed aggregate result envelope produced by the local verifier.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.client import HTTPException
import json
import os
from pathlib import Path
import re
import ssl
import stat
import time
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import (
    HTTPRedirectHandler,
    HTTPSHandler,
    ProxyHandler,
    Request,
    build_opener,
)
import uuid


HOSTED_FILE_RUN_ENDPOINT = "https://agentsor.ai/api/v1/file-runs"
MAX_REQUEST_BYTES = 8_192
MAX_RESPONSE_BYTES = 8_192
DEFAULT_TIMEOUT_SECONDS = 15
DEFAULT_RETRY_DELAY_SECONDS = 1
MAX_RETRY_AFTER_SECONDS = 60
_RETRYABLE_HTTP_STATUSES = frozenset({408, 500, 502, 503, 504, *range(520, 528)})
_INGEST_TOKEN_RE = re.compile(r"^fr1_[A-Za-z0-9_-]{43}$")
_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")
_UTC_TIMESTAMP_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T"
    r"[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z$"
)
_ENVELOPE_FIELDS = frozenset(
    {
        "schemaVersion",
        "runId",
        "contractFingerprint",
        "startedAt",
        "finishedAt",
        "overall",
        "fileFormat",
        "rowCount",
        "byteCount",
        "schemaFingerprint",
        "outputFingerprint",
        "checks",
        "reasonCodes",
    }
)
_RECEIPT_FIELDS = frozenset(
    {
        "accepted",
        "duplicate",
        "runId",
        "deadlineState",
        "nextExpectedAt",
        "deadlineAt",
    }
)
_CHECK_FIELDS = (
    "readability",
    "schema",
    "rowBounds",
    "byteBounds",
    "eventFreshness",
    "duplicate",
)
_RESULTS = frozenset({"passed", "failed", "inconclusive"})
_CHECK_RESULTS = _RESULTS | {"not_configured"}
_REASON_CODES = frozenset(
    {
        "byte_count_above_max",
        "byte_count_below_min",
        "check_error",
        "duplicate_output",
        "event_time_missing",
        "event_too_old",
        "file_missing",
        "file_unreadable",
        "fingerprint_unavailable",
        "format_invalid",
        "row_count_above_max",
        "row_count_below_min",
        "schema_drift",
    }
)
_FAILED_CHECK_REASONS = {
    "readability": frozenset({"file_missing", "file_unreadable", "format_invalid"}),
    "schema": frozenset({"schema_drift"}),
    "rowBounds": frozenset({"row_count_above_max", "row_count_below_min"}),
    "byteBounds": frozenset({"byte_count_above_max", "byte_count_below_min"}),
    "eventFreshness": frozenset({"event_time_missing", "event_too_old"}),
    "duplicate": frozenset({"duplicate_output"}),
}
_MAX_ROW_COUNT = 1_000_000_000_000
_MAX_BYTE_COUNT = 1_099_511_627_776
_MAX_REASON_CODES = 8
_MAX_CLOCK_SKEW = timedelta(minutes=5)
_MAX_RUN_AGE = timedelta(days=7)
_MAX_RUN_DURATION = timedelta(days=7)


class HostedReportError(RuntimeError):
    """A fixed-code reporting failure that never contains a secret or body."""

    def __init__(self, code: str) -> None:
        if code not in {
            "contract_mismatch",
            "idempotency_conflict",
            "invalid_envelope",
            "invalid_ingest_token",
            "invalid_token_file",
            "invalid_receipt",
            "rate_limited",
            "report_rejected",
            "temporarily_unavailable",
            "transport_error",
        }:
            raise ValueError("unsupported hosted report error")
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class HostedReceipt:
    """Validated, content-free acknowledgement from the hosted collector."""

    run_id: uuid.UUID
    duplicate: bool
    deadline_state: str
    next_expected_at: datetime
    deadline_at: datetime


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Request,
        file_pointer: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> None:
        return None


def _build_opener() -> Any:
    """Build a TLS-verifying opener that ignores ambient proxy settings."""

    return build_opener(
        ProxyHandler({}),
        HTTPSHandler(context=ssl.create_default_context()),
        _RejectRedirects(),
    )


def _read_ingest_token(path: Path) -> str:
    """Read one owner-only regular file without following a symlink."""

    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        if not hasattr(os, "O_NOFOLLOW") and path.is_symlink():
            raise OSError("symlink token file")
        descriptor = os.open(path, flags)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size > 128
            or metadata.st_mode & 0o077
        ):
            raise OSError("unsafe token file")
        if hasattr(os, "geteuid") and metadata.st_uid != os.geteuid():
            raise OSError("token file has a different owner")
        raw = os.read(descriptor, 129)
    except OSError as exc:
        raise HostedReportError("invalid_token_file") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)

    try:
        token = raw.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise HostedReportError("invalid_token_file") from exc
    if _INGEST_TOKEN_RE.fullmatch(token) is None:
        raise HostedReportError("invalid_token_file")
    return token


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _canonical_run_id(value: Any) -> uuid.UUID:
    if not isinstance(value, str):
        raise ValueError("invalid run identifier")
    parsed = uuid.UUID(value)
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError("non-canonical run identifier")
    return parsed


def _utc_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or _UTC_TIMESTAMP_RE.fullmatch(value) is None:
        raise ValueError("invalid timestamp")
    return datetime.fromisoformat(value[:-1] + "+00:00")


def _optional_count(value: Any, *, maximum: int) -> int | None:
    if value is None:
        return None
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError("invalid aggregate count")
    return value


def _optional_fingerprint(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _FINGERPRINT_RE.fullmatch(value) is None:
        raise ValueError("invalid fingerprint")
    return value


def _validate_envelope(envelope: Mapping[str, object]) -> uuid.UUID:
    """Validate the full fixed schema before any bytes leave the process."""

    if not isinstance(envelope, dict) or set(envelope) != _ENVELOPE_FIELDS:
        raise ValueError("invalid envelope fields")
    if type(envelope["schemaVersion"]) is not int or envelope["schemaVersion"] != 1:
        raise ValueError("invalid schema version")
    run_id = _canonical_run_id(envelope["runId"])
    contract_fingerprint = envelope["contractFingerprint"]
    if (
        not isinstance(contract_fingerprint, str)
        or _FINGERPRINT_RE.fullmatch(contract_fingerprint) is None
    ):
        raise ValueError("invalid contract fingerprint")

    started_at = _utc_timestamp(envelope["startedAt"])
    finished_at = _utc_timestamp(envelope["finishedAt"])
    current = datetime.now(timezone.utc)
    if (
        started_at > finished_at
        or finished_at - started_at > _MAX_RUN_DURATION
        or finished_at > current + _MAX_CLOCK_SKEW
        or finished_at < current - _MAX_RUN_AGE
    ):
        raise ValueError("invalid run timestamps")

    overall = envelope["overall"]
    file_format = envelope["fileFormat"]
    if not isinstance(overall, str) or overall not in _RESULTS:
        raise ValueError("invalid overall result")
    if file_format not in {"csv", "parquet"}:
        raise ValueError("invalid file format")
    row_count = _optional_count(envelope["rowCount"], maximum=_MAX_ROW_COUNT)
    byte_count = _optional_count(envelope["byteCount"], maximum=_MAX_BYTE_COUNT)
    schema_fingerprint = _optional_fingerprint(envelope["schemaFingerprint"])
    output_fingerprint = _optional_fingerprint(envelope["outputFingerprint"])

    checks = envelope["checks"]
    if not isinstance(checks, dict) or set(checks) != set(_CHECK_FIELDS):
        raise ValueError("invalid check fields")
    if any(
        not isinstance(checks[field], str) or checks[field] not in _CHECK_RESULTS
        for field in _CHECK_FIELDS
    ):
        raise ValueError("invalid check result")
    if any(
        checks[field] == "not_configured"
        for field in ("readability", "schema", "rowBounds", "byteBounds")
    ):
        raise ValueError("required check is not configured")

    reasons = envelope["reasonCodes"]
    if (
        not isinstance(reasons, list)
        or len(reasons) > _MAX_REASON_CODES
        or len(set(reasons)) != len(reasons)
        or any(
            not isinstance(reason, str) or reason not in _REASON_CODES
            for reason in reasons
        )
    ):
        raise ValueError("invalid reason codes")
    check_values = tuple(checks[field] for field in _CHECK_FIELDS)
    if overall == "passed" and (
        any(value not in {"passed", "not_configured"} for value in check_values)
        or reasons
    ):
        raise ValueError("passed result is contradictory")
    if overall == "failed" and ("failed" not in check_values or not reasons):
        raise ValueError("failed result lacks evidence")
    if overall == "inconclusive" and (
        "failed" in check_values or "inconclusive" not in check_values or not reasons
    ):
        raise ValueError("inconclusive result is contradictory")
    if checks["rowBounds"] == "passed" and row_count is None:
        raise ValueError("row bounds passed without a count")
    if checks["byteBounds"] == "passed" and byte_count is None:
        raise ValueError("byte bounds passed without a count")
    if checks["schema"] == "passed" and schema_fingerprint is None:
        raise ValueError("schema passed without a fingerprint")
    if checks["duplicate"] in {"passed", "failed"} and output_fingerprint is None:
        raise ValueError("duplicate result lacks a fingerprint")
    if checks["readability"] == "passed" and (
        row_count is None
        or byte_count is None
        or schema_fingerprint is None
        or output_fingerprint is None
    ):
        raise ValueError("readability passed without measurements")
    reason_checks = {
        "file_missing": ("readability", "failed"),
        "file_unreadable": ("readability", "failed"),
        "format_invalid": ("readability", "failed"),
        "schema_drift": ("schema", "failed"),
        "row_count_below_min": ("rowBounds", "failed"),
        "row_count_above_max": ("rowBounds", "failed"),
        "byte_count_below_min": ("byteBounds", "failed"),
        "byte_count_above_max": ("byteBounds", "failed"),
        "event_time_missing": ("eventFreshness", "failed"),
        "event_too_old": ("eventFreshness", "failed"),
        "duplicate_output": ("duplicate", "failed"),
        "fingerprint_unavailable": ("duplicate", "inconclusive"),
    }
    if any(
        checks[field] != required_status
        for reason, (field, required_status) in reason_checks.items()
        if reason in reasons
    ):
        raise ValueError("reason code contradicts its check")
    if "check_error" in reasons and not (
        checks["readability"] == "inconclusive" or checks["duplicate"] == "inconclusive"
    ):
        raise ValueError("check error lacks an inconclusive check")
    reason_set = frozenset(reasons)
    if any(
        checks[field] == "failed" and reason_set.isdisjoint(compatible_reasons)
        for field, compatible_reasons in _FAILED_CHECK_REASONS.items()
    ):
        raise ValueError("failed check lacks matching evidence")
    return run_id


def _encode_envelope(envelope: Mapping[str, object]) -> tuple[bytes, uuid.UUID]:
    try:
        run_id = _validate_envelope(envelope)
        body = json.dumps(
            dict(envelope),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise HostedReportError("invalid_envelope") from exc
    if not body or len(body) > MAX_REQUEST_BYTES:
        raise HostedReportError("invalid_envelope")
    return body, run_id


def _decode_receipt(raw: bytes, expected_run_id: uuid.UUID) -> HostedReceipt:
    if not raw or len(raw) > MAX_RESPONSE_BYTES:
        raise HostedReportError("invalid_receipt")
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("non-finite JSON")
            ),
        )
        if not isinstance(value, dict) or set(value) != _RECEIPT_FIELDS:
            raise ValueError("invalid receipt fields")
        run_id = _canonical_run_id(value["runId"])
        if (
            value["accepted"] is not True
            or type(value["duplicate"]) is not bool
            or run_id != expected_run_id
            or value["deadlineState"] not in {"on_time", "overdue"}
        ):
            raise ValueError("invalid receipt values")
        next_expected_at = _utc_timestamp(value["nextExpectedAt"])
        deadline_at = _utc_timestamp(value["deadlineAt"])
        if deadline_at < next_expected_at:
            raise ValueError("invalid receipt deadline")
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise HostedReportError("invalid_receipt") from exc
    return HostedReceipt(
        run_id=run_id,
        duplicate=value["duplicate"],
        deadline_state=value["deadlineState"],
        next_expected_at=next_expected_at,
        deadline_at=deadline_at,
    )


def _http_error_code(error: HTTPError) -> str:
    if error.code in {401, 404}:
        return "invalid_ingest_token"
    if error.code == 429:
        return "rate_limited"
    if error.code in _RETRYABLE_HTTP_STATUSES:
        return "temporarily_unavailable"
    if error.code == 409:
        try:
            raw = error.read(MAX_RESPONSE_BYTES + 1)
            value = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_keys,
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return "report_rejected"
        if (
            isinstance(value, dict)
            and set(value) == {"detail"}
            and value["detail"]
            in {
                "contract_mismatch",
                "idempotency_conflict",
            }
        ):
            return value["detail"]
    return "report_rejected"


def _retry_delay_seconds(error: HTTPError | None = None) -> int | None:
    if error is None:
        return DEFAULT_RETRY_DELAY_SECONDS
    retry_after = error.headers.get("Retry-After") if error.headers else None
    if retry_after is None:
        return DEFAULT_RETRY_DELAY_SECONDS
    if (
        not isinstance(retry_after, str)
        or re.fullmatch(r"[0-9]{1,3}", retry_after) is None
    ):
        return None
    delay = int(retry_after)
    return delay if 0 <= delay <= MAX_RETRY_AFTER_SECONDS else None


def post_run_envelope(
    envelope: Mapping[str, object],
    *,
    token_file: Path,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> HostedReceipt:
    """Post one result, replaying its exact bytes once after ambiguity."""

    if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
        raise HostedReportError("transport_error")
    body, run_id = _encode_envelope(envelope)
    token = _read_ingest_token(token_file)
    for attempt in range(2):
        request = Request(
            HOSTED_FILE_RUN_ENDPOINT,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "agentsor-file/0.2",
                "Connection": "close",
            },
        )
        try:
            with _build_opener().open(
                request,
                timeout=timeout_seconds,
            ) as response:
                if response.status != 202:
                    raise HostedReportError("report_rejected")
                content_type = response.headers.get("Content-Type", "")
                if content_type.split(";", 1)[0].strip().lower() != "application/json":
                    raise HostedReportError("invalid_receipt")
                raw = response.read(MAX_RESPONSE_BYTES + 1)
            return _decode_receipt(raw, run_id)
        except HTTPError as exc:
            code = _http_error_code(exc)
            retry_delay = (
                _retry_delay_seconds(exc)
                if attempt == 0 and exc.code in _RETRYABLE_HTTP_STATUSES
                else None
            )
            if retry_delay is None:
                raise HostedReportError(code) from exc
            exc.close()
            time.sleep(retry_delay)
        except HostedReportError as exc:
            if attempt != 0 or exc.code != "invalid_receipt":
                raise
            time.sleep(DEFAULT_RETRY_DELAY_SECONDS)
        except (URLError, OSError, TimeoutError, HTTPException) as exc:
            if attempt != 0:
                raise HostedReportError("transport_error") from exc
            time.sleep(DEFAULT_RETRY_DELAY_SECONDS)
    raise AssertionError("bounded hosted-report attempts were exhausted")
