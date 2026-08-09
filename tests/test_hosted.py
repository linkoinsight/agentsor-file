from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import stat
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from uuid import uuid4

import pytest

from parquet_guard import hosted
from parquet_guard.hosted import HostedReportError, post_run_envelope


FIXED_NOW = datetime(2026, 7, 27, 12, 0, 2, tzinfo=timezone.utc)
REAL_UTC_NOW = hosted._utc_now


@pytest.fixture(autouse=True)
def fixed_hosted_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(hosted, "_utc_now", lambda: FIXED_NOW)


def utc_timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def test_real_clock_returns_aware_utc_datetime() -> None:
    current = REAL_UTC_NOW()

    assert isinstance(current, datetime)
    assert current.tzinfo is timezone.utc
    assert current.utcoffset() == timedelta(0)


def envelope() -> dict[str, object]:
    return {
        "schemaVersion": 1,
        "runId": str(uuid4()),
        "contractFingerprint": "a" * 64,
        "startedAt": "2026-07-27T12:00:00Z",
        "finishedAt": "2026-07-27T12:00:01Z",
        "overall": "passed",
        "fileFormat": "parquet",
        "rowCount": 1,
        "byteCount": 10,
        "schemaFingerprint": "b" * 64,
        "outputFingerprint": "c" * 64,
        "checks": {
            "readability": "passed",
            "schema": "passed",
            "rowBounds": "passed",
            "byteBounds": "passed",
            "eventFreshness": "not_configured",
            "duplicate": "not_configured",
        },
        "reasonCodes": [],
    }


def test_run_age_boundary_is_stable() -> None:
    value = envelope()
    boundary = FIXED_NOW - hosted._MAX_RUN_AGE
    value["startedAt"] = utc_timestamp(boundary)
    value["finishedAt"] = utc_timestamp(boundary)

    assert hosted._validate_envelope(value).int != 0

    older = boundary - timedelta(microseconds=1)
    value["startedAt"] = utc_timestamp(older)
    value["finishedAt"] = utc_timestamp(older)
    with pytest.raises(ValueError, match="invalid run timestamps"):
        hosted._validate_envelope(value)


def test_future_clock_skew_boundary_is_stable() -> None:
    value = envelope()
    boundary = FIXED_NOW + hosted._MAX_CLOCK_SKEW
    value["startedAt"] = utc_timestamp(boundary)
    value["finishedAt"] = utc_timestamp(boundary)

    assert hosted._validate_envelope(value).int != 0

    future = boundary + timedelta(microseconds=1)
    value["startedAt"] = utc_timestamp(future)
    value["finishedAt"] = utc_timestamp(future)
    with pytest.raises(ValueError, match="invalid run timestamps"):
        hosted._validate_envelope(value)


def token_file(tmp_path: Path) -> Path:
    path = tmp_path / "ingest-token"
    path.write_text(f"fr1_{'A' * 43}\n", encoding="ascii")
    path.chmod(0o600)
    return path


class FakeResponse:
    status = 202
    headers = {"Content-Type": "application/json; charset=utf-8"}

    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self, limit: int) -> bytes:
        return self._body[:limit]

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


def receipt_body(run_id: str, *, duplicate: bool = False) -> bytes:
    return json.dumps(
        {
            "accepted": True,
            "duplicate": duplicate,
            "runId": run_id,
            "deadlineState": "on_time",
            "nextExpectedAt": "2026-07-28T12:00:00Z",
            "deadlineAt": "2026-07-28T13:00:00Z",
        }
    ).encode()


def test_post_uses_only_fixed_endpoint_and_bearer_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = envelope()

    class FakeOpener:
        def open(self, request: object, timeout: int) -> FakeResponse:
            assert request.full_url == hosted.HOSTED_FILE_RUN_ENDPOINT
            assert request.method == "POST"
            assert request.get_header("Authorization") == f"Bearer fr1_{'A' * 43}"
            assert request.get_header("Content-type") == "application/json"
            assert timeout == 15
            sent = json.loads(request.data)
            assert sent == value
            return FakeResponse(receipt_body(value["runId"]))

    monkeypatch.setattr(hosted, "_build_opener", lambda: FakeOpener())
    receipt = post_run_envelope(value, token_file=token_file(tmp_path))

    assert str(receipt.run_id) == value["runId"]
    assert receipt.deadline_state == "on_time"
    assert receipt.duplicate is False


def test_exact_duplicate_receipt_is_a_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = envelope()
    monkeypatch.setattr(
        hosted,
        "_build_opener",
        lambda: SimpleNamespace(
            open=lambda *_args, **_kwargs: FakeResponse(
                receipt_body(value["runId"], duplicate=True)
            )
        ),
    )

    receipt = post_run_envelope(value, token_file=token_file(tmp_path))

    assert str(receipt.run_id) == value["runId"]
    assert receipt.duplicate is True


def test_ambiguous_transport_retries_the_exact_encoded_run_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = envelope()
    sent_bodies: list[bytes] = []
    delays: list[int] = []

    class AmbiguousThenDuplicateOpener:
        def open(self, request: object, timeout: int) -> FakeResponse:
            assert timeout == 15
            sent_bodies.append(bytes(request.data))
            if len(sent_bodies) == 1:
                raise URLError("response lost after possible commit")
            return FakeResponse(receipt_body(value["runId"], duplicate=True))

    monkeypatch.setattr(
        hosted,
        "_build_opener",
        lambda: AmbiguousThenDuplicateOpener(),
    )
    monkeypatch.setattr(hosted.time, "sleep", delays.append)

    receipt = post_run_envelope(value, token_file=token_file(tmp_path))

    assert receipt.duplicate is True
    assert sent_bodies == [sent_bodies[0], sent_bodies[0]]
    assert json.loads(sent_bodies[0])["runId"] == value["runId"]
    assert delays == [hosted.DEFAULT_RETRY_DELAY_SECONDS]


def test_invalid_success_receipt_retries_the_exact_encoded_run_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = envelope()
    sent_bodies: list[bytes] = []
    delays: list[int] = []

    class InvalidThenDuplicateOpener:
        def open(self, request: object, timeout: int) -> FakeResponse:
            assert timeout == 15
            sent_bodies.append(bytes(request.data))
            return FakeResponse(
                (
                    b'{"accepted":true}'
                    if len(sent_bodies) == 1
                    else receipt_body(value["runId"], duplicate=True)
                )
            )

    monkeypatch.setattr(
        hosted,
        "_build_opener",
        lambda: InvalidThenDuplicateOpener(),
    )
    monkeypatch.setattr(hosted.time, "sleep", delays.append)

    receipt = post_run_envelope(value, token_file=token_file(tmp_path))

    assert receipt.duplicate is True
    assert sent_bodies == [sent_bodies[0], sent_bodies[0]]
    assert delays == [hosted.DEFAULT_RETRY_DELAY_SECONDS]


def test_ambiguous_transport_stops_after_one_exact_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    delays: list[int] = []

    class AlwaysAmbiguousOpener:
        def open(self, _request: object, timeout: int) -> FakeResponse:
            nonlocal calls
            assert timeout == 15
            calls += 1
            raise URLError("still ambiguous")

    monkeypatch.setattr(
        hosted,
        "_build_opener",
        lambda: AlwaysAmbiguousOpener(),
    )
    monkeypatch.setattr(hosted.time, "sleep", delays.append)

    with pytest.raises(HostedReportError, match="^transport_error$"):
        post_run_envelope(envelope(), token_file=token_file(tmp_path))

    assert calls == 2
    assert delays == [hosted.DEFAULT_RETRY_DELAY_SECONDS]


@pytest.mark.parametrize("mode", [0o604, 0o640, 0o644])
def test_token_file_must_be_owner_only(tmp_path: Path, mode: int) -> None:
    path = token_file(tmp_path)
    path.chmod(mode)

    with pytest.raises(HostedReportError, match="invalid_token_file"):
        post_run_envelope(envelope(), token_file=path)


def test_token_file_symlink_is_rejected(tmp_path: Path) -> None:
    path = token_file(tmp_path)
    link = tmp_path / "link"
    link.symlink_to(path)

    with pytest.raises(HostedReportError, match="invalid_token_file"):
        post_run_envelope(envelope(), token_file=link)


def test_invalid_envelope_is_rejected_before_token_read(tmp_path: Path) -> None:
    value = envelope()
    value["path"] = "/must/not/be-representable"

    with pytest.raises(HostedReportError, match="invalid_envelope"):
        post_run_envelope(value, token_file=tmp_path / "missing")


@pytest.mark.parametrize(
    ("field", "unsafe_value"),
    [
        ("rowCount", "/private/customer/path"),
        ("checks", {"freeFormSecret": "customer-record"}),
        ("reasonCodes", ["customer-record"]),
    ],
)
def test_nested_free_form_values_are_rejected_before_network(
    tmp_path: Path,
    field: str,
    unsafe_value: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = envelope()
    value[field] = unsafe_value
    monkeypatch.setattr(
        hosted,
        "_build_opener",
        lambda: pytest.fail("network must not be reached"),
    )

    with pytest.raises(HostedReportError, match="invalid_envelope"):
        post_run_envelope(value, token_file=token_file(tmp_path))


def test_reason_must_match_its_check_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = envelope()
    value["overall"] = "failed"
    value["checks"]["rowBounds"] = "failed"
    value["reasonCodes"] = ["duplicate_output"]
    monkeypatch.setattr(
        hosted,
        "_build_opener",
        lambda: pytest.fail("network must not be reached"),
    )

    with pytest.raises(HostedReportError, match="invalid_envelope"):
        post_run_envelope(value, token_file=token_file(tmp_path))


def test_failed_check_requires_its_own_fixed_reason_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = envelope()
    value["overall"] = "failed"
    value["checks"]["schema"] = "failed"
    value["checks"]["rowBounds"] = "failed"
    value["reasonCodes"] = ["schema_drift"]
    monkeypatch.setattr(
        hosted,
        "_build_opener",
        lambda: pytest.fail("network must not be reached"),
    )

    with pytest.raises(HostedReportError, match="invalid_envelope"):
        post_run_envelope(value, token_file=token_file(tmp_path))


def test_mismatched_or_duplicate_receipt_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = envelope()
    bad = receipt_body(str(uuid4())).replace(
        b'"accepted": true',
        b'"accepted": true, "accepted": true',
    )
    calls = 0
    delays: list[int] = []

    def invalid_response(*_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        return nullcontext(FakeResponse(bad))

    monkeypatch.setattr(
        hosted,
        "_build_opener",
        lambda: SimpleNamespace(open=invalid_response),
    )
    monkeypatch.setattr(hosted.time, "sleep", delays.append)

    with pytest.raises(HostedReportError, match="invalid_receipt"):
        post_run_envelope(value, token_file=token_file(tmp_path))

    assert calls == 2
    assert delays == [hosted.DEFAULT_RETRY_DELAY_SECONDS]


def test_token_file_permissions_are_actually_private(tmp_path: Path) -> None:
    path = token_file(tmp_path)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (401, b"", "invalid_ingest_token"),
        (404, b"", "invalid_ingest_token"),
        (429, b"", "rate_limited"),
        (503, b"", "temporarily_unavailable"),
        (
            409,
            b'{"detail":"contract_mismatch"}',
            "contract_mismatch",
        ),
        (
            409,
            b'{"detail":"idempotency_conflict"}',
            "idempotency_conflict",
        ),
        (409, b'{"detail":"arbitrary"}', "report_rejected"),
    ],
)
def test_fixed_hosted_rejections_are_actionable_without_body_echo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    body: bytes,
    expected: str,
) -> None:
    value = envelope()
    calls = 0
    delays: list[int] = []

    class RejectingOpener:
        def open(self, request: object, timeout: int) -> object:
            nonlocal calls
            calls += 1
            headers = {"Content-Type": "application/json"}
            if status == 503:
                headers["Retry-After"] = "60"
            raise HTTPError(
                request.full_url,
                status,
                "must not be echoed",
                headers,
                io.BytesIO(body),
            )

    monkeypatch.setattr(hosted, "_build_opener", lambda: RejectingOpener())
    monkeypatch.setattr(hosted.time, "sleep", delays.append)

    with pytest.raises(HostedReportError, match=f"^{expected}$"):
        post_run_envelope(value, token_file=token_file(tmp_path))

    assert calls == (2 if status == 503 else 1)
    assert delays == ([60] if status == 503 else [])


@pytest.mark.parametrize(
    "status",
    [408, 500, 502, 504, *range(520, 528)],
)
def test_transient_http_failure_retries_once_then_accepts_duplicate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    value = envelope()
    sent_bodies: list[bytes] = []
    delays: list[int] = []

    class TransientThenDuplicateOpener:
        def open(self, request: object, timeout: int) -> FakeResponse:
            assert timeout == 15
            sent_bodies.append(bytes(request.data))
            if len(sent_bodies) == 1:
                raise HTTPError(
                    request.full_url,
                    status,
                    "transient",
                    {"Content-Type": "text/plain"},
                    io.BytesIO(b""),
                )
            return FakeResponse(receipt_body(value["runId"], duplicate=True))

    monkeypatch.setattr(
        hosted,
        "_build_opener",
        lambda: TransientThenDuplicateOpener(),
    )
    monkeypatch.setattr(hosted.time, "sleep", delays.append)

    receipt = post_run_envelope(value, token_file=token_file(tmp_path))

    assert receipt.duplicate is True
    assert sent_bodies == [sent_bodies[0], sent_bodies[0]]
    assert delays == [hosted.DEFAULT_RETRY_DELAY_SECONDS]


@pytest.mark.parametrize("retry_after", ["61", "-1", "tomorrow", "1.5"])
def test_unbounded_retry_after_fails_without_an_internal_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    retry_after: str,
) -> None:
    calls = 0

    class UnboundedRetryOpener:
        def open(self, request: object, timeout: int) -> object:
            nonlocal calls
            assert timeout == 15
            calls += 1
            raise HTTPError(
                request.full_url,
                503,
                "transient",
                {"Retry-After": retry_after},
                io.BytesIO(b""),
            )

    monkeypatch.setattr(hosted, "_build_opener", lambda: UnboundedRetryOpener())
    monkeypatch.setattr(
        hosted.time,
        "sleep",
        lambda _delay: pytest.fail("unbounded retry must not sleep"),
    )

    with pytest.raises(HostedReportError, match="^temporarily_unavailable$"):
        post_run_envelope(envelope(), token_file=token_file(tmp_path))

    assert calls == 1
