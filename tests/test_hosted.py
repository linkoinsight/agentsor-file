from __future__ import annotations

from contextlib import nullcontext
import io
import json
from pathlib import Path
import stat
from types import SimpleNamespace
from urllib.error import HTTPError
from uuid import uuid4

import pytest

from parquet_guard import hosted
from parquet_guard.hosted import HostedReportError, post_run_envelope


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


def receipt_body(run_id: str) -> bytes:
    return json.dumps(
        {
            "accepted": True,
            "duplicate": False,
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
    monkeypatch.setattr(
        hosted,
        "_build_opener",
        lambda: SimpleNamespace(
            open=lambda *_args, **_kwargs: nullcontext(FakeResponse(bad))
        ),
    )

    with pytest.raises(HostedReportError, match="invalid_receipt"):
        post_run_envelope(value, token_file=token_file(tmp_path))


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

    class RejectingOpener:
        def open(self, request: object, timeout: int) -> object:
            raise HTTPError(
                request.full_url,
                status,
                "must not be echoed",
                {"Content-Type": "application/json"},
                io.BytesIO(body),
            )

    monkeypatch.setattr(hosted, "_build_opener", lambda: RejectingOpener())

    with pytest.raises(HostedReportError, match=f"^{expected}$"):
        post_run_envelope(value, token_file=token_file(tmp_path))
