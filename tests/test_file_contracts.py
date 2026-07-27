from __future__ import annotations

from datetime import datetime, timedelta, timezone
from importlib.resources import files
import json
import os
from pathlib import Path
import re
import uuid

from jsonschema import Draft202012Validator
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pytest

from parquet_guard import (
    ContractConfigError,
    FileContract,
    FileContractInputError,
    check_file,
    load_file_contract,
)
from parquet_guard.file_cli import main
from parquet_guard.fingerprints import (
    FingerprintKeyError,
    project_fingerprint,
)
from parquet_guard.hosted import HostedReceipt


KEY = b"k" * 32
OTHER_KEY = b"q" * 32
NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
FINGERPRINT = re.compile(r"^[0-9a-f]{64}$")


def basic_contract(**changes: object) -> FileContract:
    values: dict[str, object] = {
        "required_schema": {"id": "string", "value": "int64"},
        "min_rows": 1,
        "max_rows": 10,
        "min_bytes": 1,
        "max_bytes": 1024 * 1024,
    }
    values.update(changes)
    return FileContract(**values).validate()


def basic_table() -> pa.Table:
    return pa.table(
        {
            "id": pa.array(["a", "b"], type=pa.string()),
            "value": pa.array([1, 2], type=pa.int64()),
        }
    )


def write_table(path: Path, table: pa.Table, file_format: str) -> None:
    if file_format == "parquet":
        pq.write_table(table, path)
    else:
        pacsv.write_csv(table, path)


def result_schema() -> dict[str, object]:
    resource = files("parquet_guard").joinpath("schemas/result-envelope-v1.schema.json")
    return json.loads(resource.read_text(encoding="utf-8"))


def assert_valid_envelope(value: dict[str, object]) -> None:
    Draft202012Validator(result_schema()).validate(value)


@pytest.mark.parametrize("file_format", ["csv", "parquet"])
def test_csv_and_parquet_pass_the_same_contract(
    tmp_path: Path, file_format: str
) -> None:
    source = tmp_path / f"input.{file_format}"
    write_table(source, basic_table(), file_format)

    result = check_file(
        source,
        basic_contract(),
        fingerprint_key=KEY,
        now=NOW,
    ).as_dict()

    assert result["overall"] == "passed"
    assert result["fileFormat"] == file_format
    assert result["rowCount"] == 2
    assert result["checks"] == {
        "readability": "passed",
        "schema": "passed",
        "rowBounds": "passed",
        "byteBounds": "passed",
        "eventFreshness": "not_configured",
        "duplicate": "not_configured",
    }
    assert result["reasonCodes"] == []
    assert result["startedAt"] == result["finishedAt"] == "2026-07-27T12:00:00Z"
    assert_valid_envelope(result)


def test_envelope_has_only_fixed_redacted_fields(tmp_path: Path) -> None:
    source = tmp_path / "customer-name-secret.parquet"
    table = pa.table(
        {
            "private-column": pa.array(["private-value"]),
            "value": pa.array([1], type=pa.int64()),
        }
    )
    pq.write_table(table, source)

    value = check_file(
        source,
        basic_contract(),
        fingerprint_key=KEY,
        now=NOW,
    ).as_dict()
    rendered = json.dumps(value, sort_keys=True)

    assert set(value) == {
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
    assert "customer-name-secret" not in rendered
    assert "private-column" not in rendered
    assert "private-value" not in rendered
    assert value["reasonCodes"] == ["schema_drift"]
    assert_valid_envelope(value)


def test_fingerprints_are_keyed_and_domain_separated() -> None:
    first = project_fingerprint(KEY, "schema", b"same")
    repeated = project_fingerprint(KEY, "schema", b"same")

    assert first == repeated
    assert first != project_fingerprint(OTHER_KEY, "schema", b"same")
    assert first != project_fingerprint(KEY, "output", b"same")
    assert FINGERPRINT.fullmatch(first)
    with pytest.raises(FingerprintKeyError):
        project_fingerprint(b"short", "schema", b"same")


def test_contract_fingerprint_is_semantic_order_independent() -> None:
    first = FileContract(required_schema={"id": "string", "value": "int64"}).validate()
    second = FileContract(required_schema={"value": "int64", "id": "string"}).validate()

    assert first.fingerprint(KEY) == second.fingerprint(KEY)
    assert first.fingerprint(KEY) != first.fingerprint(OTHER_KEY)


def test_schema_fingerprint_is_stable_for_repeated_checks(tmp_path: Path) -> None:
    source = tmp_path / "input.parquet"
    pq.write_table(basic_table(), source)

    first = check_file(source, basic_contract(), fingerprint_key=KEY, now=NOW)
    second = check_file(source, basic_contract(), fingerprint_key=KEY, now=NOW)
    other_project = check_file(
        source, basic_contract(), fingerprint_key=OTHER_KEY, now=NOW
    )

    assert first.schema_fingerprint == second.schema_fingerprint
    assert first.output_fingerprint == second.output_fingerprint
    assert first.schema_fingerprint != other_project.schema_fingerprint
    assert first.output_fingerprint != other_project.output_fingerprint


@pytest.mark.parametrize(
    ("contents", "reason"),
    [
        (b"not parquet", "format_invalid"),
        (None, "file_missing"),
    ],
)
def test_unreadable_results_are_typed_and_redacted(
    tmp_path: Path, contents: bytes | None, reason: str
) -> None:
    source = tmp_path / "do-not-disclose.parquet"
    if contents is not None:
        source.write_bytes(contents)

    value = check_file(
        source,
        basic_contract(),
        fingerprint_key=KEY,
        now=NOW,
    ).as_dict()
    rendered = json.dumps(value)

    assert value["overall"] == "failed"
    assert value["checks"]["readability"] == "failed"
    assert value["reasonCodes"] == [reason]
    assert "do-not-disclose" not in rendered
    assert "not parquet" not in rendered
    assert_valid_envelope(value)


@pytest.mark.parametrize(
    ("minimum", "maximum", "rows", "reason"),
    [
        (3, None, 2, "row_count_below_min"),
        (None, 1, 2, "row_count_above_max"),
    ],
)
def test_row_bounds_are_explicit(
    tmp_path: Path,
    minimum: int | None,
    maximum: int | None,
    rows: int,
    reason: str,
) -> None:
    source = tmp_path / "input.parquet"
    pq.write_table(basic_table().slice(0, rows), source)
    contract = basic_contract(min_rows=minimum, max_rows=maximum)

    value = check_file(source, contract, fingerprint_key=KEY, now=NOW).as_dict()

    assert value["checks"]["rowBounds"] == "failed"
    assert reason in value["reasonCodes"]
    assert_valid_envelope(value)


@pytest.mark.parametrize(
    ("minimum", "maximum", "reason"),
    [
        (10**6, None, "byte_count_below_min"),
        (None, 1, "byte_count_above_max"),
    ],
)
def test_byte_bounds_short_circuit_file_parsing(
    tmp_path: Path,
    minimum: int | None,
    maximum: int | None,
    reason: str,
) -> None:
    source = tmp_path / "input.parquet"
    pq.write_table(basic_table(), source)
    contract = basic_contract(min_bytes=minimum, max_bytes=maximum)

    value = check_file(source, contract, fingerprint_key=KEY, now=NOW).as_dict()

    assert value["checks"]["byteBounds"] == "failed"
    assert value["checks"]["readability"] == "inconclusive"
    assert value["outputFingerprint"] is None
    assert reason in value["reasonCodes"]
    assert_valid_envelope(value)


@pytest.mark.parametrize(
    ("event_values", "expected_status", "reason"),
    [
        (
            [NOW - timedelta(minutes=5)],
            "passed",
            None,
        ),
        (
            [NOW - timedelta(hours=2)],
            "failed",
            "event_too_old",
        ),
        (
            [None],
            "failed",
            "event_time_missing",
        ),
    ],
)
def test_optional_event_time_freshness(
    tmp_path: Path,
    event_values: list[datetime | None],
    expected_status: str,
    reason: str | None,
) -> None:
    source = tmp_path / "input.parquet"
    table = pa.table(
        {
            "id": pa.array(["a"], type=pa.string()),
            "event_time": pa.array(
                [
                    value.replace(tzinfo=None) if value is not None else None
                    for value in event_values
                ],
                type=pa.timestamp("ms"),
            ),
        }
    )
    pq.write_table(table, source)
    contract = FileContract(
        required_schema={
            "id": "string",
            "event_time": "timestamp[ms]",
        },
        min_rows=1,
        event_time_column="event_time",
        max_age_seconds=3600,
    ).validate()

    value = check_file(source, contract, fingerprint_key=KEY, now=NOW).as_dict()

    assert value["checks"]["eventFreshness"] == expected_status
    if reason is None:
        assert value["reasonCodes"] == []
    else:
        assert reason in value["reasonCodes"]
    assert_valid_envelope(value)


def test_duplicate_state_records_only_keyed_fingerprints(tmp_path: Path) -> None:
    source = tmp_path / "sensitive-name.parquet"
    state = tmp_path / "state.json"
    pq.write_table(basic_table(), source)

    first = check_file(
        source,
        basic_contract(),
        fingerprint_key=KEY,
        state_path=state,
        now=NOW,
    )
    second = check_file(
        source,
        basic_contract(),
        fingerprint_key=KEY,
        state_path=state,
        now=NOW,
    )
    state_value = json.loads(state.read_text())

    assert first.checks["duplicate"] == "passed"
    assert second.checks["duplicate"] == "passed"
    assert state_value == {
        "schema": "agentsor.file-contract-state/v1",
        "fingerprints": [first.output_fingerprint],
    }
    assert "sensitive-name" not in state.read_text()
    assert "id" not in state.read_text()


def test_duplicate_rejection_is_contract_controlled(tmp_path: Path) -> None:
    source = tmp_path / "input.parquet"
    state = tmp_path / "state.json"
    pq.write_table(basic_table(), source)
    contract = basic_contract(reject_duplicates=True)

    first = check_file(
        source,
        contract,
        fingerprint_key=KEY,
        state_path=state,
        now=NOW,
    )
    second = check_file(
        source,
        contract,
        fingerprint_key=KEY,
        state_path=state,
        now=NOW,
    )

    assert first.overall == "passed"
    assert second.overall == "failed"
    assert second.checks["duplicate"] == "failed"
    assert second.reason_codes == ("duplicate_output",)
    assert_valid_envelope(second.as_dict())


def test_duplicate_rejection_without_state_is_inconclusive(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.parquet"
    pq.write_table(basic_table(), source)

    value = check_file(
        source,
        basic_contract(reject_duplicates=True),
        fingerprint_key=KEY,
        now=NOW,
    ).as_dict()

    assert value["overall"] == "inconclusive"
    assert value["checks"]["duplicate"] == "inconclusive"
    assert value["reasonCodes"] == ["fingerprint_unavailable"]
    assert_valid_envelope(value)


def test_damaged_or_symlink_state_fails_closed_without_path_leak(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.parquet"
    state = tmp_path / "private-state-name.json"
    pq.write_table(basic_table(), source)
    state.write_text("damaged")

    value = check_file(
        source,
        basic_contract(),
        fingerprint_key=KEY,
        state_path=state,
        now=NOW,
    ).as_dict()

    assert value["overall"] == "inconclusive"
    assert value["checks"]["duplicate"] == "inconclusive"
    assert value["reasonCodes"] == ["check_error"]
    assert "private-state-name" not in json.dumps(value)
    assert_valid_envelope(value)


def test_csv_values_must_safely_cast_to_required_types(tmp_path: Path) -> None:
    source = tmp_path / "input.csv"
    source.write_text('id,value\n"a","not-an-integer"\n', encoding="utf-8")

    value = check_file(
        source,
        basic_contract(),
        fingerprint_key=KEY,
        now=NOW,
    ).as_dict()

    assert value["checks"]["readability"] == "passed"
    assert value["checks"]["schema"] == "failed"
    assert value["reasonCodes"] == ["schema_drift"]
    assert_valid_envelope(value)


def test_contract_loader_rejects_unknown_and_invalid_settings(
    tmp_path: Path,
) -> None:
    contract_path = tmp_path / "contract.toml"
    contract_path.write_text(
        """
[contract]
format = "parquet"
unknown_typo = true

[schema]
id = "string"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ContractConfigError):
        load_file_contract(contract_path)
    with pytest.raises(ContractConfigError):
        FileContract(
            required_schema={"time": "string"},
            event_time_column="time",
            max_age_seconds=60,
        ).validate()
    with pytest.raises(ContractConfigError):
        FileContract(
            required_schema={"id": "string"},
            max_bytes=2**40 + 1,
        ).validate()


def test_contract_format_mismatch_is_a_redacted_failure(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.csv"
    pacsv.write_csv(basic_table(), source)

    value = check_file(
        source,
        basic_contract(file_format="parquet"),
        fingerprint_key=KEY,
        now=NOW,
    ).as_dict()

    assert value["overall"] == "failed"
    assert value["checks"]["readability"] == "failed"
    assert value["reasonCodes"] == ["format_invalid"]
    assert_valid_envelope(value)


def test_unsupported_extension_is_an_input_error(tmp_path: Path) -> None:
    source = tmp_path / "input.json"
    source.write_text("{}")

    with pytest.raises(FileContractInputError, match="unsupported_format"):
        check_file(source, basic_contract(), fingerprint_key=KEY)


def test_cli_init_creates_private_key_and_refuses_overwrite(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    contract = tmp_path / "contract.toml"
    key = tmp_path / "fingerprint.key"

    first = main(
        [
            "init",
            "--format",
            "parquet",
            "--contract",
            str(contract),
            "--fingerprint-key-file",
            str(key),
        ]
    )
    output = capsys.readouterr()

    assert first == 0
    assert json.loads(output.out) == {
        "schemaVersion": 1,
        "operation": "init",
        "status": "created",
    }
    assert output.err == ""
    assert len(key.read_bytes()) == 32
    assert stat_mode(key) == 0o600
    assert load_file_contract(contract).file_format == "parquet"
    secret = key.read_bytes().hex()
    assert secret not in output.out

    second = main(
        [
            "init",
            "--format",
            "parquet",
            "--contract",
            str(contract),
            "--fingerprint-key-file",
            str(key),
        ]
    )
    repeated = capsys.readouterr()
    assert second == 2
    assert json.loads(repeated.out)["error"] == "init_target_exists"


def stat_mode(path: Path) -> int:
    return os.stat(path).st_mode & 0o777


def test_cli_check_emits_only_the_result_envelope(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "secret-customer-name.parquet"
    contract = tmp_path / "contract.toml"
    key = tmp_path / "secret-project-key"
    pq.write_table(basic_table(), source)
    contract.write_text(
        """
[contract]
format = "parquet"
min_rows = 1

[schema]
id = "string"
value = "int64"
""".strip(),
        encoding="utf-8",
    )
    key.write_bytes(KEY)
    key.chmod(0o600)

    exit_code = main(
        [
            "check",
            str(source),
            "--contract",
            str(contract),
            "--fingerprint-key-file",
            str(key),
        ]
    )
    output = capsys.readouterr()
    value = json.loads(output.out)

    assert exit_code == 0
    assert output.err == ""
    assert value["overall"] == "passed"
    assert "secret-customer-name" not in output.out
    assert "secret-project-key" not in output.out
    assert KEY.hex() not in output.out
    assert_valid_envelope(value)


def test_cli_errors_are_fixed_and_redacted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "private-input.json"
    contract = tmp_path / "private-contract.toml"
    key = tmp_path / "private-key"
    source.write_text("{}")
    contract.write_text("[broken")
    key.write_bytes(KEY)
    key.chmod(0o600)

    exit_code = main(
        [
            "check",
            str(source),
            "--contract",
            str(contract),
            "--fingerprint-key-file",
            str(key),
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 2
    assert json.loads(output.out) == {
        "schema": "agentsor.file-contract-error/v1",
        "error": "invalid_contract",
    }
    assert "private-" not in output.out
    assert output.err == ""


@pytest.mark.parametrize("mode", [0o604, 0o640, 0o644])
def test_cli_rejects_group_or_world_readable_fingerprint_key(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    mode: int,
) -> None:
    source = tmp_path / "input.parquet"
    contract = tmp_path / "contract.toml"
    key = tmp_path / "fingerprint.key"
    pq.write_table(basic_table(), source)
    contract.write_text(
        """
[contract]
format = "parquet"

[schema]
id = "string"
value = "int64"
""".strip(),
        encoding="utf-8",
    )
    key.write_bytes(KEY)
    key.chmod(mode)

    exit_code = main(
        [
            "check",
            str(source),
            "--contract",
            str(contract),
            "--fingerprint-key-file",
            str(key),
        ]
    )

    assert exit_code == 2
    assert json.loads(capsys.readouterr().out)["error"] == ("invalid_fingerprint_key")


def test_cli_report_posts_generated_envelope_without_exposing_token(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.parquet"
    pq.write_table(basic_table(), source)
    contract = tmp_path / "contract.toml"
    contract.write_text(
        """
[contract]
format = "parquet"
min_rows = 1
max_rows = 10
min_bytes = 1
max_bytes = 100000

[schema]
id = "string"
value = "int64"
""".strip(),
        encoding="utf-8",
    )
    key = tmp_path / "fingerprint.key"
    key.write_bytes(KEY)
    key.chmod(0o600)
    token = tmp_path / "ingest-token"
    token.write_text(f"fr1_{'A' * 43}\n", encoding="ascii")
    token.chmod(0o600)
    captured_envelope: dict[str, object] = {}

    def fake_post(
        value: dict[str, object],
        *,
        token_file: Path,
    ) -> HostedReceipt:
        captured_envelope.update(value)
        assert token_file == token
        return HostedReceipt(
            run_id=uuid.UUID(str(value["runId"])),
            duplicate=False,
            deadline_state="on_time",
            next_expected_at=datetime(2026, 7, 28, tzinfo=timezone.utc),
            deadline_at=datetime(2026, 7, 28, 1, tzinfo=timezone.utc),
        )

    monkeypatch.setattr("parquet_guard.file_cli.post_run_envelope", fake_post)
    exit_code = main(
        [
            "report",
            str(source),
            "--contract",
            str(contract),
            "--fingerprint-key-file",
            str(key),
            "--token-file",
            str(token),
        ]
    )
    rendered = capsys.readouterr().out

    assert exit_code == 0
    assert captured_envelope["overall"] == "passed"
    assert str(token) not in rendered
    assert f"fr1_{'A' * 43}" not in rendered
    receipt = json.loads(rendered)
    assert receipt["operation"] == "report"
    assert receipt["accepted"] is True
    assert receipt["runId"] == captured_envelope["runId"]


def test_cli_report_rejects_shared_credential_target_before_read(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    shared = tmp_path / "shared"

    exit_code = main(
        [
            "report",
            str(tmp_path / "input.parquet"),
            "--contract",
            str(tmp_path / "contract.toml"),
            "--fingerprint-key-file",
            str(shared),
            "--token-file",
            str(shared),
        ]
    )

    assert exit_code == 2
    assert json.loads(capsys.readouterr().out)["error"] == (
        "credential_target_conflict"
    )


def test_schema_command_prints_the_packaged_schema(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["schema"]) == 0
    output = capsys.readouterr()

    assert json.loads(output.out) == result_schema()
    assert output.err == ""


def test_schema_rejects_null_measurement_for_a_passed_check(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.parquet"
    pq.write_table(basic_table(), source)
    value = check_file(source, basic_contract(), fingerprint_key=KEY, now=NOW).as_dict()
    value["rowCount"] = None

    errors = list(Draft202012Validator(result_schema()).iter_errors(value))

    assert errors
