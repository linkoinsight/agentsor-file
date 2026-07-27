"""Command line interface for Agentsor File Contracts."""

from __future__ import annotations

import argparse
from importlib.resources import files
import json
import os
from pathlib import Path
import secrets
import stat
from typing import Sequence
from uuid import uuid4

from .contracts import ContractConfigError, load_file_contract
from .file_contracts import (
    DuplicateStateError,
    FileContractInputError,
    check_file,
)
from .fingerprints import FingerprintKeyError, validate_fingerprint_key
from .hosted import HostedReportError, post_run_envelope


ERROR_SCHEMA = "agentsor.file-contract-error/v1"
_INIT_TEMPLATE = """[contract]
format = "{file_format}"
allow_extra_columns = false
min_rows = 1
max_rows = 1000000
min_bytes = 1
max_bytes = 104857600
csv_delimiter = ","
reject_duplicates = false

# Optional freshness requires both settings and a timestamp in [schema].
# event_time_column = "event_time"
# max_age_seconds = 86400

[schema]
id = "string"
"""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agentsor-file",
        description=("Check one local CSV or Parquet file and emit redacted JSON."),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="create a local key and example contract")
    init.add_argument(
        "--format",
        choices=("csv", "parquet"),
        required=True,
        dest="file_format",
        help="example contract file format",
    )
    init.add_argument(
        "--contract",
        type=Path,
        required=True,
        help="new TOML contract path",
    )
    init.add_argument(
        "--fingerprint-key-file",
        type=Path,
        required=True,
        help="new private key path",
    )

    def add_check_arguments(command: argparse.ArgumentParser) -> None:
        command.add_argument("input", type=Path, help="CSV or Parquet input path")
        command.add_argument(
            "--contract",
            type=Path,
            required=True,
            help="TOML file contract path",
        )
        command.add_argument(
            "--fingerprint-key-file",
            type=Path,
            required=True,
            help="private project fingerprint key path",
        )

    check = subparsers.add_parser("check", help="check one local file offline")
    add_check_arguments(check)
    check.add_argument(
        "--state",
        type=Path,
        help="optional local JSON state path for duplicate detection",
    )
    report = subparsers.add_parser(
        "report",
        help="check one local file and report its redacted result",
    )
    add_check_arguments(report)
    report.add_argument(
        "--token-file",
        type=Path,
        required=True,
        help="owner-only hosted ingest-token file",
    )
    subparsers.add_parser("schema", help="print the result-envelope JSON Schema")
    return parser


def _emit(value: dict[str, object]) -> None:
    print(json.dumps(value, sort_keys=True, separators=(",", ":")))


def _error(code: str) -> int:
    _emit({"schema": ERROR_SCHEMA, "error": code})
    return 2


def _same_path(first: Path, second: Path) -> bool:
    return first.resolve(strict=False) == second.resolve(strict=False)


def _load_key(path: Path) -> bytes:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor: int | None = None
    try:
        if not hasattr(os, "O_NOFOLLOW") and path.is_symlink():
            raise FingerprintKeyError("unsafe fingerprint key file")
        descriptor = os.open(path, flags)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size > 4096
            or metadata.st_mode & 0o077
        ):
            raise FingerprintKeyError("unsafe fingerprint key file")
        if hasattr(os, "geteuid") and metadata.st_uid != os.geteuid():
            raise FingerprintKeyError("unsafe fingerprint key file")
        value = os.read(descriptor, 4097)
    except (OSError, FingerprintKeyError) as exc:
        raise FingerprintKeyError("could not load fingerprint key") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    return validate_fingerprint_key(value)


def _publish_new_file(path: Path, payload: bytes, mode: int) -> None:
    if path.is_symlink() or path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            mode,
        )
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _init(arguments: argparse.Namespace) -> int:
    contract_path = arguments.contract
    key_path = arguments.fingerprint_key_file
    if _same_path(contract_path, key_path):
        return _error("init_target_conflict")
    if (
        contract_path.is_symlink()
        or contract_path.exists()
        or key_path.is_symlink()
        or key_path.exists()
    ):
        return _error("init_target_exists")
    contract = _INIT_TEMPLATE.format(file_format=arguments.file_format).encode("utf-8")
    try:
        _publish_new_file(contract_path, contract, 0o644)
        _publish_new_file(key_path, secrets.token_bytes(32), 0o600)
    except FileExistsError:
        return _error("init_target_exists")
    except OSError:
        return _error("init_failed")
    _emit({"schemaVersion": 1, "operation": "init", "status": "created"})
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI without printing paths, values, keys, or exceptions."""

    arguments = _parser().parse_args(argv)
    if arguments.command == "schema":
        schema_path = files("parquet_guard").joinpath(
            "schemas/result-envelope-v1.schema.json"
        )
        print(schema_path.read_text(encoding="utf-8").strip())
        return 0
    if arguments.command == "init":
        return _init(arguments)

    targets: list[Path] = [
        arguments.input,
        arguments.contract,
        arguments.fingerprint_key_file,
    ]
    state_path = getattr(arguments, "state", None)
    token_file = getattr(arguments, "token_file", None)
    if state_path is not None and any(
        _same_path(state_path, target) for target in targets
    ):
        return _error("state_target_conflict")
    if token_file is not None and any(
        _same_path(token_file, target) for target in targets
    ):
        return _error("credential_target_conflict")
    try:
        key = _load_key(arguments.fingerprint_key_file)
        contract = load_file_contract(arguments.contract)
        result = check_file(
            arguments.input,
            contract,
            fingerprint_key=key,
            state_path=state_path,
        )
    except ContractConfigError:
        return _error("invalid_contract")
    except FingerprintKeyError:
        return _error("invalid_fingerprint_key")
    except DuplicateStateError:
        return _error("invalid_state")
    except FileContractInputError as exc:
        return _error(
            "unsupported_format"
            if str(exc) == "unsupported_format"
            else "invalid_input"
        )
    except Exception:
        return _error("internal_error")
    if arguments.command == "report":
        try:
            receipt = post_run_envelope(
                result.as_dict(),
                token_file=arguments.token_file,
            )
        except HostedReportError as exc:
            return _error(exc.code)
        _emit(
            {
                "schemaVersion": 1,
                "operation": "report",
                "accepted": True,
                "duplicate": receipt.duplicate,
                "runId": str(receipt.run_id),
                "overall": result.overall,
                "deadlineState": receipt.deadline_state,
                "nextExpectedAt": (
                    receipt.next_expected_at.isoformat().replace("+00:00", "Z")
                ),
                "deadlineAt": (receipt.deadline_at.isoformat().replace("+00:00", "Z")),
            }
        )
        return 0 if result.overall == "passed" else 1
    _emit(result.as_dict())
    return 0 if result.overall == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
