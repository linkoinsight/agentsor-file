"""Configuration for offline CSV and Parquet file contracts."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from types import MappingProxyType
from typing import Mapping
import tomllib

import pyarrow as pa

from .fingerprints import project_fingerprint


class ContractConfigError(ValueError):
    """Raised when a file contract is missing, ambiguous, or unsafe."""


_CONTRACT_KEYS = {
    "format",
    "allow_extra_columns",
    "min_rows",
    "max_rows",
    "min_bytes",
    "max_bytes",
    "csv_delimiter",
    "event_time_column",
    "max_age_seconds",
    "reject_duplicates",
}
_BOUNDS = ("min_rows", "max_rows", "min_bytes", "max_bytes")
MAX_REPORTED_ROWS = 10**12
MAX_REPORTED_BYTES = 2**40


@dataclass(frozen=True)
class FileContract:
    """A validated, format-independent file contract."""

    required_schema: Mapping[str, str]
    file_format: str | None = None
    allow_extra_columns: bool = False
    min_rows: int | None = None
    max_rows: int | None = None
    min_bytes: int | None = None
    max_bytes: int | None = None
    csv_delimiter: str = ","
    event_time_column: str | None = None
    max_age_seconds: int | None = None
    reject_duplicates: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "required_schema", MappingProxyType(dict(self.required_schema))
        )

    def validate(self) -> "FileContract":
        """Return this contract after validating all invariants."""

        if not self.required_schema:
            raise ContractConfigError(
                "schema must contain at least one required column"
            )
        for column, type_name in self.required_schema.items():
            if not isinstance(column, str) or not column:
                raise ContractConfigError(
                    "schema column names must be non-empty strings"
                )
            if not isinstance(type_name, str) or not type_name:
                raise ContractConfigError("schema values must be Arrow type aliases")
            try:
                pa.type_for_alias(type_name)
            except (TypeError, ValueError) as exc:
                raise ContractConfigError(
                    f"unsupported Arrow type alias for {column!r}: {type_name!r}"
                ) from exc

        if not isinstance(self.allow_extra_columns, bool):
            raise ContractConfigError(
                "contract.allow_extra_columns must be true or false"
            )
        if self.file_format not in {None, "csv", "parquet"}:
            raise ContractConfigError("contract.format must be csv or parquet")
        if not isinstance(self.reject_duplicates, bool):
            raise ContractConfigError(
                "contract.reject_duplicates must be true or false"
            )
        for name in _BOUNDS:
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ContractConfigError(
                    f"contract.{name} must be a non-negative integer"
                )
        for name in ("min_rows", "max_rows"):
            value = getattr(self, name)
            if value is not None and value > MAX_REPORTED_ROWS:
                raise ContractConfigError(
                    f"contract.{name} exceeds the supported row bound"
                )
        for name in ("min_bytes", "max_bytes"):
            value = getattr(self, name)
            if value is not None and value > MAX_REPORTED_BYTES:
                raise ContractConfigError(
                    f"contract.{name} exceeds the supported byte bound"
                )
        if (
            self.min_rows is not None
            and self.max_rows is not None
            and self.min_rows > self.max_rows
        ):
            raise ContractConfigError("contract.min_rows exceeds contract.max_rows")
        if (
            self.min_bytes is not None
            and self.max_bytes is not None
            and self.min_bytes > self.max_bytes
        ):
            raise ContractConfigError("contract.min_bytes exceeds contract.max_bytes")

        delimiter_bytes = (
            self.csv_delimiter.encode("utf-8")
            if isinstance(self.csv_delimiter, str)
            else b""
        )
        if len(delimiter_bytes) != 1 or self.csv_delimiter in {"\r", "\n", '"'}:
            raise ContractConfigError(
                "contract.csv_delimiter must be one ASCII character"
            )

        freshness_values = (self.event_time_column, self.max_age_seconds)
        if (freshness_values[0] is None) != (freshness_values[1] is None):
            raise ContractConfigError(
                "event_time_column and max_age_seconds must be configured together"
            )
        if self.event_time_column is not None:
            if (
                not isinstance(self.event_time_column, str)
                or not self.event_time_column
            ):
                raise ContractConfigError(
                    "contract.event_time_column must be a non-empty string"
                )
            configured_type = self.required_schema.get(self.event_time_column)
            if configured_type is None:
                raise ContractConfigError(
                    "event_time_column must name a required schema column"
                )
            if not pa.types.is_timestamp(pa.type_for_alias(configured_type)):
                raise ContractConfigError(
                    "event_time_column must use an Arrow timestamp type"
                )
            if (
                isinstance(self.max_age_seconds, bool)
                or not isinstance(self.max_age_seconds, int)
                or self.max_age_seconds < 0
            ):
                raise ContractConfigError(
                    "contract.max_age_seconds must be a non-negative integer"
                )
        return self

    def canonical_value(self) -> dict[str, object]:
        """Return the stable semantic representation used for hashing."""

        return {
            "schema": "agentsor.file-contract/v1",
            "format": self.file_format,
            "required_schema": [
                {
                    "name": name,
                    "type": str(pa.type_for_alias(type_name)),
                }
                for name, type_name in sorted(self.required_schema.items())
            ],
            "allow_extra_columns": self.allow_extra_columns,
            "min_rows": self.min_rows,
            "max_rows": self.max_rows,
            "min_bytes": self.min_bytes,
            "max_bytes": self.max_bytes,
            "csv_delimiter": self.csv_delimiter,
            "event_time_column": self.event_time_column,
            "max_age_seconds": self.max_age_seconds,
            "reject_duplicates": self.reject_duplicates,
        }

    def fingerprint(self, key: bytes) -> str:
        """Return a project-scoped fingerprint of effective semantics."""

        payload = json.dumps(
            self.canonical_value(), sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return project_fingerprint(key, "contract", payload)


def _optional_integer(table: dict[str, object], key: str) -> int | None:
    value = table.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractConfigError(f"contract.{key} must be a non-negative integer")
    return value


def _optional_string(table: dict[str, object], key: str) -> str | None:
    value = table.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ContractConfigError(f"contract.{key} must be a non-empty string")
    return value


def load_file_contract(path: Path | str) -> FileContract:
    """Load a strict TOML file contract."""

    contract_path = Path(path)
    try:
        raw = tomllib.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise ContractConfigError("could not load the file contract") from exc

    if set(raw) - {"contract", "schema"}:
        raise ContractConfigError("file contract contains an unknown table")
    contract_table = raw.get("contract")
    schema = raw.get("schema")
    if not isinstance(contract_table, dict) or not isinstance(schema, dict):
        raise ContractConfigError(
            "file contract requires [contract] and [schema] tables"
        )
    if set(contract_table) - _CONTRACT_KEYS:
        raise ContractConfigError("contract table contains an unknown setting")
    if not all(
        isinstance(key, str) and isinstance(value, str) for key, value in schema.items()
    ):
        raise ContractConfigError(
            "schema entries must map column names to Arrow type aliases"
        )

    allow_extra = contract_table.get("allow_extra_columns", False)
    delimiter = contract_table.get("csv_delimiter", ",")
    return FileContract(
        required_schema=dict(schema),
        file_format=contract_table.get("format"),
        allow_extra_columns=allow_extra,
        min_rows=_optional_integer(contract_table, "min_rows"),
        max_rows=_optional_integer(contract_table, "max_rows"),
        min_bytes=_optional_integer(contract_table, "min_bytes"),
        max_bytes=_optional_integer(contract_table, "max_bytes"),
        csv_delimiter=delimiter,
        event_time_column=_optional_string(contract_table, "event_time_column"),
        max_age_seconds=_optional_integer(contract_table, "max_age_seconds"),
        reject_duplicates=contract_table.get("reject_duplicates", False),
    ).validate()
