"""External configuration for the demonstration pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
import tomllib

import pyarrow as pa


class ConfigError(ValueError):
    """Raised when a configuration cannot be used safely."""


@dataclass(frozen=True)
class PipelineConfig:
    """Validated runtime configuration."""

    input_dir: Path
    output_dir: Path
    quarantine_dir: Path
    state_dir: Path
    required_schema: dict[str, str]
    output_compression: str = "zstd"
    allow_extra_columns: bool = False
    amount_column: str | None = None
    amount_multiplier: str = "100"
    amount_output_column: str = "amount_cents"
    amount_rounding: str = "half_even"

    def validate(self) -> "PipelineConfig":
        directories = (
            self.input_dir.resolve(),
            self.output_dir.resolve(),
            self.quarantine_dir.resolve(),
            self.state_dir.resolve(),
        )
        if len(set(directories)) != len(directories):
            raise ConfigError("pipeline directories must be distinct")
        if not self.required_schema:
            raise ConfigError("schema must contain at least one required column")
        for column, type_name in self.required_schema.items():
            if not column or not isinstance(column, str):
                raise ConfigError("schema column names must be non-empty strings")
            try:
                pa.type_for_alias(type_name)
            except (ValueError, TypeError) as exc:
                raise ConfigError(
                    f"unsupported Arrow type alias for {column!r}: {type_name!r}"
                ) from exc
        if self.amount_column is not None:
            configured_type = self.required_schema.get(self.amount_column)
            if configured_type not in {"double", "float64", "float", "float32"}:
                raise ConfigError(
                    "amount_column must name a required floating-point column"
                )
            if (
                not isinstance(self.amount_output_column, str)
                or not self.amount_output_column.strip()
            ):
                raise ConfigError("amount_output_column must be a non-empty string")
            if self.amount_output_column in self.required_schema:
                raise ConfigError(
                    "amount_output_column must not overwrite a required input column"
                )
            if isinstance(self.amount_multiplier, bool):
                raise ConfigError("amount_multiplier must be a decimal number")
            try:
                multiplier = Decimal(self.amount_multiplier)
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise ConfigError("amount_multiplier must be a decimal number") from exc
            if not multiplier.is_finite():
                raise ConfigError("amount_multiplier must be finite")
            if self.amount_rounding not in {
                "half_even",
                "half_up",
                "half_down",
                "down",
                "up",
            }:
                raise ConfigError("unsupported amount_rounding rule")
        if self.output_compression not in {
            "none",
            "snappy",
            "gzip",
            "brotli",
            "zstd",
            "lz4",
        }:
            raise ConfigError("unsupported Parquet output compression")
        if self.output_compression != "none" and not pa.Codec.is_available(
            self.output_compression
        ):
            raise ConfigError(
                f"Parquet compression codec is unavailable: {self.output_compression}"
            )
        return self


def _required_string(table: dict[str, object], key: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"pipeline.{key} must be a non-empty string")
    return value.strip()


def load_config(path: Path | str) -> PipelineConfig:
    """Load a TOML configuration, resolving paths relative to that file."""

    config_path = Path(path).resolve()
    try:
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"could not load configuration: {exc}") from exc

    pipeline = raw.get("pipeline")
    schema = raw.get("schema")
    if not isinstance(pipeline, dict) or not isinstance(schema, dict):
        raise ConfigError("configuration requires [pipeline] and [schema] tables")

    base = config_path.parent

    def configured_path(key: str) -> Path:
        value = Path(_required_string(pipeline, key))
        return value.resolve() if value.is_absolute() else (base / value).resolve()

    if not all(
        isinstance(key, str) and isinstance(value, str) for key, value in schema.items()
    ):
        raise ConfigError("schema entries must map column names to Arrow type aliases")

    amount_column = pipeline.get("amount_column")
    if amount_column is not None and (
        not isinstance(amount_column, str) or not amount_column.strip()
    ):
        raise ConfigError("pipeline.amount_column must be a non-empty string")

    allow_extra = pipeline.get("allow_extra_columns", False)
    if not isinstance(allow_extra, bool):
        raise ConfigError("pipeline.allow_extra_columns must be true or false")

    compression = pipeline.get("output_compression", "zstd")
    if not isinstance(compression, str):
        raise ConfigError("pipeline.output_compression must be a string")

    amount_multiplier = pipeline.get("amount_multiplier", "100")
    if isinstance(amount_multiplier, (int, float)):
        amount_multiplier = str(amount_multiplier)
    if not isinstance(amount_multiplier, str) or not amount_multiplier.strip():
        raise ConfigError(
            "pipeline.amount_multiplier must be a decimal string or number"
        )

    amount_output_column = pipeline.get("amount_output_column", "amount_cents")
    if not isinstance(amount_output_column, str) or not amount_output_column.strip():
        raise ConfigError("pipeline.amount_output_column must be a non-empty string")

    amount_rounding = pipeline.get("amount_rounding", "half_even")
    if not isinstance(amount_rounding, str):
        raise ConfigError("pipeline.amount_rounding must be a string")

    return PipelineConfig(
        input_dir=configured_path("input_dir"),
        output_dir=configured_path("output_dir"),
        quarantine_dir=configured_path("quarantine_dir"),
        state_dir=configured_path("state_dir"),
        required_schema=dict(schema),
        output_compression=compression.lower(),
        allow_extra_columns=allow_extra,
        amount_column=amount_column.strip() if isinstance(amount_column, str) else None,
        amount_multiplier=amount_multiplier.strip(),
        amount_output_column=amount_output_column.strip(),
        amount_rounding=amount_rounding.strip().lower(),
    ).validate()
