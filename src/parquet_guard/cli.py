"""Command-line entry point."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import sys
from typing import Any

from .config import ConfigError, load_config
from .pipeline import run_pipeline, summary_dict


class JsonFormatter(logging.Formatter):
    """Emit one small structured event per log line."""

    def format(self, record: logging.LogRecord) -> str:
        value: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname.lower(),
            "event": getattr(record, "event", record.getMessage()),
        }
        for key in (
            "run_id",
            "file",
            "reason",
            "rows",
            "discovered",
            "succeeded",
            "quarantined",
            "skipped",
            "failed",
        ):
            if hasattr(record, key):
                value[key] = getattr(record, key)
        return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Process, validate, transform, and quarantine Parquet inputs."
    )
    parser.add_argument(
        "--config", required=True, help="Path to the TOML configuration"
    )
    return parser


def main() -> int:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("parquet_guard")
    logger.handlers[:] = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False

    arguments = _parser().parse_args()
    try:
        summary = run_pipeline(load_config(arguments.config))
    except ConfigError as exc:
        print(
            json.dumps({"error": "invalid_config", "message": str(exc)}),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(summary_dict(summary), sort_keys=True))
    return 0 if summary.quarantined == 0 and summary.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
