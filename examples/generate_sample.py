"""Create a deterministic, non-customer Parquet fixture for the demo."""

from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data/inbox/sample.parquet",
        help="Destination Parquet path",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace an existing sample at the destination",
    )
    args = parser.parse_args()
    if args.output.exists() and not args.force:
        parser.error("output already exists; pass --force to replace it")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "event_id": pa.array([1001, 1002, 1003], type=pa.int64()),
            "amount": pa.array([10.25, 2.34, 1.005], type=pa.float64()),
            "category": pa.array(["alpha", "beta", "sample"], type=pa.string()),
        }
    )
    pq.write_table(table, args.output, compression="zstd")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
