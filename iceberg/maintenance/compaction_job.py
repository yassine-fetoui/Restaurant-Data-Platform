"""
iceberg/maintenance/compaction_job.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Rewrite small Iceberg data files into optimal 128 MB files.
Run via Airflow or manually: python compaction_job.py --table bronze.kitchen_telemetry

Usage:
    python compaction_job.py --table <namespace.table> [--partition-filter <expr>]
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass

from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import GreaterThan
from pyiceberg.table import Table

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

TARGET_FILE_SIZE = 128 * 1024 * 1024   # 128 MB
MIN_FILE_COUNT   = 5                    # Only compact if at least N small files


@dataclass
class CompactionResult:
    table: str
    files_before: int
    files_after: int
    bytes_rewritten: int
    duration_secs: float


def compact_table(identifier: str, partition_filter: str | None = None) -> CompactionResult:
    """Compact small files in an Iceberg table."""
    catalog = load_catalog(
        "glue",
        **{"type": "glue", "warehouse": "s3://restaurant-iceberg-prod/"},
    )
    table: Table = catalog.load_table(identifier)

    # Snapshot before
    scan = table.scan()
    if partition_filter:
        scan = scan.filter(partition_filter)

    files_before = _count_data_files(table)
    log.info("Table %s: %d data files before compaction", identifier, files_before)

    if files_before < MIN_FILE_COUNT:
        log.info("Skipping compaction — fewer than %d files", MIN_FILE_COUNT)
        return CompactionResult(
            table=identifier,
            files_before=files_before,
            files_after=files_before,
            bytes_rewritten=0,
            duration_secs=0.0,
        )

    start = time.monotonic()

    # PyIceberg rewrite_data_files (equivalent to Spark rewriteDataFiles)
    from pyiceberg.table.rewrite import rewrite_data_files

    result = rewrite_data_files(
        table,
        target_file_size_bytes=TARGET_FILE_SIZE,
        strategy="binpack",        # bin-pack for random data
    )

    duration = time.monotonic() - start
    files_after = _count_data_files(table)

    log.info(
        "Compaction complete for %s: %d → %d files in %.1fs",
        identifier, files_before, files_after, duration,
    )

    return CompactionResult(
        table=identifier,
        files_before=files_before,
        files_after=files_after,
        bytes_rewritten=result.rewritten_bytes_count if hasattr(result, "rewritten_bytes_count") else 0,
        duration_secs=duration,
    )


def _count_data_files(table: Table) -> int:
    return sum(1 for _ in table.inspect.files().to_pydict()["file_path"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Compact Iceberg table data files")
    parser.add_argument("--table", required=True, help="Dotted table identifier, e.g. bronze.kitchen_telemetry")
    parser.add_argument("--partition-filter", default=None, help="Optional partition filter expression")
    args = parser.parse_args()

    result = compact_table(args.table, args.partition_filter)

    log.info(
        "Result | table=%s files_before=%d files_after=%d bytes_rewritten=%d duration=%.1fs",
        result.table, result.files_before, result.files_after,
        result.bytes_rewritten, result.duration_secs,
    )

    if result.files_after > result.files_before:
        log.error("Compaction increased file count — investigate!")
        sys.exit(1)


if __name__ == "__main__":
    main()
