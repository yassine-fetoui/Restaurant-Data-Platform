"""
iceberg/maintenance/expire_snapshots.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Expire old Iceberg snapshots while honouring compliance retention windows.

Food safety tables (kitchen_telemetry, temperature_logs) → 7-year retention.
All other tables → configurable via --older-than-days (default: 7).

Usage:
    python expire_snapshots.py --older-than-days 7
    python expire_snapshots.py --table bronze.pos_transactions --older-than-days 30
"""
from __future__ import annotations

import argparse
import logging
from datetime import timedelta

import pandas as pd
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

# Tables with mandatory long-term retention (never expire within 7 years)
FOOD_SAFETY_TABLES = {
    "bronze.kitchen_telemetry",
    "bronze.temperature_logs",
    "bronze.equipment_health",
}

SEVEN_YEARS_DAYS = 365 * 7


def expire_table_snapshots(identifier: str, older_than_days: int) -> None:
    """Expire snapshots older than N days, skipping food-safety tables within retention."""
    if identifier in FOOD_SAFETY_TABLES and older_than_days < SEVEN_YEARS_DAYS:
        log.warning(
            "Skipping %s — food safety table requires 7-year retention (requested %d days)",
            identifier, older_than_days,
        )
        return

    catalog = load_catalog(
        "glue",
        **{"type": "glue", "warehouse": "s3://restaurant-iceberg-prod/"},
    )
    table: Table = catalog.load_table(identifier)

    cutoff_ms = int(
        (pd.Timestamp.utcnow() - pd.Timedelta(days=older_than_days)).timestamp() * 1000
    )

    before = _snapshot_count(table)
    table.expire_snapshots().expire_older_than(cutoff_ms).commit()
    after = _snapshot_count(table)

    log.info(
        "Expired snapshots for %s: %d → %d (cutoff: %d days)",
        identifier, before, after, older_than_days,
    )


def _snapshot_count(table: Table) -> int:
    return len(list(table.history()))


def _list_all_tables(catalog) -> list[str]:
    tables = []
    for namespace in catalog.list_namespaces():
        for table in catalog.list_tables(namespace):
            tables.append(".".join(table))
    return tables


def main() -> None:
    parser = argparse.ArgumentParser(description="Expire old Iceberg snapshots")
    parser.add_argument("--table", default=None, help="Single table (default: all tables)")
    parser.add_argument("--older-than-days", type=int, default=7, help="Expire snapshots older than N days")
    args = parser.parse_args()

    catalog = load_catalog("glue", **{"type": "glue", "warehouse": "s3://restaurant-iceberg-prod/"})

    if args.table:
        targets = [args.table]
    else:
        targets = _list_all_tables(catalog)

    log.info("Processing %d tables with retention=%d days", len(targets), args.older_than_days)

    for table_id in targets:
        try:
            expire_table_snapshots(table_id, args.older_than_days)
        except Exception as exc:
            log.error("Failed to expire snapshots for %s: %s", table_id, exc)


if __name__ == "__main__":
    main()
