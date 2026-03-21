"""Shared Iceberg helpers for Airflow DAGs."""
from __future__ import annotations

import logging

import pandas as pd
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table

log = logging.getLogger(__name__)

_catalog: Catalog | None = None


def _get_catalog() -> Catalog:
    global _catalog
    if _catalog is None:
        _catalog = load_catalog(
            "glue",
            **{
                "type": "glue",
                "warehouse": "s3://restaurant-iceberg-prod/",
            },
        )
    return _catalog


def get_iceberg_table(identifier: str) -> Table:
    """Load an Iceberg table by dotted identifier (e.g. 'bronze.kitchen_telemetry')."""
    return _get_catalog().load_table(identifier)


def write_to_iceberg(table: Table, df: pd.DataFrame) -> None:
    """Append a pandas DataFrame to an Iceberg table using PyArrow."""
    import pyarrow as pa

    arrow_table = pa.Table.from_pandas(df, schema=table.schema().as_arrow())
    table.append(arrow_table)
    log.info("Wrote %d rows to %s", len(df), table.name())
