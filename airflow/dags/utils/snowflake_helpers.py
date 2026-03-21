"""Shared Snowflake helpers for Airflow DAGs."""
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

import snowflake.connector
from airflow.hooks.base import BaseHook


@contextmanager
def get_snowflake_conn(conn_id: str = "snowflake_restaurant") -> Generator:
    """Context manager that yields a Snowflake connection and closes on exit."""
    airflow_conn = BaseHook.get_connection(conn_id)
    conn = snowflake.connector.connect(
        account=airflow_conn.host,
        user=airflow_conn.login,
        password=airflow_conn.password,
        database=airflow_conn.schema,
        warehouse=airflow_conn.extra_dejson.get("warehouse", "DATA_ENGINEERING_WH"),
        role=airflow_conn.extra_dejson.get("role", "DATA_ENGINEER"),
    )
    try:
        yield conn
    finally:
        conn.close()
