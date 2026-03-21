"""
restaurant_daily_operations.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Daily data pipeline for multi-location restaurant operations.

Schedule : 06:00 AM daily (before lunch prep)
SLA      : Must complete by 09:00 AM
Owner    : data-engineering

Bronze → Silver → Gold transformation covering:
  - POS ingestion (Toast/Square) → Iceberg
  - Inventory reconciliation
  - Kitchen prep list generation
  - Dynamic pricing update
  - Automated supplier purchase orders
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta

import pandas as pd
import requests
from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime

from utils.iceberg_helpers import get_iceberg_table, write_to_iceberg
from utils.snowflake_helpers import get_snowflake_conn
from utils.alert_handlers import trigger_pagerduty, post_slack

log = logging.getLogger(__name__)

# ─── Dataset declarations (Airflow data-aware scheduling) ─────────────────────
inventory_updates = Dataset("s3://restaurant-iceberg-prod/silver/inventory/")
demand_forecast   = Dataset("s3://restaurant-iceberg-prod/gold/demand_forecast/")


# ─── DAG definition ───────────────────────────────────────────────────────────
@dag(
    dag_id="restaurant_daily_operations",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True,
        "email": ["data-engineering@restaurant.com"],
        "sla": timedelta(hours=3),
    },
    params={
        "locations": Param(
            default=["all"],
            type="array",
            description="Location IDs to process, or ['all'] for every location.",
        ),
        "simulate": Param(
            default=False,
            type="boolean",
            description="Dry-run mode: skip external API writes.",
        ),
    },
    tags=["restaurant", "bronze", "silver", "gold", "daily"],
    doc_md=__doc__,
)
def restaurant_daily_pipeline() -> None:

    # ── Bronze layer ──────────────────────────────────────────────────────────
    @task_group(group_id="bronze_ingestion")
    def bronze_ingestion() -> None:

        @task(task_id="ingest_pos_transactions")
        def ingest_pos(**context: dict) -> dict:
            """Pull overnight orders from Toast API → write to Iceberg bronze."""
            locations: list[str] = context["params"]["locations"]
            table = get_iceberg_table("bronze.pos_transactions")

            records_total = 0
            for location_id in locations:
                resp = requests.get(
                    "https://api.toasttab.com/orders/v2/orders",
                    headers={"Authorization": f"Bearer {_get_secret('toast_api_key')}"},
                    params={"restaurantGuid": location_id, "startDate": "{{ ds }}"},
                    timeout=30,
                )
                resp.raise_for_status()
                orders = resp.json().get("orders", [])
                if not orders:
                    continue

                df = pd.DataFrame(orders)
                # PCI-DSS: tokenise payment data immediately, never persist raw PAN
                df["payment_token"] = df["payment_data"].apply(_tokenise_pan)
                df.drop(columns=["payment_data"], inplace=True)
                df["_ingested_at"] = pd.Timestamp.utcnow()

                write_to_iceberg(table, df)
                records_total += len(df)
                log.info("Ingested %d orders for location %s", len(df), location_id)

            return {"records_ingested": records_total}

        @task(task_id="compact_kitchen_telemetry")
        def compact_kitchen_telemetry() -> None:
            """Merge small IoT files written by Kafka consumers."""
            from pyiceberg.expressions import GreaterThan
            from pyiceberg.table.rewrite import rewrite_data_files

            table = get_iceberg_table("bronze.kitchen_telemetry")
            rewrite_data_files(
                table,
                target_file_size_bytes=128 * 1024 * 1024,  # 128 MB
                filter=GreaterThan(
                    "timestamp",
                    (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).isoformat(),
                ),
            )
            log.info("Compaction complete for kitchen_telemetry")

        ingest_pos() >> compact_kitchen_telemetry()

    # ── Silver layer ──────────────────────────────────────────────────────────
    @task_group(group_id="silver_transformations")
    def silver_transformations() -> None:

        reconcile_inventory = SnowflakeOperator(
            task_id="reconcile_inventory_levels",
            snowflake_conn_id="snowflake_restaurant",
            outlets=[inventory_updates],
            sql="""
            MERGE INTO silver.inventory_levels AS tgt
            USING (
                SELECT
                    location_id,
                    menu_item_id,
                    SUM(quantity_sold)   AS used_qty,
                    DATE(order_time)     AS usage_date
                FROM bronze.pos_transactions
                WHERE DATE(order_time) = '{{ ds }}'
                GROUP BY location_id, menu_item_id, DATE(order_time)
            ) AS src
            ON  tgt.location_id   = src.location_id
            AND tgt.menu_item_id  = src.menu_item_id
            AND tgt.inventory_date = src.usage_date
            WHEN MATCHED THEN
                UPDATE SET current_quantity = current_quantity - src.used_qty,
                           updated_at       = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (location_id, menu_item_id, inventory_date, current_quantity)
                SELECT  src.location_id,
                        src.menu_item_id,
                        src.usage_date,
                        mi.par_level - src.used_qty
                FROM menu_items mi
                WHERE mi.id = src.menu_item_id;
            """,
        )

        @task(task_id="anonymise_customer_pii")
        def anonymise_customer_pii() -> None:
            """Apply Presidio anonymisation to silver customer_profiles."""
            from presidio_analyzer import AnalyzerEngine
            from presidio_anonymizer import AnonymizerEngine

            analyzer  = AnalyzerEngine()
            anonymizer = AnonymizerEngine()
            table = get_iceberg_table("silver.customer_profiles")
            df = table.scan().to_pandas()

            for col in ["customer_name", "phone", "email"]:
                df[col] = df[col].apply(
                    lambda v: anonymizer.anonymize(
                        text=v,
                        analyzer_results=analyzer.analyze(text=v, language="en"),
                    ).text
                )

            table.overwrite(df)
            log.info("PII anonymisation complete — %d rows processed", len(df))

        reconcile_inventory >> anonymise_customer_pii()

    # ── Gold layer ────────────────────────────────────────────────────────────
    @task_group(group_id="gold_outputs")
    def gold_outputs() -> None:

        @task(task_id="calculate_dynamic_pricing", outlets=[demand_forecast])
        def calculate_dynamic_pricing(**context: dict) -> None:
            """ML-driven pricing multipliers pushed to delivery platforms."""
            import tensorflow as tf  # type: ignore[import]

            model = tf.keras.models.load_model(
                "s3://restaurant-ml-models/demand_forecaster/latest/"
            )
            features = _get_location_features("{{ ds }}")
            predicted_demand = model.predict(features)

            pricing_rows = []
            for location_id, demand in zip(features["location_id"], predicted_demand):
                if demand > 1.5:
                    multiplier = 1.15   # surge +15 %
                elif demand < 0.7:
                    multiplier = 0.90   # off-peak -10 %
                else:
                    multiplier = 1.00

                pricing_rows.append(
                    {
                        "location_id": location_id,
                        "date": "{{ ds }}",
                        "demand_multiplier": multiplier,
                        "created_at": pd.Timestamp.utcnow(),
                    }
                )

            write_to_iceberg(
                get_iceberg_table("gold.dynamic_pricing"),
                pd.DataFrame(pricing_rows),
            )

            if not context["params"]["simulate"]:
                _push_pricing_to_apis(pricing_rows)

        generate_prep_lists = SnowflakeOperator(
            task_id="generate_prep_lists",
            snowflake_conn_id="snowflake_restaurant",
            sql="""
            CREATE OR REPLACE TABLE gold.prep_lists AS
            SELECT
                i.location_id,
                i.menu_item_id,
                m.menu_item_name,
                GREATEST(0, f.predicted_orders - i.current_quantity) AS prep_quantity,
                m.prep_time_minutes,
                CASE
                    WHEN f.predicted_orders > i.current_quantity * 1.5 THEN 'URGENT'
                    WHEN f.predicted_orders > i.current_quantity        THEN 'STANDARD'
                    ELSE 'MINIMAL'
                END AS priority,
                CURRENT_TIMESTAMP() AS generated_at
            FROM silver.inventory_levels       i
            JOIN gold.demand_forecast          f  USING (location_id, menu_item_id)
            JOIN reference.menu_items          m  ON m.id = i.menu_item_id
            WHERE i.inventory_date = '{{ ds }}'
              AND f.meal_period    = 'lunch';
            """,
        )

        @task(task_id="auto_generate_purchase_orders")
        def auto_generate_purchase_orders() -> None:
            """Create supplier POs for items below 30 % of par level."""
            low_stock = _get_low_stock_items(threshold_pct=0.30)
            for item in low_stock:
                _create_purchase_order(
                    supplier_id=item["supplier_id"],
                    item_id=item["menu_item_id"],
                    quantity=item["par_level"] - item["current_quantity"],
                    required_by="{{ tomorrow_ds }} 10:00:00",
                )
                log.info("PO raised for item %s at location %s", item["menu_item_id"], item["location_id"])

        calculate_dynamic_pricing() >> generate_prep_lists >> auto_generate_purchase_orders()

    # ── Food safety (parallel, highest priority) ───────────────────────────────
    @task(task_id="food_safety_check")
    def food_safety_check() -> None:
        """Alert on temperature violations and lock affected equipment."""
        violations = _get_temperature_violations(since_minutes=60)
        for v in violations:
            if v["severity"] == "critical":
                trigger_pagerduty(
                    summary=f"CRITICAL: Food safety violation at {v['location_id']}",
                    severity="critical",
                    details=v,
                )
                _lock_equipment_in_pos(v["location_id"], v["equipment_id"])
                log.warning("Food safety alert raised for %s", v)

    # ── Wire up ───────────────────────────────────────────────────────────────
    bronze = bronze_ingestion()
    silver = silver_transformations()
    gold   = gold_outputs()
    safety = food_safety_check()

    bronze >> silver >> gold
    safety  # Runs in parallel — independent of ETL SLA


# ─── Instantiate ──────────────────────────────────────────────────────────────
restaurant_dag = restaurant_daily_pipeline()


# ─── Private helpers (would live in utils/ in production) ─────────────────────

def _get_secret(name: str) -> str:
    import boto3
    client = boto3.client("secretsmanager")
    return client.get_secret_value(SecretId=f"restaurant/{name}")["SecretString"]


def _tokenise_pan(payment_data: dict) -> str:
    """Replace raw PAN with a Stripe/Braintree vault token."""
    # Implementation calls tokenisation service — PAN never touches our storage
    return payment_data.get("token", "tok_unknown")


def _get_location_features(ds: str) -> pd.DataFrame:
    with get_snowflake_conn() as conn:
        return pd.read_sql(
            "SELECT * FROM gold.location_features WHERE feature_date = %(ds)s",
            conn,
            params={"ds": ds},
        )


def _push_pricing_to_apis(pricing: list[dict]) -> None:
    for row in pricing:
        requests.post(
            "https://api.doordash.com/v2/pricing",
            json={"storeId": row["location_id"], "multiplier": row["demand_multiplier"]},
            headers={"Authorization": f"Bearer {_get_secret('doordash_api_key')}"},
            timeout=10,
        ).raise_for_status()


def _get_temperature_violations(since_minutes: int) -> list[dict]:
    table = get_iceberg_table("bronze.kitchen_telemetry")
    cutoff = (pd.Timestamp.utcnow() - pd.Timedelta(minutes=since_minutes)).isoformat()
    df = table.scan(row_filter=f"timestamp > '{cutoff}' AND temperature_f > 40").to_pandas()
    return df.to_dict("records")


def _lock_equipment_in_pos(location_id: str, equipment_id: str) -> None:
    requests.post(
        f"https://api.toasttab.com/config/v2/locations/{location_id}/equipment/{equipment_id}/lock",
        headers={"Authorization": f"Bearer {_get_secret('toast_api_key')}"},
        timeout=10,
    ).raise_for_status()


def _get_low_stock_items(threshold_pct: float) -> list[dict]:
    with get_snowflake_conn() as conn:
        df = pd.read_sql(
            """
            SELECT location_id, menu_item_id, supplier_id, par_level, current_quantity
            FROM silver.inventory_levels
            WHERE inventory_date = CURRENT_DATE()
              AND current_quantity < par_level * %(thr)s
            """,
            conn,
            params={"thr": threshold_pct},
        )
    return df.to_dict("records")


def _create_purchase_order(
    supplier_id: str,
    item_id: str,
    quantity: float,
    required_by: str,
) -> None:
    requests.post(
        "https://api.supplier-portal.com/v1/orders",
        json={"supplierId": supplier_id, "itemId": item_id, "quantity": quantity, "requiredBy": required_by},
        headers={"Authorization": f"Bearer {_get_secret('supplier_api_key')}"},
        timeout=10,
    ).raise_for_status()
