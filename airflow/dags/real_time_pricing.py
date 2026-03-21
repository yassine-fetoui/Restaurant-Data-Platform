"""
real_time_pricing.py
━━━━━━━━━━━━━━━━━━━
Every-15-minute pricing adjustment based on live queue depth and inventory.
Runs during service hours only (10 AM – 11 PM).

Business logic:
  - Queue > 15 min  → surge +10 % on delivery apps
  - Bestseller < 10 units → mark item 86 (auto-remove from menus)
  - Recovery: re-list item once stock > par_level * 0.4
"""
from __future__ import annotations

import logging
from datetime import timedelta

import requests
from airflow.decorators import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import datetime

from utils.iceberg_helpers import get_iceberg_table
from utils.alert_handlers import post_slack

log = logging.getLogger(__name__)


@dag(
    dag_id="real_time_pricing",
    schedule="*/15 10-23 * * *",   # every 15 min, 10 AM–11 PM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=3,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=12),
    },
    tags=["restaurant", "pricing", "real-time"],
    doc_md=__doc__,
)
def realtime_pricing_dag() -> None:

    @task(task_id="fetch_live_queue_depths")
    def fetch_queue_depths() -> dict:
        """Poll POS APIs for current order queue at each active location."""
        locations = _get_active_locations()
        queue_by_location: dict[str, float] = {}

        for loc in locations:
            resp = requests.get(
                f"https://api.toasttab.com/orders/v2/queue/{loc}",
                headers={"Authorization": f"Bearer {_get_secret('toast_api_key')}"},
                timeout=10,
            )
            resp.raise_for_status()
            queue_by_location[loc] = resp.json().get("estimatedWaitMinutes", 0)

        log.info("Queue depths fetched for %d locations", len(queue_by_location))
        return queue_by_location

    @task(task_id="fetch_real_time_inventory")
    def fetch_inventory() -> dict:
        """Read latest inventory snapshot from Iceberg silver layer."""
        import pandas as pd

        table = get_iceberg_table("silver.inventory_levels")
        df = (
            table.scan(row_filter=f"inventory_date = '{pd.Timestamp.today().date()}'")
            .to_pandas()
        )
        # Return {location_id: {item_id: current_qty}}
        result: dict = {}
        for _, row in df.iterrows():
            result.setdefault(row["location_id"], {})[row["menu_item_id"]] = row["current_quantity"]
        return result

    @task(task_id="adjust_pricing_and_menu")
    def adjust_pricing_and_menu(queue_depths: dict, inventory: dict) -> None:
        """Apply surge pricing or item-86 based on live signals."""
        active_locations = _get_active_locations()

        for location_id in active_locations:
            wait_minutes = queue_depths.get(location_id, 0)
            loc_inventory = inventory.get(location_id, {})

            # ── Surge pricing ─────────────────────────────────────────────────
            if wait_minutes > 15:
                _set_delivery_multiplier(location_id, multiplier=1.10)
                log.info("Surge pricing applied at %s (wait=%.0f min)", location_id, wait_minutes)
            else:
                _set_delivery_multiplier(location_id, multiplier=1.00)

            # ── 86 items running critically low ───────────────────────────────
            for item_id, qty in loc_inventory.items():
                par = _get_par_level(location_id, item_id)
                if qty < 10:
                    _mark_item_86(location_id, item_id)
                    post_slack(
                        channel="#kitchen-ops",
                        message=f"⚠️ 86'd: item `{item_id}` at `{location_id}` (qty={qty})",
                    )
                elif qty > par * 0.4:
                    _restore_item(location_id, item_id)

    # ── Wire up ───────────────────────────────────────────────────────────────
    queues    = fetch_queue_depths()
    inventory = fetch_inventory()
    adjust_pricing_and_menu(queues, inventory)


realtime_dag = realtime_pricing_dag()


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_secret(name: str) -> str:
    import boto3
    return boto3.client("secretsmanager").get_secret_value(
        SecretId=f"restaurant/{name}"
    )["SecretString"]


def _get_active_locations() -> list[str]:
    import boto3, json
    ssm = boto3.client("ssm")
    raw = ssm.get_parameter(Name="/restaurant/active_locations")["Parameter"]["Value"]
    return json.loads(raw)


def _set_delivery_multiplier(location_id: str, multiplier: float) -> None:
    for platform_url, secret_key in [
        ("https://api.doordash.com/v2/pricing", "doordash_api_key"),
        ("https://api.uber.com/v1/eats/store/pricing", "ubereats_api_key"),
    ]:
        requests.post(
            platform_url,
            json={"storeId": location_id, "multiplier": multiplier},
            headers={"Authorization": f"Bearer {_get_secret(secret_key)}"},
            timeout=10,
        ).raise_for_status()


def _mark_item_86(location_id: str, item_id: str) -> None:
    requests.patch(
        f"https://api.toasttab.com/config/v2/locations/{location_id}/menu/items/{item_id}",
        json={"available": False},
        headers={"Authorization": f"Bearer {_get_secret('toast_api_key')}"},
        timeout=10,
    ).raise_for_status()


def _restore_item(location_id: str, item_id: str) -> None:
    requests.patch(
        f"https://api.toasttab.com/config/v2/locations/{location_id}/menu/items/{item_id}",
        json={"available": True},
        headers={"Authorization": f"Bearer {_get_secret('toast_api_key')}"},
        timeout=10,
    ).raise_for_status()


def _get_par_level(location_id: str, item_id: str) -> float:
    # Simplified: in production this would be cached from Snowflake reference table
    return 50.0
