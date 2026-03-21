"""Alert routing helpers (PagerDuty + Slack)."""
from __future__ import annotations

import logging

import boto3
import requests

log = logging.getLogger(__name__)


def _get_secret(name: str) -> str:
    return boto3.client("secretsmanager").get_secret_value(
        SecretId=f"restaurant/{name}"
    )["SecretString"]


def trigger_pagerduty(summary: str, severity: str, details: dict) -> None:
    """Send a PagerDuty Events API v2 alert."""
    payload = {
        "routing_key": _get_secret("pagerduty_integration_key"),
        "event_action": "trigger",
        "payload": {
            "summary": summary,
            "severity": severity,
            "source": "restaurant-data-platform",
            "custom_details": details,
        },
    }
    resp = requests.post(
        "https://events.pagerduty.com/v2/enqueue",
        json=payload,
        timeout=10,
    )
    resp.raise_for_status()
    log.warning("PagerDuty alert sent: %s", summary)


def post_slack(channel: str, message: str) -> None:
    """Post a message to a Slack channel via Incoming Webhook."""
    webhook_url = _get_secret("slack_webhook_url")
    requests.post(
        webhook_url,
        json={"channel": channel, "text": message},
        timeout=10,
    ).raise_for_status()
    log.info("Slack message sent to %s", channel)
