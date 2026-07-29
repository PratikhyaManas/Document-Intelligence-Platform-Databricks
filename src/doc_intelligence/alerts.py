"""Alerting — pushes notifications for high-severity anomalies, PII
findings, or failed data-quality checks to Slack (via incoming webhook)
and/or email. Kept dependency-free (uses `requests` + Databricks SDK's
mail helper is not required — plain SMTP-less approach via Slack is the
primary path; email uses the workspace's configured notification
destination through `dbutils` on job failure, handled separately in the
job YAML's `email_notifications` block).
"""

import json

import requests
from pyspark.sql import DataFrame


def format_anomaly_alert(rows: list[dict]) -> str:
    lines = [f"🚨 *{len(rows)} new document anomal{'y' if len(rows)==1 else 'ies'} detected*"]
    for r in rows[:20]:
        lines.append(
            f"• `{r['doc_id'][:12]}…` — *{r['anomaly_type']}* "
            f"({r['severity']}): {r.get('details', '')}"
        )
    if len(rows) > 20:
        lines.append(f"...and {len(rows) - 20} more.")
    return "\n".join(lines)


def send_slack_alert(webhook_url: str, message: str) -> bool:
    if not webhook_url:
        return False
    resp = requests.post(webhook_url, data=json.dumps({"text": message}), headers={"Content-Type": "application/json"})
    return resp.status_code == 200


def alert_on_high_severity_anomalies(
    anomalies_df: DataFrame, webhook_url: str, min_severity: str = "HIGH"
) -> int:
    severity_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
    threshold = severity_order.get(min_severity, 2)

    rows = [
        r.asDict()
        for r in anomalies_df.collect()
        if severity_order.get(r["severity"], 0) >= threshold
    ]
    if not rows:
        return 0

    message = format_anomaly_alert(rows)
    send_slack_alert(webhook_url, message)
    return len(rows)


def alert_on_pii_found(redacted_df: DataFrame, webhook_url: str) -> int:
    rows = [r.asDict() for r in redacted_df.filter("contains_pii = true").collect()]
    if not rows:
        return 0
    lines = [f"🔒 *{len(rows)} document(s) contain PII* — see gold_redacted_documents"]
    for r in rows[:20]:
        lines.append(f"• `{r['doc_id'][:12]}…` — types: {', '.join(r.get('pii_types_found') or [])}")
    send_slack_alert(webhook_url, "\n".join(lines))
    return len(rows)
