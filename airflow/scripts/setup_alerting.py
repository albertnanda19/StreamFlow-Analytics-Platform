import logging
import os

import requests

logger = logging.getLogger(__name__)

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")


def send_pipeline_alert(
    dag_id: str,
    task_id: str,
    error_message: str,
    severity: str = "ERROR",
) -> None:
    icon = {"ERROR": "🔴", "WARN": "🟡", "INFO": "🟢"}.get(severity, "⚪")
    text = (
        f"{icon} *StreamFlow Alert* [{severity}]\n"
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Message*: {error_message}"
    )

    logger.warning("ALERT [%s] %s/%s: %s", severity, dag_id, task_id, error_message)

    if not SLACK_WEBHOOK:
        logger.info("SLACK_WEBHOOK_URL not set — alert logged only")
        return

    try:
        resp = requests.post(SLACK_WEBHOOK, json={"text": text}, timeout=10)
        if resp.status_code != 200:
            logger.error("Slack webhook returned %s: %s", resp.status_code, resp.text)
    except Exception as exc:
        logger.error("Failed to send Slack alert: %s", exc)


def on_failure_callback(context: dict) -> None:
    dag_id   = context["dag"].dag_id
    task_id  = context["task"].task_id
    exc      = context.get("exception", "Unknown error")
    send_pipeline_alert(dag_id, task_id, str(exc), severity="ERROR")


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    task_ids = ", ".join(str(t.task_id) for t in task_list)
    send_pipeline_alert(
        dag.dag_id,
        task_ids,
        f"SLA missed for tasks: {task_ids}",
        severity="WARN",
    )
