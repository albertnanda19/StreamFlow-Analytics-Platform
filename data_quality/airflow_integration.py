import logging
import sys
from pathlib import Path

GX_ROOT = Path(__file__).parent
sys.path.insert(0, str(GX_ROOT))

from run_checkpoints import run_checkpoint

logger = logging.getLogger(__name__)

_CHECKPOINT_MAP = {
    "bronze": "bronze",
    "silver": "silver",
    "gold":   "gold",
}


def _execute(alias: str, context: dict) -> dict:
    try:
        results = run_checkpoint(alias)
        failed  = results["expectations_failed"]
        total   = results["expectations_total"]
        stats   = {
            "checkpoint":           alias,
            "success":              results["success"],
            "expectations_total":   total,
            "expectations_passed":  results["expectations_passed"],
            "expectations_failed":  failed,
            "success_pct":          results["success_pct"],
        }
        context["ti"].xcom_push(key=f"gx_{alias}_results", value=stats)

        if not results["success"]:
            from airflow.exceptions import AirflowException
            raise AirflowException(
                f"GX {alias} checkpoint failed: "
                f"{failed}/{total} expectations failed "
                f"({results['success_pct']}% pass rate)"
            )
        return stats
    except ImportError:
        logger.error("Run inside Airflow context to push XComs")
        raise


def run_bronze_quality_check(**context) -> dict:
    return _execute("bronze", context)


def run_silver_quality_check(**context) -> dict:
    return _execute("silver", context)


def run_gold_quality_check(**context) -> dict:
    return _execute("gold", context)


def run_all_quality_checks(**context) -> dict:
    results = {}
    any_failed = False
    for alias in ("bronze", "silver", "gold"):
        try:
            r = _execute(alias, context)
            results[alias] = r
        except Exception as exc:
            logger.error("GX check failed for %s: %s", alias, exc)
            results[alias] = {"success": False, "error": str(exc)}
            any_failed = True

    context["ti"].xcom_push(key="gx_all_results", value=results)

    if any_failed:
        from airflow.exceptions import AirflowException
        raise AirflowException("One or more GX checkpoints failed — see XCom for details")

    return results
