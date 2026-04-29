import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("run_checkpoints")

GX_ROOT     = Path(__file__).parent
PG_HOST     = os.getenv("POSTGRES_HOST",      "localhost")
PG_PORT     = os.getenv("POSTGRES_PORT",      "5432")
PG_USER     = os.getenv("POSTGRES_USER",      "streamflow")
PG_PASS     = os.getenv("POSTGRES_PASSWORD",  "streamflow123")
PG_DB       = os.getenv("POSTGRES_SOURCE_DB", "streamflow_source")

CHECKPOINTS = {
    "bronze": "bronze_checkpoint",
    "silver": "silver_checkpoint",
    "gold":   "gold_checkpoint",
}


def _get_context():
    import great_expectations as gx
    return gx.get_context(context_root_dir=str(GX_ROOT))


def _parse_results(checkpoint_result) -> dict:
    total = passed = failed = 0
    suite_results = []

    for key, vr in checkpoint_result.run_results.items():
        stats     = vr["validation_result"]["statistics"]
        ev_count  = stats.get("evaluated_expectations", 0)
        pass_count = stats.get("successful_expectations", 0)
        fail_count = stats.get("unsuccessful_expectations", 0)
        total  += ev_count
        passed += pass_count
        failed += fail_count

        suite_results.append({
            "suite_name":           str(key.expectation_suite_identifier),
            "expectations_total":   ev_count,
            "expectations_passed":  pass_count,
            "expectations_failed":  fail_count,
            "success":              fail_count == 0,
        })

    return {
        "success":              failed == 0,
        "expectations_total":   total,
        "expectations_passed":  passed,
        "expectations_failed":  failed,
        "success_pct":          round(passed / max(total, 1) * 100, 2),
        "suite_results":        suite_results,
        "run_time":             datetime.utcnow().isoformat(),
    }


def _persist_results(checkpoint_name: str, results: dict) -> None:
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT,
            user=PG_USER, password=PG_PASS, dbname=PG_DB,
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS gx_validation_results (
                id                   SERIAL PRIMARY KEY,
                run_time             TIMESTAMP DEFAULT NOW(),
                checkpoint_name      VARCHAR(100),
                suite_name           VARCHAR(100),
                table_name           VARCHAR(100),
                expectations_total   INTEGER,
                expectations_passed  INTEGER,
                expectations_failed  INTEGER,
                success_pct          FLOAT,
                run_id               VARCHAR(100)
            )
        """)
        run_id = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        for sr in results["suite_results"]:
            cur.execute("""
                INSERT INTO gx_validation_results
                    (checkpoint_name, suite_name, expectations_total,
                     expectations_passed, expectations_failed, success_pct, run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                checkpoint_name,
                sr["suite_name"],
                sr["expectations_total"],
                sr["expectations_passed"],
                sr["expectations_failed"],
                round(sr["expectations_passed"] / max(sr["expectations_total"], 1) * 100, 2),
                run_id,
            ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Results persisted to gx_validation_results for run_id=%s", run_id)
    except Exception as exc:
        logger.error("Failed to persist validation results: %s", exc)


def run_checkpoint(checkpoint_alias: str) -> dict:
    checkpoint_name = CHECKPOINTS.get(checkpoint_alias, checkpoint_alias)
    logger.info("Running checkpoint: %s", checkpoint_name)

    context = _get_context()
    result  = context.run_checkpoint(checkpoint_name=checkpoint_name)
    parsed  = _parse_results(result)
    parsed["checkpoint_name"] = checkpoint_name
    return parsed


def _print_summary(results: dict, output_format: str) -> None:
    if output_format == "json":
        print(json.dumps(results, indent=2))
        return

    sep = "=" * 60
    status = "✅ PASSED" if results["success"] else "❌ FAILED"
    print(f"\n{sep}")
    print(f"  Checkpoint: {results.get('checkpoint_name', '?')}")
    print(f"  Status:     {status}")
    print(f"  Total:      {results['expectations_total']}")
    print(f"  Passed:     {results['expectations_passed']}")
    print(f"  Failed:     {results['expectations_failed']}")
    print(f"  Success %:  {results['success_pct']}%")
    print(sep)

    for sr in results.get("suite_results", []):
        icon = "✅" if sr["success"] else "❌"
        print(f"  {icon} {sr['suite_name']}: {sr['expectations_passed']}/{sr['expectations_total']}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run GX checkpoints for StreamFlow")
    parser.add_argument(
        "--checkpoint",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Which checkpoint to run",
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit code 1 if any expectation fails",
    )
    parser.add_argument(
        "--output-format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )

    args          = parser.parse_args()
    any_failed    = False
    to_run        = list(CHECKPOINTS.keys()) if args.checkpoint == "all" else [args.checkpoint]

    for alias in to_run:
        try:
            results = run_checkpoint(alias)
            _print_summary(results, args.output_format)
            _persist_results(CHECKPOINTS[alias], results)
            if not results["success"]:
                any_failed = True
        except Exception as exc:
            logger.error("Checkpoint '%s' raised an exception: %s", alias, exc)
            any_failed = True

    if args.fail_on_error and any_failed:
        logger.error("One or more checkpoints failed — exiting with code 1")
        sys.exit(1)


if __name__ == "__main__":
    main()
