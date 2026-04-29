from __future__ import annotations

from typing import Any, Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.expectations.expectation import QueryExpectation
from great_expectations.expectations.metrics.query_metrics.query_table import QueryTable


class ExpectRevenueToMatchItems(QueryExpectation):
    """Expect that gross_revenue in silver_orders matches sum(item_revenue)
    in silver_order_items within a configurable tolerance.

    This is a cross-table validation that catches discrepancies between
    the order header and its exploded line items — a critical data integrity check.

    Args:
        tolerance_pct: Maximum allowed fraction of orders with revenue mismatch.
                       Defaults to 0.001 (0.1%).
    """

    metric_dependencies = ("query.table",)

    query = """
        WITH order_totals AS (
            SELECT
                so.order_id,
                so.gross_revenue,
                COALESCE(SUM(oi.item_revenue), 0) AS items_total
            FROM silver.silver_orders so
            LEFT JOIN silver.silver_order_items oi
                ON so.order_id = oi.order_id
            WHERE so.gross_revenue > 0
            GROUP BY so.order_id, so.gross_revenue
        ),
        mismatches AS (
            SELECT
                order_id,
                gross_revenue,
                items_total,
                ABS(gross_revenue - items_total) / NULLIF(gross_revenue, 0) AS pct_diff
            FROM order_totals
            WHERE ABS(gross_revenue - items_total) / NULLIF(gross_revenue, 0) > {tolerance}
        )
        SELECT
            COUNT(*) AS mismatch_count,
            (SELECT COUNT(*) FROM order_totals) AS total_orders
        FROM mismatches
    """

    success_keys = ("tolerance_pct", "query")

    default_kwarg_values = {
        "tolerance_pct": 0.001,
        "catch_exceptions": False,
        "result_format": "BASIC",
    }

    examples = [
        {
            "data": [
                {
                    "schema": {
                        "fields": [
                            {"name": "mismatch_count", "type": "integer"},
                            {"name": "total_orders",   "type": "integer"},
                        ]
                    },
                    "data": {"mismatch_count": [0], "total_orders": [100]},
                }
            ],
            "tests": [
                {
                    "title": "no_mismatches_passes",
                    "exact_match_out": False,
                    "in": {"tolerance_pct": 0.001},
                    "out": {"success": True},
                }
            ],
        }
    ]

    library_metadata = {
        "maturity":       "experimental",
        "tags":           ["revenue", "cross-table", "custom", "data-integrity"],
        "contributors":   ["data-team"],
        "package":        "streamflow_expectations",
    }

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict[str, Any],
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[SqlAlchemyExecutionEngine] = None,
    ) -> dict:
        tolerance_pct = configuration.kwargs.get("tolerance_pct", self.default_kwarg_values["tolerance_pct"])
        query_result  = metrics.get("query.table", [{}])

        if not query_result:
            return {"success": False, "result": {"observed_value": "No query result returned"}}

        row           = query_result[0]
        mismatch_count = int(row.get("mismatch_count", 0))
        total_orders   = int(row.get("total_orders", 1))

        mismatch_rate  = mismatch_count / max(total_orders, 1)
        success        = mismatch_rate <= tolerance_pct

        return {
            "success": success,
            "result": {
                "observed_value":  mismatch_rate,
                "mismatch_count":  mismatch_count,
                "total_orders":    total_orders,
                "tolerance_pct":   tolerance_pct,
                "details": (
                    f"{mismatch_count} of {total_orders} orders "
                    f"({mismatch_rate:.4%}) have revenue mismatch > {tolerance_pct:.1%}"
                ),
            },
        }

    def get_validation_dependencies(
        self,
        configuration: ExpectationConfiguration,
        execution_engine: Optional[SqlAlchemyExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        deps = super().get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )
        tolerance = configuration.kwargs.get("tolerance_pct", 0.001)
        deps["metrics"]["query.table"].metric_kwargs["query"] = self.query.format(
            tolerance=tolerance
        )
        return deps
