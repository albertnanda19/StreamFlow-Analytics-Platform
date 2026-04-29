from __future__ import annotations

import re
from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

_UUID4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


class ColumnValuesUuidV4(ColumnMapMetricProvider):
    condition_metric_name = "column_values.valid_uuid"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.astype(str).str.match(_UUID4_RE)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        import sqlalchemy as sa
        pattern = (
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}"
            r"-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
        )
        return sa.func.regexp_matches(column.cast(sa.String), pattern)


class ExpectColumnValuesToBeValidUuid(ColumnMapExpectation):
    """Expect every value in a column to be a valid UUID v4 string.

    The check validates the canonical UUID4 format:
    xxxxxxxx-xxxx-4xxx-[89ab]xxx-xxxxxxxxxxxx

    Args:
        column: Column name to validate.

    Keyword Args:
        mostly: Minimum fraction of values that must pass (0-1). Defaults to 1.0.
    """

    map_metric = "column_values.valid_uuid"

    success_keys = ("mostly",)

    default_kwarg_values = {
        "mostly": 1.0,
        "row_condition": None,
        "condition_parser": None,
    }

    examples = [
        {
            "data": {
                "valid_uuids": [
                    "a2f3e4b5-1234-4abc-89ab-000000000000",
                    "00000000-0000-4000-8000-000000000000",
                ],
                "invalid_uuids": [
                    "not-a-uuid",
                    "00000000-0000-1000-8000-000000000000",
                ],
            },
            "tests": [
                {
                    "title": "valid_uuid_passes",
                    "exact_match_out": False,
                    "in": {"column": "valid_uuids"},
                    "out": {"success": True},
                },
                {
                    "title": "invalid_uuid_fails",
                    "exact_match_out": False,
                    "in": {"column": "invalid_uuids"},
                    "out": {"success": False},
                },
            ],
        }
    ]

    library_metadata = {
        "maturity":       "experimental",
        "tags":           ["uuid", "format", "custom"],
        "contributors":   ["data-team"],
        "package":        "streamflow_expectations",
    }

    @classmethod
    def _get_supported_validations(cls):
        return ["column_map_expectation"]
