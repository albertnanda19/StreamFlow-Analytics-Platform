import logging
import re
from typing import Optional, Sequence

from airflow.operators.bash import BashOperator
from airflow.utils.context import Context

logger = logging.getLogger(__name__)

_RESULT_RE = re.compile(
    r"Done\. (\d+) models? (\w+), (\d+) warnings?, (\d+) errors?",
    re.IGNORECASE,
)
_WARN_RE  = re.compile(r"Warning in model (\S+)")
_ERROR_RE = re.compile(r"Compilation Error in model (\S+)")


class DbtRunOperator(BashOperator):
    def __init__(
        self,
        *,
        dbt_command:   str = "run",
        select:        Optional[str] = None,
        exclude:       Optional[str] = None,
        target:        str = "prod",
        full_refresh:  bool = False,
        dbt_project_dir: str = "/opt/airflow/dbt_project",
        profiles_dir:    str = "/opt/airflow/dbt_project",
        **kwargs,
    ) -> None:
        self.dbt_command      = dbt_command
        self.select           = select
        self.exclude          = exclude
        self.target           = target
        self.full_refresh     = full_refresh
        self.dbt_project_dir  = dbt_project_dir
        self.profiles_dir     = profiles_dir

        cmd = self._build_command()
        super().__init__(bash_command=cmd, **kwargs)

    def _build_command(self) -> str:
        parts = [
            f"cd {self.dbt_project_dir}",
            f"&& dbt {self.dbt_command}",
            f"--target {self.target}",
            f"--profiles-dir {self.profiles_dir}",
        ]
        if self.select:
            parts.append(f"--select {self.select}")
        if self.exclude:
            parts.append(f"--exclude {self.exclude}")
        if self.full_refresh and self.dbt_command == "run":
            parts.append("--full-refresh")
        parts.append("--no-write-json")
        return " ".join(parts)

    def execute(self, context: Context):
        output = super().execute(context)

        if not isinstance(output, str):
            return output

        parsed = {
            "models_run": 0,
            "warnings":   0,
            "errors":     0,
            "warn_models":  [],
            "error_models": [],
        }

        m = _RESULT_RE.search(output)
        if m:
            parsed["models_run"] = int(m.group(1))
            parsed["warnings"]   = int(m.group(3))
            parsed["errors"]     = int(m.group(4))

        parsed["warn_models"]  = _WARN_RE.findall(output)
        parsed["error_models"] = _ERROR_RE.findall(output)

        context["ti"].xcom_push(key="dbt_results", value=parsed)
        logger.info(
            "dbt %s complete: %d models, %d warnings, %d errors",
            self.dbt_command, parsed["models_run"], parsed["warnings"], parsed["errors"],
        )

        if parsed["errors"] > 0:
            raise RuntimeError(f"dbt reported {parsed['errors']} error(s): {parsed['error_models']}")

        return parsed
