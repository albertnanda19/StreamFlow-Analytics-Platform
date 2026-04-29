import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from botocore.client import Config

logger = logging.getLogger(__name__)


class DeltaFreshnessSensor(BaseSensorOperator):
    template_fields: tuple = ("s3_path", "max_age_minutes")

    def __init__(
        self,
        s3_path:         str,
        max_age_minutes: int = 120,
        bucket:          Optional[str] = None,
        prefix:          Optional[str] = None,
        minio_endpoint:  str = "",
        aws_key:         str = "",
        aws_secret:      str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_path         = s3_path
        self.max_age_minutes = max_age_minutes
        self.minio_endpoint  = minio_endpoint or os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.aws_key         = aws_key        or os.getenv("AWS_ACCESS_KEY_ID", "streamflow")
        self.aws_secret      = aws_secret     or os.getenv("AWS_SECRET_ACCESS_KEY", "streamflow123")

        path = s3_path.lstrip("s3a://").lstrip("s3://")
        parts = path.split("/", 1)
        self.bucket = bucket or parts[0]
        self.prefix = prefix or (parts[1] if len(parts) > 1 else "")

    def _get_s3_client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.minio_endpoint,
            aws_access_key_id=self.aws_key,
            aws_secret_access_key=self.aws_secret,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )

    def poke(self, context: Context) -> bool:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(minutes=self.max_age_minutes)
        s3     = self._get_s3_client()

        try:
            resp = s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
            objects = resp.get("Contents", [])
            fresh   = [
                o for o in objects
                if o["LastModified"] >= cutoff
            ]

            logger.info(
                "DeltaFreshnessSensor: %s — %d objects found, %d fresh (cutoff: %s)",
                self.s3_path, len(objects), len(fresh), cutoff.isoformat(),
            )

            if fresh:
                context["ti"].xcom_push(
                    key="fresh_object_count",
                    value=len(fresh),
                )
                return True
            return False
        except Exception as exc:
            logger.error("DeltaFreshnessSensor error for %s: %s", self.s3_path, exc)
            return False
