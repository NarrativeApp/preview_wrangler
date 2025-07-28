"""Scanner for marker files created by ml-upload-lambda."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from .s3_client import S3Client

logger = logging.getLogger(__name__)


class MarkerScanner:
    """Scans for marker files to identify projects with both preview.v1 and v3.gz data."""

    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client
        self.bucket = "prod.ml-meta-upload.getnarrativeapp.com"

    def scan_for_projects(
        self,
        hours_back: int = 24,
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> list[tuple[str, str]]:
        """
        Scan for projects that have both preview.v1 and v3.gz marker files.

        Args:
            hours_back: How many hours back to scan (default: 24)
            start_datetime: Optional start datetime (overrides hours_back)
            end_datetime: Optional end datetime (defaults to now)

        Returns:
            List of (user_id, project_id) tuples
        """
        if end_datetime is None:
            end_datetime = datetime.now(timezone.utc)

        if start_datetime is None:
            start_datetime = end_datetime - timedelta(hours=hours_back)

        # Round down to hour boundaries
        start_datetime = start_datetime.replace(minute=0, second=0, microsecond=0)
        end_datetime = end_datetime.replace(minute=0, second=0, microsecond=0)

        logger.info(f"Scanning for marker files from {start_datetime} to {end_datetime}")

        # Get all preview.v1 projects
        preview_projects = self._scan_marker_prefix("preview.v1", start_datetime, end_datetime)
        logger.info(f"Found {len(preview_projects)} projects with preview.v1 data")

        # Get all v3.gz projects
        v3_projects = self._scan_marker_prefix("v3", start_datetime, end_datetime)
        logger.info(f"Found {len(v3_projects)} projects with v3.gz data")

        # Find intersection - projects that have both
        common_projects = preview_projects.intersection(v3_projects)
        logger.info(f"Found {len(common_projects)} projects with both preview.v1 and v3.gz data")

        return sorted(common_projects)

    def _scan_marker_prefix(
        self, prefix: str, start_datetime: datetime, end_datetime: datetime
    ) -> set[tuple[str, str]]:
        """
        Scan a specific marker prefix for user/project pairs.

        Args:
            prefix: Either "preview.v1" or "v3"
            start_datetime: Start time for scanning
            end_datetime: End time for scanning

        Returns:
            Set of (user_id, project_id) tuples
        """
        projects = set()

        # Generate datetime paths to scan
        current = start_datetime
        while current <= end_datetime:
            datetime_path = current.strftime("%Y/%m/%d/%H")
            path_prefix = f"{prefix}/{datetime_path}/"

            logger.info(f"Scanning path: {path_prefix}")

            try:
                # List all objects under this hour path directly
                paginator = self.s3_client.s3.get_paginator("list_objects_v2")
                pages = paginator.paginate(Bucket=self.bucket, Prefix=path_prefix)

                for page in pages:
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            key = obj["Key"]
                            # Extract user_id and project_id from marker file path
                            # Format: prefix/YYYY/MM/DD/HH/user_id/project_id
                            parts = key.split("/")
                            if len(parts) >= 7:  # Ensure valid path structure
                                user_id = parts[5]
                                project_id = parts[6]
                                projects.add((user_id, project_id))

            except Exception as e:
                logger.warning(f"Error scanning path {path_prefix}: {e}")

            current += timedelta(hours=1)

        return projects

    def get_project_files(self, user_id: str, project_id: str) -> dict[str, str | list[str]]:
        """
        Get the actual data files for a project.

        Args:
            user_id: User ID
            project_id: Project ID

        Returns:
            Dictionary with 'ml_file' and 'preview_files' keys
        """
        result: dict[str, str | list[str]] = {
            "ml_file": f"{user_id}/{project_id}/{project_id}.v3.gz",
            "preview_files": [],
        }

        # List all files in the preview.v1 directory
        preview_prefix = f"{user_id}/{project_id}/preview.v1/"

        try:
            paginator = self.s3_client.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=preview_prefix)

            for page in pages:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        key = obj["Key"]
                        # Only include JPEG files
                        if key.lower().endswith(".jpg"):
                            preview_files = result["preview_files"]
                            assert isinstance(preview_files, list)
                            preview_files.append(key)

        except Exception as e:
            logger.error(f"Error listing preview files for {user_id}/{project_id}: {e}")

        return result
