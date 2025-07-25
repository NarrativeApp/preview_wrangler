"""CSV parser to extract preview directories."""

import csv
import logging
import re
from dataclasses import dataclass
from pathlib import Path

from tqdm import tqdm

logger = logging.getLogger(__name__)


@dataclass
class PreviewDirectory:
    """Represents a preview directory with associated project info."""

    user_uuid: str
    project_uuid: str
    preview_path: str
    ml_upload_path: str

    @property
    def output_dir(self) -> str:
        """Get output directory name for this project."""
        return self.project_uuid


class CSVParser:
    """Parses inventory CSV files to find preview directories."""

    # Pattern for preview directories: <user_uuid>/<project_uuid>/preview.v1
    PREVIEW_PATTERN = re.compile(r"^([a-f0-9-]{36})/([a-f0-9-]{36})/preview\.v1/")

    def __init__(self):
        """Initialize CSV parser."""
        self.preview_dirs: set[tuple[str, str]] = set()

    def parse_csv_files(self, csv_paths: list[Path]) -> list[PreviewDirectory]:
        """Parse all CSV files to find preview directories.

        Args:
            csv_paths: List of CSV file paths

        Returns:
            List of preview directories with ML upload files
        """
        logger.info(f"Parsing {len(csv_paths)} CSV files")

        # First pass: collect all unique preview directories
        for csv_path in tqdm(csv_paths, desc="Scanning for preview directories"):
            self._scan_csv_for_previews(csv_path)

        logger.info(f"Found {len(self.preview_dirs)} unique preview directories")

        # Second pass: check for ML upload files
        ml_files = set()
        for csv_path in tqdm(csv_paths, desc="Scanning for ML upload files"):
            ml_files.update(self._scan_csv_for_ml_files(csv_path))

        logger.info(f"Found {len(ml_files)} ML upload files")

        # Match preview directories with ML files
        qualified_previews = []
        for user_uuid, project_uuid in self.preview_dirs:
            ml_upload_path = f"{user_uuid}/{project_uuid}/{project_uuid}.v3.gz"

            if ml_upload_path in ml_files:
                preview = PreviewDirectory(
                    user_uuid=user_uuid,
                    project_uuid=project_uuid,
                    preview_path=f"{user_uuid}/{project_uuid}/preview.v1",
                    ml_upload_path=ml_upload_path,
                )
                qualified_previews.append(preview)

        logger.info(
            f"Found {len(qualified_previews)} preview directories with matching ML upload files"
        )

        return qualified_previews

    def _scan_csv_for_previews(self, csv_path: Path):
        """Scan CSV for preview directories.

        Args:
            csv_path: Path to CSV file
        """
        try:
            with open(csv_path, encoding="utf-8") as f:
                reader = csv.reader(f)

                for row in reader:
                    if len(row) < 2:  # Need at least bucket and key
                        continue

                    key = row[1]  # Key is second column
                    match = self.PREVIEW_PATTERN.match(key)

                    if match:
                        user_uuid = match.group(1)
                        project_uuid = match.group(2)
                        self.preview_dirs.add((user_uuid, project_uuid))

        except Exception as e:
            logger.error(f"Error parsing {csv_path}: {e}")

    def _scan_csv_for_ml_files(self, csv_path: Path) -> set[str]:
        """Scan CSV for ML upload files.

        Args:
            csv_path: Path to CSV file

        Returns:
            Set of ML upload file paths
        """
        ml_files = set()

        try:
            with open(csv_path, encoding="utf-8") as f:
                reader = csv.reader(f)

                for row in reader:
                    if len(row) < 2:
                        continue

                    key = row[1]
                    # Check if it's an ML upload file
                    if key.endswith(".v3.gz") and key.count("/") == 2:
                        # Verify it matches pattern: user/project/project.v3.gz
                        parts = key.split("/")
                        if len(parts) == 3:
                            user_uuid, project_uuid, filename = parts
                            expected_filename = f"{project_uuid}.v3.gz"
                            if filename == expected_filename:
                                ml_files.add(key)

        except Exception as e:
            logger.error(f"Error scanning {csv_path} for ML files: {e}")

        return ml_files
