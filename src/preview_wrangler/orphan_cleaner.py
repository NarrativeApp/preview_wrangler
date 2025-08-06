"""Clean up orphaned preview data using inventory files to cross-check against marker files."""

import csv
import logging
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from .cache import CacheManager
from .csv_downloader import CSVDownloader
from .inventory import InventoryManager
from .marker_scanner import MarkerScanner
from .s3_client import S3Client

logger = logging.getLogger(__name__)


def _process_csv_for_projects(csv_path: Path) -> set[tuple[str, str]]:
    """Process a single CSV file to extract preview directory projects.

    Args:
        csv_path: Path to CSV file

    Returns:
        Set of (user_id, project_id) tuples for projects with preview data
    """
    preview_pattern = re.compile(r"^([a-f0-9-]{36})/([a-f0-9-]{36})/preview\.v1/")
    preview_projects = set()

    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.reader(f)

            for row in reader:
                if len(row) < 2:
                    continue

                key = row[1]  # Key is second column

                # Check for preview directory
                match = preview_pattern.match(key)
                if match:
                    user_uuid = match.group(1)
                    project_uuid = match.group(2)
                    preview_projects.add((user_uuid, project_uuid))

    except Exception as e:
        logger.error(f"Error processing {csv_path}: {e}")

    return preview_projects


def _process_csv_for_all_projects(
    args: tuple[Path, set[tuple[str, str]], Optional[datetime], Optional[datetime]],
) -> dict[str, list[tuple[str, int, str]]]:
    """Process a single CSV file to extract preview files and sizes for ALL orphaned projects.

    Much more efficient than processing each project individually - scans each CSV once
    and collects files for all orphaned projects in a single pass.

    Args:
        args: Tuple of (csv_path, orphaned_projects_set, start_datetime, end_datetime)

    Returns:
        Dictionary mapping project paths to lists of (file_path, file_size, last_modified) tuples found in this CSV
    """
    csv_path, orphaned_projects_set, start_datetime, end_datetime = args
    project_files = {}

    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.reader(f)

            for row in reader:
                if len(row) < 4:  # Need bucket, key, size, and last_modified columns
                    continue

                key = row[1]  # Key is second column
                try:
                    size = int(row[2])  # Size is third column
                except (ValueError, IndexError):
                    size = 0  # Default to 0 if size can't be parsed

                last_modified = (
                    row[3] if len(row) > 3 else "unknown"
                )  # Last modified is fourth column

                # Only process preview files
                if "/preview.v1/" in key:
                    # Extract user_id/project_id from path
                    parts = key.split("/")
                    if len(parts) >= 3:
                        user_id, project_id = parts[0], parts[1]

                        # Fast O(1) lookup: is this an orphaned project?
                        if (user_id, project_id) in orphaned_projects_set:
                            # Filter by date range if specified
                            if start_datetime is not None and end_datetime is not None:
                                try:
                                    # Parse the last_modified timestamp
                                    file_datetime = datetime.fromisoformat(
                                        last_modified.replace("Z", "+00:00")
                                    )
                                    if not (start_datetime <= file_datetime <= end_datetime):
                                        continue  # Skip files outside date range
                                except (ValueError, AttributeError):
                                    # Skip files with invalid timestamps when date filtering is active
                                    continue

                            project_path = f"{user_id}/{project_id}"
                            if project_path not in project_files:
                                project_files[project_path] = []
                            project_files[project_path].append((key, size, last_modified))

    except Exception as e:
        logger.debug(f"Error processing {csv_path} for all projects: {e}")

    return project_files


class OrphanCleaner:
    """Identifies and removes preview data found in inventory that doesn't have corresponding marker files."""

    MAX_WORKERS = 8  # Process multiple CSV files concurrently

    def __init__(self, s3_client: S3Client, cache_manager: CacheManager):
        self.s3_client = s3_client
        self.cache_manager = cache_manager
        self.bucket = "prod.ml-meta-upload.getnarrativeapp.com"
        self.marker_scanner = MarkerScanner(s3_client)
        self.inventory_manager = InventoryManager(s3_client)
        self.csv_downloader = CSVDownloader(s3_client, cache_manager)

    def find_orphaned_data(
        self,
        days_back: int = 7,  # Default to 7 days
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
        return_non_orphaned: bool = False,
    ) -> tuple[dict[str, list[tuple[str, str]]], int, Optional[dict[str, list[tuple[str, str]]]]]:
        """
        Find all preview data in inventory that doesn't have corresponding marker files.

        Args:
            days_back: How many days back to scan for valid markers (default: 7)
            start_datetime: Optional start datetime (overrides days_back)
            end_datetime: Optional end datetime (defaults to now)
            return_non_orphaned: If True, also return non-orphaned files for analysis

        Returns:
            Tuple of (orphaned_files_dict, total_size_bytes, non_orphaned_files_dict)
            - orphaned_files_dict: Dictionary mapping project paths to lists of (file_path, last_modified) tuples
            - total_size_bytes: Total size of all orphaned files
            - non_orphaned_files_dict: Optional dictionary of non-orphaned files (if return_non_orphaned=True)
        """
        if end_datetime is None:
            end_datetime = datetime.now(timezone.utc)

        if start_datetime is None:
            start_datetime = end_datetime - timedelta(days=days_back)

        logger.info(f"Scanning for valid marker files from {start_datetime} to {end_datetime}")

        # Step 1: Get all projects with valid marker files
        valid_projects = set(
            self.marker_scanner.scan_for_projects(
                start_datetime=start_datetime, end_datetime=end_datetime
            )
        )
        logger.info(f"Found {len(valid_projects)} projects with valid marker files")

        # Step 2: Download and parse inventory to find ALL preview data
        logger.info("Downloading latest S3 inventory...")
        manifest = self.inventory_manager.get_latest_manifest()
        logger.info(f"Inventory created at: {manifest.creation_date}")

        # Download CSV files
        csv_paths = self.csv_downloader.download_csv_files(manifest)

        # Parse CSV files to find all preview directories using concurrent processing
        logger.info(
            f"Parsing inventory files to find all preview data using {self.MAX_WORKERS} workers..."
        )
        all_inventory_projects = self._parse_csv_files_fast(csv_paths)
        logger.info(
            f"Found {len(all_inventory_projects)} total projects with preview data in inventory"
        )

        # Step 3: Find orphaned projects (in inventory but not in markers)
        orphaned_projects = all_inventory_projects - valid_projects
        non_orphaned_projects = all_inventory_projects.intersection(valid_projects)
        logger.info(f"Found {len(orphaned_projects)} orphaned projects")
        logger.info(
            f"Found {len(non_orphaned_projects)} non-orphaned projects (have valid markers)"
        )

        # Step 4: Collect all files from orphaned projects using concurrent processing
        orphaned_files, orphaned_size = self._get_project_files_fast(
            list(orphaned_projects), csv_paths, start_datetime, end_datetime
        )

        # Step 5: Optionally collect non-orphaned files for analysis
        non_orphaned_files = None
        if return_non_orphaned and non_orphaned_projects:
            non_orphaned_files, _ = self._get_project_files_fast(
                list(non_orphaned_projects), csv_paths, start_datetime, end_datetime
            )

        return orphaned_files, orphaned_size, non_orphaned_files

    def _parse_csv_files_fast(self, csv_paths: list[Path]) -> set[tuple[str, str]]:
        """Parse CSV files to find all preview projects using concurrent processing.

        Args:
            csv_paths: List of CSV file paths

        Returns:
            Set of (user_id, project_id) tuples for all projects with preview data
        """
        all_projects = set()

        # Process CSV files concurrently
        with ProcessPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_csv = {
                executor.submit(_process_csv_for_projects, csv_path): csv_path
                for csv_path in csv_paths
            }

            # Process results as they complete
            with tqdm(total=len(csv_paths), desc="Processing CSV files") as pbar:
                for future in as_completed(future_to_csv):
                    csv_path = future_to_csv[future]
                    try:
                        projects = future.result()
                        all_projects.update(projects)
                    except Exception as e:
                        logger.error(f"Failed to process {csv_path}: {e}")
                    finally:
                        pbar.update(1)

        return all_projects

    def _get_project_files_fast(
        self,
        orphaned_projects: list[tuple[str, str]],
        csv_paths: list[Path],
        start_datetime: Optional[datetime] = None,
        end_datetime: Optional[datetime] = None,
    ) -> tuple[dict[str, list[tuple[str, str]]], int]:
        """Get preview files for orphaned projects using optimized single-pass processing.

        Much faster than the previous approach - scans each CSV once instead of once per project.

        Args:
            orphaned_projects: List of (user_id, project_id) tuples
            csv_paths: List of CSV file paths
            start_datetime: Optional start datetime for filtering files
            end_datetime: Optional end datetime for filtering files

        Returns:
            Tuple of (project_files_dict, total_size_bytes)
            - project_files_dict: Dictionary mapping project paths to lists of (file_path, last_modified) tuples
            - total_size_bytes: Total size of all files that would be deleted
        """
        # Convert to set for O(1) lookups
        orphaned_projects_set = set(orphaned_projects)

        if start_datetime and end_datetime:
            logger.info(
                f"Collecting PREVIEW files for {len(orphaned_projects)} projects from {len(csv_paths)} CSV files (date filtered: {start_datetime.date()} to {end_datetime.date()})..."
            )
        else:
            logger.info(
                f"Collecting PREVIEW files for {len(orphaned_projects)} projects from {len(csv_paths)} CSV files (optimized single-pass)..."
            )

        all_project_files = {}
        total_size = 0

        # Process each CSV once to collect files for ALL orphaned projects
        with ProcessPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            # Submit one task per CSV file (not per project!)
            future_to_csv = {
                executor.submit(
                    _process_csv_for_all_projects,
                    (csv_path, orphaned_projects_set, start_datetime, end_datetime),
                ): csv_path
                for csv_path in csv_paths
            }

            # Process results as they complete
            with tqdm(total=len(csv_paths), desc="Processing CSV files") as pbar:
                for future in as_completed(future_to_csv):
                    csv_path = future_to_csv[future]

                    try:
                        csv_project_files = future.result()

                        # Merge results from this CSV into the overall results
                        for project_path, file_data in csv_project_files.items():
                            if project_path not in all_project_files:
                                all_project_files[project_path] = []

                            # Extract file paths and dates, accumulate sizes
                            for file_path, file_size, last_modified in file_data:
                                all_project_files[project_path].append((file_path, last_modified))
                                total_size += file_size

                    except Exception as e:
                        logger.error(f"Failed to process {csv_path}: {e}")
                    finally:
                        pbar.update(1)

        logger.info(f"Found files for {len(all_project_files)} orphaned projects")
        logger.info(f"Total size of orphaned preview files: {total_size / (1024**3):.2f} GB")
        return all_project_files, total_size

    def delete_orphaned_data(
        self,
        orphaned_files: dict[str, list[tuple[str, str]]],
        dry_run: bool = True,
        batch_size: int = 1000,
    ) -> tuple[int, int]:
        """
        Delete orphaned PREVIEW files from S3 (preserves ML upload files).

        Args:
            orphaned_files: Dictionary mapping project paths to lists of (file_path, last_modified) tuples
            dry_run: If True, don't actually delete, just report what would be deleted
            batch_size: Number of files to delete in each batch

        Returns:
            Tuple of (projects_deleted, files_deleted)
        """
        total_projects = len(orphaned_files)
        total_files = sum(len(files) for files in orphaned_files.values())

        if dry_run:
            logger.info(f"DRY RUN: Would delete {total_files} files from {total_projects} projects")
            return total_projects, total_files

        logger.info(f"Deleting {total_files} files from {total_projects} projects...")

        files_deleted = 0
        projects_deleted = len(orphaned_files)

        # Collect all preview files from all projects into a single list
        all_preview_files = []
        total_filtered = 0

        for project_path, files in orphaned_files.items():
            # Safety check: only delete preview files - extract file paths from tuples
            preview_files = [
                file_path
                for file_path, _ in files
                if "/preview.v1/" in file_path and not file_path.endswith(".v3.gz")
            ]

            filtered_count = len(files) - len(preview_files)
            if filtered_count > 0:
                total_filtered += filtered_count
                logger.debug(f"Filtered out {filtered_count} non-preview files from {project_path}")

            all_preview_files.extend(preview_files)

        if total_filtered > 0:
            logger.warning(f"Filtered out {total_filtered} non-preview files total")

        logger.info(f"Deleting {len(all_preview_files)} files in batches of {batch_size}...")

        # Delete all files in batches across all projects (much more efficient)
        with tqdm(total=len(all_preview_files), desc="Deleting files", unit="files") as pbar:
            for i in range(0, len(all_preview_files), batch_size):
                batch = all_preview_files[i : i + batch_size]
                delete_objects = [{"Key": key} for key in batch]

                try:
                    response = self.s3_client.s3.delete_objects(
                        Bucket=self.bucket, Delete={"Objects": delete_objects, "Quiet": True}
                    )

                    # Check for errors
                    if "Errors" in response:
                        for error in response["Errors"]:
                            logger.error(
                                f"Error deleting {error['Key']}: {error['Code']} - {error['Message']}"
                            )
                        # Count successful deletes (total - errors)
                        successful_deletes = len(batch) - len(response["Errors"])
                    else:
                        successful_deletes = len(batch)

                    files_deleted += successful_deletes
                    pbar.update(len(batch))

                except Exception as e:
                    logger.error(f"Error deleting batch starting at index {i}: {e}")
                    pbar.update(len(batch))

        logger.info(f"Deleted {files_deleted} files from {projects_deleted} projects")
        return projects_deleted, files_deleted

    def generate_report(
        self, orphaned_files: dict[str, list[tuple[str, str]]], output_file: Optional[Path] = None
    ) -> str:
        """
        Generate a detailed report of orphaned data.

        Args:
            orphaned_files: Dictionary mapping project paths to lists of (file_path, last_modified) tuples
            output_file: Optional file path to save the report

        Returns:
            Report as a string
        """
        report_lines = ["# Orphaned Preview Data Report", ""]

        total_projects = len(orphaned_files)
        total_files = sum(len(files) for files in orphaned_files.values())

        report_lines.append(f"Total orphaned projects: {total_projects}")
        report_lines.append(f"Total orphaned files: {total_files}")
        report_lines.append("")
        report_lines.append("## Projects and Files")
        report_lines.append("")

        for project_path in sorted(orphaned_files.keys()):
            files = orphaned_files[project_path]
            report_lines.append(f"### {project_path}")
            report_lines.append(f"  Files: {len(files)}")

            # Group files by type - extract file paths from tuples
            preview_files = [file_path for file_path, _ in files if "/preview.v1/" in file_path]
            v3_files = [file_path for file_path, _ in files if file_path.endswith(".v3.gz")]
            other_files = [
                file_path
                for file_path, _ in files
                if file_path not in preview_files and file_path not in v3_files
            ]

            if preview_files:
                report_lines.append(f"  - Preview files: {len(preview_files)}")
            if v3_files:
                report_lines.append(f"  - V3 files: {len(v3_files)}")
            if other_files:
                report_lines.append(f"  - Other files: {len(other_files)}")

            report_lines.append("")

        report = "\n".join(report_lines)

        if output_file:
            output_file.write_text(report)
            logger.info(f"Report saved to {output_file}")

        return report
