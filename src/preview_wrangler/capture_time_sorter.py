"""
Capture time sorting using v3.gz ML upload file data.

This module renames JPEG files using metadata from project ML upload files
to create organized filenames based on camera model, serial, capture time, and image UUID.
"""

import gzip
import json
import logging
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from tqdm import tqdm

logger = logging.getLogger(__name__)


class CaptureTimeSorter:
    """Sorts/renames images using capture time data from v3.gz ML upload files."""

    def __init__(
        self,
        input_dir: Path,
        output_dir: Optional[Path] = None,
        overwrite: bool = False,
    ):
        """
        Initialize the capture time sorter.

        Args:
            input_dir: Directory containing project directories with images and v3.gz files
            output_dir: Directory to save renamed images (None = in-place)
            overwrite: Whether to overwrite existing renamed images
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir) if output_dir else None
        self.overwrite = overwrite

    def _parse_v3_file(self, v3_path: Path) -> dict[str, dict[str, Any]]:
        """
        Parse a v3.gz file to extract capture time metadata for images.

        Args:
            v3_path: Path to the v3.gz file

        Returns:
            Dictionary mapping image IDs to their metadata
        """
        metadata = {}

        try:
            with gzip.open(v3_path, "rt") as f:
                data = json.load(f)

            if "images" in data:
                for image in data["images"]:
                    if "id" in image and "meta" in image:
                        image_id = image["id"]
                        meta = image["meta"]

                        # Extract required fields
                        required_fields = ["capture_time", "model", "camera_serial"]
                        if all(field in meta for field in required_fields):
                            metadata[image_id] = {
                                "capture_time": meta["capture_time"],
                                "model": meta["model"],
                                "camera_serial": meta["camera_serial"],
                            }
                        else:
                            logger.debug(f"Missing required metadata for image {image_id}")

        except Exception as e:
            logger.error(f"Error parsing {v3_path}: {e}")

        return metadata

    def _sanitize_filename_component(self, component: str) -> str:
        """
        Sanitize a filename component by replacing problematic characters.

        Args:
            component: Component to sanitize

        Returns:
            Sanitized component safe for filenames
        """
        # Replace spaces and special characters with underscores
        sanitized = re.sub(r"[^\w\-.]", "_", component)
        # Remove multiple consecutive underscores
        sanitized = re.sub(r"_+", "_", sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip("_")
        return sanitized

    def _format_capture_time(self, capture_time: str) -> str:
        """
        Format capture time from ISO format to YYYYMMDD_HHMMSS.

        Args:
            capture_time: ISO format timestamp (e.g., "2025-07-12T02:32:56")

        Returns:
            Formatted timestamp (e.g., "20250712_023256")
        """
        try:
            # Parse ISO format timestamp
            dt = datetime.fromisoformat(capture_time.replace("Z", "+00:00"))
            return dt.strftime("%Y%m%d_%H%M%S")
        except Exception as e:
            logger.warning(f"Error formatting capture time '{capture_time}': {e}")
            # Return sanitized original as fallback
            return self._sanitize_filename_component(capture_time)

    def _generate_new_filename(self, image_id: str, metadata: dict[str, Any]) -> str:
        """
        Generate new filename from metadata.

        Args:
            image_id: Image UUID
            metadata: Metadata dictionary with model, camera_serial, capture_time

        Returns:
            New filename in format: model_serial_time_uuid.jpg
        """
        model = self._sanitize_filename_component(metadata["model"])
        serial = self._sanitize_filename_component(metadata["camera_serial"])
        formatted_time = self._format_capture_time(metadata["capture_time"])

        return f"{model}_{serial}_{formatted_time}_{image_id}.jpg"

    def _get_image_path(self, project_dir: Path, image_id: str) -> Optional[Path]:
        """
        Get JPEG file path from image ID.

        Args:
            project_dir: Project directory to search
            image_id: Image ID from v3 file (matches JPEG filename)

        Returns:
            Path to JPEG file, or None if not found
        """
        # Try common JPEG extensions
        for ext in [".jpg", ".jpeg", ".JPG", ".JPEG"]:
            jpeg_path = project_dir / f"{image_id}{ext}"
            if jpeg_path.exists():
                return jpeg_path

        return None

    def _get_output_path(self, input_path: Path, new_filename: str) -> Path:
        """
        Get the output path for a renamed image.

        Args:
            input_path: Input image path
            new_filename: New filename to use

        Returns:
            Output path for the renamed image
        """
        if self.output_dir:
            # Maintain directory structure in output
            relative_path = input_path.relative_to(self.input_dir)
            return self.output_dir / relative_path.parent / new_filename
        else:
            # In-place renaming
            return input_path.parent / new_filename

    def _rename_image(self, input_path: Path, output_path: Path) -> bool:
        """
        Rename/copy an image file.

        Args:
            input_path: Input image path
            output_path: Output image path

        Returns:
            True if file was renamed/copied, False if skipped
        """
        # Check if already exists and not overwriting
        if output_path.exists() and not self.overwrite:
            logger.debug(f"Skipping {input_path.name} - output already exists")
            return False

        # Skip if input and output are the same
        if str(input_path) == str(output_path):
            logger.debug(f"Skipping {input_path.name} - already has correct name")
            return False

        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if self.output_dir:
                # Copy to output directory
                shutil.copy2(input_path, output_path)
            else:
                # In-place rename
                input_path.rename(output_path)

            logger.debug(f"Renamed {input_path.name} -> {output_path.name}")
            return True

        except Exception as e:
            logger.error(f"Error renaming {input_path}: {e}")
            raise

    def process_projects(self) -> dict[str, Any]:
        """
        Process all projects in the input directory.

        Returns:
            Summary statistics of the sorting process
        """
        # Find all project directories (those containing .v3.gz files)
        project_dirs = []
        for item in self.input_dir.iterdir():
            if item.is_dir():
                # Filter out macOS metadata files (._*) and get only real v3.gz files
                v3_files = [f for f in item.glob("*.v3.gz") if not f.name.startswith("._")]
                if v3_files:
                    project_dirs.append(item)

        if not project_dirs:
            logger.info("No project directories with v3.gz files found")
            return {
                "total_projects": 0,
                "total_images": 0,
                "renamed": 0,
                "skipped": 0,
                "errors": 0,
            }

        logger.info(f"Found {len(project_dirs)} project directories")

        # Process each project
        total_stats: dict[str, Any] = {
            "total_projects": len(project_dirs),
            "total_images": 0,
            "renamed": 0,
            "skipped": 0,
            "errors": 0,
        }

        for project_dir in tqdm(project_dirs, desc="Processing projects"):
            try:
                project_stats = self._process_single_project(project_dir)

                # Aggregate stats
                total_stats["total_images"] += project_stats["total_images"]
                total_stats["renamed"] += project_stats["renamed"]
                total_stats["skipped"] += project_stats["skipped"]
                total_stats["errors"] += project_stats["errors"]

            except Exception as e:
                logger.error(f"Error processing project {project_dir.name}: {e}")
                total_stats["errors"] += 1

        # Log summary
        logger.info("\nCapture Time Sorting Summary:")
        logger.info(f"  Total projects: {total_stats['total_projects']}")
        logger.info(f"  Total images: {total_stats['total_images']}")
        logger.info(f"  Images renamed: {total_stats['renamed']}")
        logger.info(f"  Images skipped: {total_stats['skipped']}")
        logger.info(f"  Errors: {total_stats['errors']}")

        return total_stats

    def _process_single_project(self, project_dir: Path) -> dict[str, Any]:
        """
        Process a single project directory.

        Args:
            project_dir: Project directory path

        Returns:
            Statistics for this project
        """
        # Find the v3.gz file (exclude macOS metadata files)
        v3_files = [f for f in project_dir.glob("*.v3.gz") if not f.name.startswith("._")]
        if not v3_files:
            return {
                "total_images": 0,
                "renamed": 0,
                "skipped": 0,
                "errors": 0,
            }

        v3_file = v3_files[0]  # Should only be one per project

        # Parse metadata
        metadata_map = self._parse_v3_file(v3_file)
        if not metadata_map:
            logger.debug(f"No capture time metadata found in {v3_file}")
            return {
                "total_images": 0,
                "renamed": 0,
                "skipped": 0,
                "errors": 0,
            }

        # Find and process images
        stats: dict[str, Any] = {
            "total_images": 0,
            "renamed": 0,
            "skipped": 0,
            "errors": 0,
        }

        for image_id, metadata in metadata_map.items():
            # Get the corresponding JPEG file path
            jpeg_path = self._get_image_path(project_dir, image_id)
            if not jpeg_path:
                logger.debug(f"JPEG not found for image ID {image_id}")
                continue

            stats["total_images"] += 1

            try:
                # Generate new filename
                new_filename = self._generate_new_filename(image_id, metadata)

                # Get output path
                output_path = self._get_output_path(jpeg_path, new_filename)

                # Rename image
                was_renamed = self._rename_image(jpeg_path, output_path)

                # Update stats
                if was_renamed:
                    stats["renamed"] += 1
                else:
                    stats["skipped"] += 1

            except Exception as e:
                logger.error(f"Error processing {jpeg_path}: {e}")
                stats["errors"] += 1

        return stats


def capture_time_sort(input_dir: str, output_dir: Optional[str] = None, overwrite: bool = False):
    """
    Main function to sort images by capture time using v3.gz data.

    Args:
        input_dir: Directory containing project directories
        output_dir: Output directory (None = in-place)
        overwrite: Overwrite existing files
    """
    # Create sorter and process images
    sorter = CaptureTimeSorter(
        input_dir=Path(input_dir),
        output_dir=Path(output_dir) if output_dir else None,
        overwrite=overwrite,
    )

    stats = sorter.process_projects()

    # Save statistics
    stats_file = Path.home() / ".preview_wrangler" / "capture_time_sort_stats.json"
    stats_file.parent.mkdir(parents=True, exist_ok=True)
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)

    logger.info(f"Statistics saved to {stats_file}")
    return stats
