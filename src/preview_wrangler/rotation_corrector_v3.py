"""
Rotation correction using v3.gz ML upload file data.

This module replaces the ML model-based rotation detection with more accurate
rotation data from the project ML upload files.
"""

import gzip
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Optional

from PIL import Image
from tqdm import tqdm

logger = logging.getLogger(__name__)


class V3RotationCorrector:
    """Corrects image rotations using data from v3.gz ML upload files."""

    def __init__(
        self,
        input_dir: Path,
        output_dir: Optional[Path] = None,
        overwrite: bool = False,
        max_workers: int = 4,
    ):
        """
        Initialize the rotation corrector.

        Args:
            input_dir: Directory containing project directories with images and v3.gz files
            output_dir: Directory to save corrected images (None = in-place)
            overwrite: Whether to overwrite existing corrected images
            max_workers: Number of worker threads for parallel processing
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir) if output_dir else None
        self.overwrite = overwrite
        self.max_workers = max_workers

    def _parse_v3_file(self, v3_path: Path) -> dict[str, str]:
        """
        Parse a v3.gz file to extract rotation data for images.

        Args:
            v3_path: Path to the v3.gz file

        Returns:
            Dictionary mapping image IDs to rotation values
        """
        rotation_data = {}

        try:
            with gzip.open(v3_path, "rt") as f:
                data = json.load(f)

            if "images" in data:
                for image in data["images"]:
                    if "id" in image and "meta" in image and "rotation" in image["meta"]:
                        image_id = image["id"]
                        rotation = image["meta"]["rotation"]
                        rotation_data[image_id] = rotation

        except Exception as e:
            logger.error(f"Error parsing {v3_path}: {e}")

        return rotation_data

    def _rotation_to_degrees(self, rotation: Any) -> int:
        """
        Convert rotation value to degrees for PIL rotation.

        Args:
            rotation: Rotation value from v3 file

        Returns:
            Degrees to rotate (0, 90, 180, 270)
        """
        if rotation is None or rotation == "None":
            return 0

        # Map rotation values to degrees (PIL rotates counter-clockwise)
        rotation_map = {
            "CW90": 270,  # Clockwise 90 = Counter-clockwise 270
            "CW180": 180,  # Clockwise 180 = Counter-clockwise 180
            "CW270": 90,  # Clockwise 270 = Counter-clockwise 90
            "CCW90": 90,  # Counter-clockwise 90
            "CCW180": 180,  # Counter-clockwise 180
            "CCW270": 270,  # Counter-clockwise 270
        }

        if rotation in rotation_map:
            return rotation_map[rotation]
        else:
            logger.warning(f"Unknown rotation value: {rotation}")
            return 0

    def _get_image_path(self, project_dir: Path, image_id: str) -> Optional[Path]:
        """
        Get JPEG file path from image ID (they match directly).

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

    def _apply_rotation(self, input_path: Path, output_path: Path, degrees: int) -> bool:
        """
        Apply rotation to an image.

        Args:
            input_path: Input image path
            output_path: Output image path
            degrees: Degrees to rotate (0, 90, 180, 270)

        Returns:
            True if rotation was applied, False if no rotation needed
        """
        if degrees == 0:
            # No rotation needed, copy if different paths
            if str(input_path) != str(output_path):
                output_path.parent.mkdir(parents=True, exist_ok=True)
                # For in-place, no copy needed
                if not output_path.exists():
                    import shutil

                    shutil.copy2(input_path, output_path)
            return False

        try:
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Open and rotate image
            with Image.open(input_path) as img:
                # Only convert to RGB if not already RGB/RGBA/L
                if img.mode not in ("RGB", "RGBA", "L"):
                    img = img.convert("RGB")

                # Apply rotation
                rotated = img.rotate(degrees, expand=True)

                # Save with original quality if possible
                rotated.save(output_path, quality=95, optimize=True)

            logger.debug(f"Rotated {input_path.name} by {degrees}° -> {output_path}")
            return True

        except Exception as e:
            logger.error(f"Error rotating {input_path}: {e}")
            raise

    def _get_output_path(self, input_path: Path) -> Path:
        """
        Get the output path for a corrected image.

        Args:
            input_path: Input image path

        Returns:
            Output path for the corrected image
        """
        if self.output_dir:
            # Maintain directory structure in output
            relative_path = input_path.relative_to(self.input_dir)
            return self.output_dir / relative_path
        else:
            # In-place correction
            return input_path

    def process_projects(self) -> dict[str, Any]:
        """
        Process all projects in the input directory.

        Returns:
            Summary statistics of the correction process
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
                "corrected": 0,
                "skipped": 0,
                "errors": 0,
            }

        logger.info(f"Found {len(project_dirs)} project directories")

        # Process each project
        total_stats: dict[str, Any] = {
            "total_projects": len(project_dirs),
            "total_images": 0,
            "corrected": 0,
            "skipped": 0,
            "errors": 0,
            "corrections_by_angle": {0: 0, 90: 0, 180: 0, 270: 0},
        }

        for project_dir in tqdm(project_dirs, desc="Processing projects"):
            try:
                project_stats = self._process_single_project(project_dir)

                # Aggregate stats
                total_stats["total_images"] += project_stats["total_images"]
                total_stats["corrected"] += project_stats["corrected"]
                total_stats["skipped"] += project_stats["skipped"]
                total_stats["errors"] += project_stats["errors"]

                for angle, count in project_stats["corrections_by_angle"].items():
                    total_stats["corrections_by_angle"][angle] += count

            except Exception as e:
                logger.error(f"Error processing project {project_dir.name}: {e}")
                total_stats["errors"] += 1

        # Log summary
        logger.info("\nRotation Correction Summary:")
        logger.info(f"  Total projects: {total_stats['total_projects']}")
        logger.info(f"  Total images: {total_stats['total_images']}")
        logger.info(f"  Images corrected: {total_stats['corrected']}")
        logger.info(f"  Images already correct: {total_stats['skipped']}")
        logger.info(f"  Errors: {total_stats['errors']}")
        logger.info("  Corrections by angle:")
        for angle, count in total_stats["corrections_by_angle"].items():
            if count > 0:
                logger.info(f"    {angle}°: {count}")

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
                "corrected": 0,
                "skipped": 0,
                "errors": 0,
                "corrections_by_angle": {0: 0, 90: 0, 180: 0, 270: 0},
            }

        v3_file = v3_files[0]  # Should only be one per project

        # Parse rotation data
        rotation_data = self._parse_v3_file(v3_file)
        if not rotation_data:
            logger.debug(f"No rotation data found in {v3_file}")
            return {
                "total_images": 0,
                "corrected": 0,
                "skipped": 0,
                "errors": 0,
                "corrections_by_angle": {0: 0, 90: 0, 180: 0, 270: 0},
            }

        # Find valid images
        image_tasks = []
        for image_id, rotation in rotation_data.items():
            jpeg_path = self._get_image_path(project_dir, image_id)
            if jpeg_path:
                image_tasks.append((image_id, rotation, jpeg_path))

        if not image_tasks:
            return {
                "total_images": 0,
                "corrected": 0,
                "skipped": 0,
                "errors": 0,
                "corrections_by_angle": {0: 0, 90: 0, 180: 0, 270: 0},
            }

        # Process images in parallel
        stats: dict[str, Any] = {
            "total_images": len(image_tasks),
            "corrected": 0,
            "skipped": 0,
            "errors": 0,
            "corrections_by_angle": {0: 0, 90: 0, 180: 0, 270: 0},
        }

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(executor.map(self._process_image_task, image_tasks))

        # Aggregate results
        for result in results:
            if result:
                stats["corrected"] += result["corrected"]
                stats["skipped"] += result["skipped"]
                stats["errors"] += result["errors"]
                for angle, count in result["corrections_by_angle"].items():
                    stats["corrections_by_angle"][angle] += count

        return stats

    def _process_image_task(self, task: tuple[str, str, Path]) -> dict[str, Any]:
        """
        Process a single image rotation task.

        Args:
            task: Tuple of (image_id, rotation, jpeg_path)

        Returns:
            Statistics for this image
        """
        image_id, rotation, jpeg_path = task
        result: dict[str, Any] = {
            "corrected": 0,
            "skipped": 0,
            "errors": 0,
            "corrections_by_angle": {0: 0, 90: 0, 180: 0, 270: 0},
        }

        try:
            # Convert rotation to degrees
            degrees = self._rotation_to_degrees(rotation)

            # Get output path
            output_path = self._get_output_path(jpeg_path)

            # Apply rotation
            was_rotated = self._apply_rotation(jpeg_path, output_path, degrees)

            # Update stats
            result["corrections_by_angle"][degrees] += 1
            if was_rotated:
                result["corrected"] += 1
            else:
                result["skipped"] += 1

        except Exception as e:
            logger.error(f"Error processing {jpeg_path}: {e}")
            result["errors"] += 1

        return result


def correct_rotations_v3(input_dir: str, output_dir: Optional[str] = None, overwrite: bool = False):
    """
    Main function to correct image rotations using v3.gz data.

    Args:
        input_dir: Directory containing project directories
        output_dir: Output directory (None = in-place)
        overwrite: Overwrite existing files
    """
    # Create corrector and process images
    corrector = V3RotationCorrector(
        input_dir=Path(input_dir),
        output_dir=Path(output_dir) if output_dir else None,
        overwrite=overwrite,
    )

    stats = corrector.process_projects()

    # Save statistics
    stats_file = Path.home() / ".preview_wrangler" / "rotation_stats_v3.json"
    stats_file.parent.mkdir(parents=True, exist_ok=True)
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)

    logger.info(f"Statistics saved to {stats_file}")
    return stats
