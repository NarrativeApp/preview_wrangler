"""
Script to detect and correct image rotations for downloaded preview images.

This module processes images that don't have EXIF rotation data and uses
a CNN model to detect and correct rotations of 90, 180, or 270 degrees.
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
import json
from PIL import Image
from PIL.ExifTags import TAGS
from tqdm import tqdm
import click

from .rotation_detector import RotationDetector, SimpleRotationDetector
from .rotation_detector_pretrained import PretrainedRotationDetector, SimpleCheckOrientationDetector

logger = logging.getLogger(__name__)


def has_exif_rotation(image_path: Path) -> bool:
    """
    Check if an image has EXIF rotation data.
    
    Args:
        image_path: Path to the image file
        
    Returns:
        True if image has EXIF orientation data, False otherwise
    """
    try:
        with Image.open(image_path) as img:
            exif = img.getexif()
            if exif:
                for tag_id, value in exif.items():
                    tag = TAGS.get(tag_id, tag_id)
                    if tag == 'Orientation' and value != 1:
                        return True
        return False
    except Exception as e:
        logger.debug(f"Error reading EXIF from {image_path}: {e}")
        return False


def find_images_without_rotation_data(base_dir: Path) -> List[Path]:
    """
    Find all JPEG images that don't have EXIF rotation data.
    
    Args:
        base_dir: Base directory to search for images
        
    Returns:
        List of image paths without EXIF rotation data
    """
    images_without_rotation = []
    
    # Find all JPEG files
    jpeg_patterns = ['*.jpg', '*.jpeg', '*.JPG', '*.JPEG']
    all_images = []
    
    for pattern in jpeg_patterns:
        all_images.extend(base_dir.rglob(pattern))
    
    logger.info(f"Found {len(all_images)} total images")
    
    # Check each image for EXIF rotation data
    for image_path in tqdm(all_images, desc="Checking EXIF data"):
        if not has_exif_rotation(image_path):
            images_without_rotation.append(image_path)
    
    logger.info(f"Found {len(images_without_rotation)} images without EXIF rotation data")
    return images_without_rotation


class RotationCorrector:
    """Manages the rotation correction process for multiple images."""
    
    def __init__(self, 
                 input_dir: Path,
                 output_dir: Optional[Path] = None,
                 overwrite: bool = False,
                 use_simple_detector: bool = False,
                 model_path: Optional[Path] = None):
        """
        Initialize the rotation corrector.
        
        Args:
            input_dir: Directory containing images to process
            output_dir: Directory to save corrected images (None = in-place)
            overwrite: Whether to overwrite existing files
            use_simple_detector: Use simple heuristics instead of CNN
            model_path: Path to pre-trained model weights
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir) if output_dir else None
        self.overwrite = overwrite
        
        # Initialize detector
        if use_simple_detector:
            logger.info("Using simple rotation detector (heuristics-based)")
            self.detector = SimpleRotationDetector()
        else:
            # Use pre-trained model
            logger.info("Using pre-trained check-orientation model")
            try:
                self.detector = SimpleCheckOrientationDetector()
            except Exception as e:
                logger.warning(f"Failed to load check-orientation model: {e}")
                logger.info("Falling back to PretrainedRotationDetector")
                self.detector = PretrainedRotationDetector()
        
        # Cache for tracking processed images
        cache_dir = Path.home() / ".preview_wrangler" / "rotation_cache"
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.processed_cache_file = self.cache_dir / "processed_images.json"
        self._load_processed_cache()
    
    def _load_processed_cache(self):
        """Load cache of processed images."""
        if self.processed_cache_file.exists():
            with open(self.processed_cache_file, 'r') as f:
                self.processed_cache = json.load(f)
        else:
            self.processed_cache = {}
    
    def _save_processed_cache(self):
        """Save cache of processed images."""
        with open(self.processed_cache_file, 'w') as f:
            json.dump(self.processed_cache, f, indent=2)
    
    def _get_cache_key(self, image_path: Path) -> str:
        """Generate cache key for an image."""
        return f"rotation_{image_path.stat().st_mtime}_{image_path.stat().st_size}_{image_path.name}"
    
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
    
    def process_images(self, 
                      images: Optional[List[Path]] = None,
                      batch_size: int = 32) -> Dict[str, Any]:
        """
        Process images to detect and correct rotations.
        
        Args:
            images: List of image paths (None = find all without EXIF)
            batch_size: Number of images to process in batch
            
        Returns:
            Summary statistics of the correction process
        """
        # Find images to process
        if images is None:
            images = find_images_without_rotation_data(self.input_dir)
        
        if not images:
            logger.info("No images to process")
            return {"total": 0, "corrected": 0, "skipped": 0, "errors": 0}
        
        # Filter out already processed images (if not overwriting)
        if not self.overwrite:
            images_to_process = []
            for img in images:
                cache_key = self._get_cache_key(img)
                if cache_key not in self.processed_cache:
                    images_to_process.append(img)
                else:
                    logger.debug(f"Skipping already processed: {img.name}")
            
            logger.info(f"Processing {len(images_to_process)} new images "
                       f"({len(images) - len(images_to_process)} already processed)")
            images = images_to_process
        
        if not images:
            return {"total": 0, "corrected": 0, "skipped": 0, "errors": 0}
        
        # Process images
        stats = {
            "total": len(images),
            "corrected": 0,
            "skipped": 0,
            "errors": 0,
            "corrections_by_angle": {0: 0, 90: 0, 180: 0, 270: 0}
        }
        
        # Use batch processing if detector supports it
        if hasattr(self.detector, 'process_batch') and batch_size > 1:
            # Batch prediction
            logger.info(f"Processing {len(images)} images in batches of {batch_size}")
            predictions = self.detector.process_batch(images, batch_size)
            
            # Apply corrections
            for img_path, predicted_angle in tqdm(
                zip(images, predictions), 
                total=len(images),
                desc="Correcting rotations"
            ):
                try:
                    output_path = self._get_output_path(img_path)
                    
                    # Apply correction
                    angle = self.detector.correct_rotation(
                        img_path, output_path, predicted_angle
                    )
                    
                    # Update stats
                    stats["corrections_by_angle"][angle] += 1
                    if angle != 0:
                        stats["corrected"] += 1
                    else:
                        stats["skipped"] += 1
                    
                    # Cache the result
                    cache_key = self._get_cache_key(img_path)
                    self.processed_cache[cache_key] = {
                        "angle": angle,
                        "output_path": str(output_path)
                    }
                    
                except Exception as e:
                    logger.error(f"Error processing {img_path}: {e}")
                    stats["errors"] += 1
        else:
            # Individual processing
            for img_path in tqdm(images, desc="Processing images"):
                try:
                    output_path = self._get_output_path(img_path)
                    
                    # Detect and correct rotation
                    angle = self.detector.correct_rotation(img_path, output_path)
                    
                    # Update stats
                    stats["corrections_by_angle"][angle] += 1
                    if angle != 0:
                        stats["corrected"] += 1
                    else:
                        stats["skipped"] += 1
                    
                    # Cache the result
                    cache_key = self._get_cache_key(img_path)
                    self.processed_cache[cache_key] = {
                        "angle": angle,
                        "output_path": str(output_path)
                    }
                    
                except Exception as e:
                    logger.error(f"Error processing {img_path}: {e}")
                    stats["errors"] += 1
        
        # Save cache after processing
        self._save_processed_cache()
        
        # Log summary
        logger.info(f"\nRotation Correction Summary:")
        logger.info(f"  Total images: {stats['total']}")
        logger.info(f"  Corrected: {stats['corrected']}")
        logger.info(f"  Already correct: {stats['skipped']}")
        logger.info(f"  Errors: {stats['errors']}")
        logger.info(f"  Corrections by angle:")
        for angle, count in stats['corrections_by_angle'].items():
            if count > 0:
                logger.info(f"    {angle}°: {count}")
        
        return stats


def correct_rotations(input_dir: str,
                     output_dir: Optional[str] = None,
                     overwrite: bool = False,
                     batch_size: int = 32,
                     use_simple: bool = False,
                     model_path: Optional[str] = None):
    """
    Main function to correct image rotations.
    
    Args:
        input_dir: Directory containing images
        output_dir: Output directory (None = in-place)
        overwrite: Overwrite existing files
        batch_size: Batch size for processing
        use_simple: Use simple detector instead of CNN
        model_path: Path to model weights
    """
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create corrector and process images
    corrector = RotationCorrector(
        input_dir=Path(input_dir),
        output_dir=Path(output_dir) if output_dir else None,
        overwrite=overwrite,
        use_simple_detector=use_simple,
        model_path=Path(model_path) if model_path else None
    )
    
    stats = corrector.process_images(batch_size=batch_size)
    
    # Save statistics
    stats_file = Path.home() / ".preview_wrangler" / "rotation_stats.json"
    stats_file.parent.mkdir(parents=True, exist_ok=True)
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    
    logger.info(f"Statistics saved to {stats_file}")


if __name__ == "__main__":
    # Example usage
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m preview_wrangler.correct_rotations <input_dir> [output_dir]")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    correct_rotations(input_dir, output_dir)