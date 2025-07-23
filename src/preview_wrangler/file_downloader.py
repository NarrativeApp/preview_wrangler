"""File downloader for JPEG and ML upload files."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

from tqdm import tqdm

from .cache import CacheManager
from .csv_parser import PreviewDirectory
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class FileDownloader:
    """Downloads JPEG and ML upload files."""
    
    BASE_BUCKET = "prod.ml-meta-upload.getnarrativeapp.com"
    JPEG_LIMIT = 20
    MAX_WORKERS = 20  # Concurrent downloads per project
    
    def __init__(
        self,
        s3_client: S3Client,
        cache_manager: CacheManager,
        output_dir: Optional[Path] = None
    ):
        """Initialize file downloader.
        
        Args:
            s3_client: S3 client instance
            cache_manager: Cache manager instance
            output_dir: Output directory for downloads
        """
        self.s3_client = s3_client
        self.cache_manager = cache_manager
        self.output_dir = output_dir or Path.cwd() / "output"
        self.output_dir.mkdir(exist_ok=True)
    
    def download_preview_files(self, preview_dirs: List[PreviewDirectory]):
        """Download files for all preview directories.
        
        Args:
            preview_dirs: List of preview directories to process
        """
        # Load progress
        progress = self.cache_manager.load_progress("preview_downloads") or {
            "completed": []
        }
        completed = set(progress["completed"])
        
        logger.info(
            f"Processing {len(preview_dirs)} preview directories "
            f"({len(completed)} already completed)"
        )
        
        for preview in tqdm(preview_dirs, desc="Downloading preview files"):
            if preview.project_uuid in completed:
                logger.debug(f"Skipping completed project {preview.project_uuid}")
                continue
            
            try:
                self._download_project_files(preview)
                
                # Update progress
                completed.add(preview.project_uuid)
                progress["completed"] = list(completed)
                self.cache_manager.save_progress("preview_downloads", progress)
                
            except Exception as e:
                logger.error(
                    f"Error processing project {preview.project_uuid}: {e}"
                )
                continue
    
    def _download_project_files(self, preview: PreviewDirectory):
        """Download files for a single project.
        
        Args:
            preview: Preview directory info
        """
        # Create project output directory
        project_dir = self.output_dir / preview.output_dir
        project_dir.mkdir(exist_ok=True)
        
        logger.info(f"Processing project {preview.project_uuid}")
        
        # Download ML upload file
        self._download_ml_file(preview, project_dir)
        
        # Download JPEG files
        self._download_jpeg_files(preview, project_dir)
    
    def _download_ml_file(self, preview: PreviewDirectory, output_dir: Path):
        """Download ML upload file.
        
        Args:
            preview: Preview directory info
            output_dir: Output directory
        """
        ml_filename = Path(preview.ml_upload_path).name
        ml_output_path = output_dir / ml_filename
        
        if ml_output_path.exists():
            logger.debug(f"ML file already exists: {ml_output_path}")
            return
        
        try:
            logger.info(f"Downloading ML file: {preview.ml_upload_path}")
            self.s3_client.download_file(
                self.BASE_BUCKET,
                preview.ml_upload_path,
                str(ml_output_path)
            )
        except Exception as e:
            logger.error(f"Failed to download ML file: {e}")
            raise
    
    def _download_jpeg_files(self, preview: PreviewDirectory, output_dir: Path):
        """Download JPEG files from preview directory.
        
        Args:
            preview: Preview directory info
            output_dir: Output directory
        """
        # List JPEG files in preview directory
        jpeg_files = self._list_jpeg_files(preview.preview_path)
        
        if not jpeg_files:
            logger.warning(f"No JPEG files found in {preview.preview_path}")
            return
        
        # Limit to 20 files
        jpeg_files = jpeg_files[:self.JPEG_LIMIT]
        
        logger.info(
            f"Downloading {len(jpeg_files)} JPEG files for "
            f"project {preview.project_uuid}"
        )
        
        # Filter out existing files
        downloads_needed = []
        for jpeg_key in jpeg_files:
            jpeg_filename = Path(jpeg_key).name
            jpeg_output_path = output_dir / jpeg_filename
            
            if jpeg_output_path.exists():
                logger.debug(f"JPEG already exists: {jpeg_output_path}")
                continue
                
            downloads_needed.append((jpeg_key, jpeg_output_path))
        
        if not downloads_needed:
            logger.debug("All JPEGs already downloaded")
            return
        
        # Download concurrently
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            # Submit all download tasks
            future_to_jpeg = {
                executor.submit(
                    self._download_single_jpeg,
                    jpeg_key,
                    output_path
                ): (jpeg_key, output_path)
                for jpeg_key, output_path in downloads_needed
            }
            
            # Wait for all downloads to complete
            for future in as_completed(future_to_jpeg):
                jpeg_key, output_path = future_to_jpeg[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Failed to download {jpeg_key}: {e}")
    
    def _download_single_jpeg(self, jpeg_key: str, output_path: Path):
        """Download a single JPEG file.
        
        Args:
            jpeg_key: S3 key for JPEG file
            output_path: Local path to save file
        """
        self.s3_client.download_file(
            self.BASE_BUCKET,
            jpeg_key,
            str(output_path)
        )
    
    def _list_jpeg_files(self, preview_path: str) -> List[str]:
        """List JPEG files in preview directory.
        
        Args:
            preview_path: Path to preview directory
            
        Returns:
            List of JPEG file keys
        """
        try:
            objects = self.s3_client.list_objects(
                self.BASE_BUCKET,
                prefix=preview_path
            )
            
            # Filter for JPEG files
            jpeg_files = [
                obj["Key"] for obj in objects
                if obj["Key"].lower().endswith((".jpg", ".jpeg"))
            ]
            
            # Sort by name for consistent ordering
            jpeg_files.sort()
            
            return jpeg_files
            
        except Exception as e:
            logger.error(f"Error listing files in {preview_path}: {e}")
            return []