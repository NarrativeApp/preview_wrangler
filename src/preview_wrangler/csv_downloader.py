"""CSV file downloader with caching support."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

from tqdm import tqdm

from .cache import CacheManager
from .inventory import InventoryFile, InventoryManifest
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class CSVDownloader:
    """Downloads inventory CSV files with caching."""
    
    MAX_WORKERS = 10  # Maximum concurrent downloads
    
    def __init__(self, s3_client: S3Client, cache_manager: CacheManager):
        """Initialize CSV downloader.
        
        Args:
            s3_client: S3 client instance
            cache_manager: Cache manager instance
        """
        self.s3_client = s3_client
        self.cache_manager = cache_manager
        self.csv_cache_dir = cache_manager.get_csv_cache_dir()
    
    def download_csv_files(self, manifest: InventoryManifest) -> List[Path]:
        """Download all CSV files from manifest.
        
        Args:
            manifest: Inventory manifest
            
        Returns:
            List of paths to decompressed CSV files
        """
        csv_paths = []
        
        logger.info(f"Downloading {len(manifest.files)} CSV files (up to {self.MAX_WORKERS} concurrent)")
        
        # Use ThreadPoolExecutor for concurrent downloads
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            # Submit all download tasks
            future_to_file = {
                executor.submit(self._download_single_csv, file_info): file_info
                for file_info in manifest.files
            }
            
            # Process completed downloads with progress bar
            with tqdm(total=len(manifest.files), desc="Downloading CSV files") as pbar:
                for future in as_completed(future_to_file):
                    file_info = future_to_file[future]
                    try:
                        csv_path = future.result()
                        if csv_path:
                            csv_paths.append(csv_path)
                    except Exception as e:
                        logger.error(f"Failed to download {file_info.key}: {e}")
                    finally:
                        pbar.update(1)
        
        logger.info(f"Downloaded {len(csv_paths)} CSV files")
        return csv_paths
    
    def _download_single_csv(self, file_info: InventoryFile) -> Optional[Path]:
        """Download and decompress a single CSV file.
        
        Args:
            file_info: File information from manifest
            
        Returns:
            Path to decompressed CSV file or None if failed
        """
        # Check if already cached
        if self.cache_manager.is_cached(file_info.key, file_info.md5_checksum):
            cached_path = self.cache_manager.get_cached_path(file_info.key)
            logger.debug(f"Using cached file: {cached_path}")
            
            # Return decompressed path
            csv_name = Path(file_info.key).stem  # Remove .gz
            decompressed_path = self.csv_cache_dir / csv_name
            if decompressed_path.exists():
                return decompressed_path
            else:
                # Decompress if needed
                return self.cache_manager.decompress_gzip(cached_path, decompressed_path)
        
        try:
            # Download file
            filename = Path(file_info.key).name
            gzip_path = self.csv_cache_dir / filename
            
            logger.debug(f"Downloading {file_info.key} ({file_info.size:,} bytes)")
            
            # Use inventory bucket
            bucket = "prod.ml-meta-upload.getnarrativeapp.com-inventory"
            self.s3_client.download_file(bucket, file_info.key, str(gzip_path))
            
            # Verify checksum
            actual_checksum = self.cache_manager.calculate_md5(gzip_path)
            if actual_checksum != file_info.md5_checksum:
                logger.error(
                    f"Checksum mismatch for {file_info.key}: "
                    f"expected {file_info.md5_checksum}, got {actual_checksum}"
                )
                gzip_path.unlink()
                raise ValueError("Checksum verification failed")
            
            # Add to cache
            self.cache_manager.add_to_cache(
                file_info.key, gzip_path, file_info.md5_checksum
            )
            
            # Decompress
            csv_name = Path(file_info.key).stem  # Remove .gz
            decompressed_path = self.csv_cache_dir / csv_name
            
            return self.cache_manager.decompress_gzip(gzip_path, decompressed_path)
            
        except Exception as e:
            logger.error(f"Error downloading {file_info.key}: {e}")
            raise