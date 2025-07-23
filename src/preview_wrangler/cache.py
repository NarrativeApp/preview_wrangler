"""Cache management for downloaded files."""

import gzip
import hashlib
import json
import logging
import shutil
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages local file cache for resumability."""
    
    def __init__(self, cache_dir: Optional[Path] = None):
        """Initialize cache manager.
        
        Args:
            cache_dir: Directory for cache storage
        """
        self.cache_dir = cache_dir or Path.home() / ".preview_wrangler" / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_file = self.cache_dir / "metadata.json"
        self._load_metadata()
    
    def _load_metadata(self):
        """Load cache metadata."""
        if self.metadata_file.exists():
            with open(self.metadata_file, "r") as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {"files": {}}
    
    def _save_metadata(self):
        """Save cache metadata."""
        with open(self.metadata_file, "w") as f:
            json.dump(self.metadata, f, indent=2)
    
    def get_csv_cache_dir(self) -> Path:
        """Get directory for CSV cache."""
        csv_dir = self.cache_dir / "csv"
        csv_dir.mkdir(exist_ok=True)
        return csv_dir
    
    def get_download_cache_dir(self) -> Path:
        """Get directory for downloaded files."""
        download_dir = self.cache_dir / "downloads"
        download_dir.mkdir(exist_ok=True)
        return download_dir
    
    def is_cached(self, key: str, checksum: Optional[str] = None) -> bool:
        """Check if file is cached.
        
        Args:
            key: S3 object key
            checksum: Optional MD5 checksum to verify
            
        Returns:
            True if file is cached and valid
        """
        if key not in self.metadata["files"]:
            return False
        
        file_info = self.metadata["files"][key]
        cache_path = Path(file_info["path"])
        
        if not cache_path.exists():
            return False
        
        # Verify checksum if provided
        if checksum and file_info.get("checksum") != checksum:
            logger.info(f"Checksum mismatch for {key}, re-downloading")
            return False
        
        return True
    
    def get_cached_path(self, key: str) -> Optional[Path]:
        """Get path to cached file.
        
        Args:
            key: S3 object key
            
        Returns:
            Path to cached file or None
        """
        if key not in self.metadata["files"]:
            return None
        
        return Path(self.metadata["files"][key]["path"])
    
    def add_to_cache(self, key: str, path: Path, checksum: Optional[str] = None):
        """Add file to cache.
        
        Args:
            key: S3 object key
            path: Local file path
            checksum: Optional MD5 checksum
        """
        self.metadata["files"][key] = {
            "path": str(path),
            "checksum": checksum
        }
        self._save_metadata()
    
    def decompress_gzip(self, gzip_path: Path, output_path: Path) -> Path:
        """Decompress gzip file.
        
        Args:
            gzip_path: Path to gzip file
            output_path: Path for decompressed file
            
        Returns:
            Path to decompressed file
        """
        logger.debug(f"Decompressing {gzip_path} to {output_path}")
        
        with gzip.open(gzip_path, "rb") as f_in:
            with open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        return output_path
    
    def calculate_md5(self, file_path: Path) -> str:
        """Calculate MD5 checksum of file.
        
        Args:
            file_path: Path to file
            
        Returns:
            MD5 checksum as hex string
        """
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def get_progress_file(self, task_name: str) -> Path:
        """Get path to progress tracking file.
        
        Args:
            task_name: Name of the task
            
        Returns:
            Path to progress file
        """
        progress_dir = self.cache_dir / "progress"
        progress_dir.mkdir(exist_ok=True)
        return progress_dir / f"{task_name}.json"
    
    def save_progress(self, task_name: str, progress_data: Dict):
        """Save task progress.
        
        Args:
            task_name: Name of the task
            progress_data: Progress data to save
        """
        progress_file = self.get_progress_file(task_name)
        with open(progress_file, "w") as f:
            json.dump(progress_data, f, indent=2)
    
    def load_progress(self, task_name: str) -> Optional[Dict]:
        """Load task progress.
        
        Args:
            task_name: Name of the task
            
        Returns:
            Progress data or None
        """
        progress_file = self.get_progress_file(task_name)
        if progress_file.exists():
            with open(progress_file, "r") as f:
                return json.load(f)
        return None
    
    def clear_cache(self):
        """Clear all cached files."""
        logger.warning("Clearing all cached files")
        shutil.rmtree(self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.metadata = {"files": {}}
        self._save_metadata()