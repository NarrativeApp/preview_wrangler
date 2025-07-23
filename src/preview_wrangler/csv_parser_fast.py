"""Fast CSV parser using concurrent processing."""

import csv
import logging
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set, Tuple

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


def process_single_csv(csv_path: Path) -> Tuple[Set[Tuple[str, str]], Set[str]]:
    """Process a single CSV file to extract preview dirs and ML files.
    
    Args:
        csv_path: Path to CSV file
        
    Returns:
        Tuple of (preview_dirs, ml_files)
    """
    preview_pattern = re.compile(r"^([a-f0-9-]{36})/([a-f0-9-]{36})/preview\.v1/")
    preview_dirs = set()
    ml_files = set()
    
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
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
                    preview_dirs.add((user_uuid, project_uuid))
                
                # Check for ML upload file
                elif key.endswith(".v3.gz") and key.count("/") == 2:
                    parts = key.split("/")
                    if len(parts) == 3:
                        user_uuid, project_uuid, filename = parts
                        expected_filename = f"{project_uuid}.v3.gz"
                        if filename == expected_filename:
                            ml_files.add(key)
    
    except Exception as e:
        logger.error(f"Error processing {csv_path}: {e}")
    
    return preview_dirs, ml_files


class FastCSVParser:
    """Fast CSV parser using multiprocessing."""
    
    MAX_WORKERS = 8  # Process multiple CSV files concurrently
    
    def parse_csv_files(self, csv_paths: List[Path]) -> List[PreviewDirectory]:
        """Parse all CSV files to find preview directories.
        
        Args:
            csv_paths: List of CSV file paths
            
        Returns:
            List of preview directories with ML upload files
        """
        logger.info(f"Parsing {len(csv_paths)} CSV files using {self.MAX_WORKERS} workers")
        
        all_preview_dirs = set()
        all_ml_files = set()
        
        # Process CSV files concurrently
        with ProcessPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_csv = {
                executor.submit(process_single_csv, csv_path): csv_path
                for csv_path in csv_paths
            }
            
            # Process results as they complete
            with tqdm(total=len(csv_paths), desc="Processing CSV files") as pbar:
                for future in as_completed(future_to_csv):
                    csv_path = future_to_csv[future]
                    try:
                        preview_dirs, ml_files = future.result()
                        all_preview_dirs.update(preview_dirs)
                        all_ml_files.update(ml_files)
                    except Exception as e:
                        logger.error(f"Failed to process {csv_path}: {e}")
                    finally:
                        pbar.update(1)
        
        logger.info(f"Found {len(all_preview_dirs)} unique preview directories")
        logger.info(f"Found {len(all_ml_files)} ML upload files")
        
        # Match preview directories with ML files
        qualified_previews = []
        for user_uuid, project_uuid in all_preview_dirs:
            ml_upload_path = f"{user_uuid}/{project_uuid}/{project_uuid}.v3.gz"
            
            if ml_upload_path in all_ml_files:
                preview = PreviewDirectory(
                    user_uuid=user_uuid,
                    project_uuid=project_uuid,
                    preview_path=f"{user_uuid}/{project_uuid}/preview.v1",
                    ml_upload_path=ml_upload_path
                )
                qualified_previews.append(preview)
        
        logger.info(
            f"Found {len(qualified_previews)} preview directories "
            "with matching ML upload files"
        )
        
        return qualified_previews