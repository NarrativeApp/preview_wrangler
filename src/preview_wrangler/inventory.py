"""Inventory manifest downloader and parser."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .s3_client import S3Client

logger = logging.getLogger(__name__)


@dataclass
class InventoryFile:
    """Represents a file in the inventory manifest."""

    key: str
    size: int
    md5_checksum: str


@dataclass
class InventoryManifest:
    """Represents an S3 inventory manifest."""

    source_bucket: str
    destination_bucket: str
    version: str
    creation_timestamp: int
    file_format: str
    file_schema: str
    files: list[InventoryFile]

    @property
    def creation_date(self) -> datetime:
        """Get creation date as datetime."""
        return datetime.fromtimestamp(self.creation_timestamp / 1000)


class InventoryManager:
    """Manages S3 inventory operations."""

    INVENTORY_BUCKET = "prod.ml-meta-upload.getnarrativeapp.com-inventory"
    INVENTORY_PREFIX = "prod.ml-meta-upload.getnarrativeapp.com/Inventory/"
    BASE_BUCKET = "prod.ml-meta-upload.getnarrativeapp.com"

    def __init__(self, s3_client: S3Client):
        """Initialize inventory manager.

        Args:
            s3_client: S3 client instance
        """
        self.s3_client = s3_client

    def find_latest_inventory(self) -> Optional[str]:
        """Find the latest inventory directory.

        Returns:
            Path to latest inventory directory or None
        """
        try:
            # List all inventory directories
            self.s3_client.list_objects(
                bucket=self.INVENTORY_BUCKET,
                prefix=self.INVENTORY_PREFIX,
                delimiter="/",
            )

            # Extract directory names (CommonPrefixes)
            prefixes = []
            paginator = self.s3_client.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(
                Bucket=self.INVENTORY_BUCKET,
                Prefix=self.INVENTORY_PREFIX,
                Delimiter="/",
            )

            for page in pages:
                if "CommonPrefixes" in page:
                    prefixes.extend([p["Prefix"] for p in page["CommonPrefixes"]])

            if not prefixes:
                logger.error("No inventory directories found")
                return None

            # Filter for date-formatted directories (YYYY-MM-DDTHH-MMZ format)
            import re

            date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}Z/$")
            date_prefixes = [p for p in prefixes if date_pattern.search(p)]

            if not date_prefixes:
                logger.error("No date-formatted inventory directories found")
                return None

            # Sort by date (directory names are in ISO format)
            date_prefixes.sort(reverse=True)
            latest = date_prefixes[0]

            logger.info(f"Found latest inventory: {latest}")
            return latest

        except Exception as e:
            logger.error(f"Error finding latest inventory: {e}")
            raise

    def download_manifest(self, inventory_path: str) -> InventoryManifest:
        """Download and parse inventory manifest.

        Args:
            inventory_path: Path to inventory directory

        Returns:
            Parsed inventory manifest
        """
        manifest_key = f"{inventory_path}manifest.json"

        try:
            # Download manifest
            logger.info(f"Downloading manifest from {manifest_key}")
            manifest_data = self.s3_client.get_object(
                bucket=self.INVENTORY_BUCKET, key=manifest_key
            )

            # Parse JSON
            manifest_json = json.loads(manifest_data)

            # Convert to dataclass
            files = [
                InventoryFile(key=f["key"], size=f["size"], md5_checksum=f["MD5checksum"])
                for f in manifest_json["files"]
            ]

            manifest = InventoryManifest(
                source_bucket=manifest_json["sourceBucket"],
                destination_bucket=manifest_json["destinationBucket"],
                version=manifest_json["version"],
                creation_timestamp=int(manifest_json["creationTimestamp"]),
                file_format=manifest_json["fileFormat"],
                file_schema=manifest_json["fileSchema"],
                files=files,
            )

            logger.info(
                f"Loaded manifest with {len(manifest.files)} files, "
                f"created at {manifest.creation_date}"
            )

            return manifest

        except Exception as e:
            logger.error(f"Error downloading manifest: {e}")
            raise

    def get_latest_manifest(self) -> InventoryManifest:
        """Get the latest inventory manifest.

        Returns:
            Latest inventory manifest
        """
        latest_path = self.find_latest_inventory()
        if not latest_path:
            raise RuntimeError("No inventory found")

        return self.download_manifest(latest_path)
