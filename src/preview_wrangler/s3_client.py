"""S3 client wrapper for AWS operations."""

import logging
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logger = logging.getLogger(__name__)


class S3Client:
    """Wrapper for S3 operations with error handling and retry logic."""

    def __init__(self, region_name: str = "us-east-1"):
        """Initialize S3 client.

        Args:
            region_name: AWS region name
        """
        try:
            self.s3 = boto3.client("s3", region_name=region_name)
            self._verify_credentials()
        except NoCredentialsError as err:
            raise RuntimeError(
                "AWS credentials not found. Please configure AWS credentials."
            ) from err

    def _verify_credentials(self):
        """Verify AWS credentials are configured."""
        try:
            self.s3.list_buckets()
        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidAccessKeyId":
                raise RuntimeError("Invalid AWS credentials") from e
            raise

    def list_objects(
        self, bucket: str, prefix: str = "", delimiter: str = ""
    ) -> list[dict[str, Any]]:
        """List objects in S3 bucket.

        Args:
            bucket: S3 bucket name
            prefix: Object prefix to filter
            delimiter: Delimiter for grouping objects

        Returns:
            List of object metadata
        """
        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter)

            objects = []
            for page in pages:
                if "Contents" in page:
                    objects.extend(page["Contents"])

            return objects
        except ClientError as e:
            logger.error(f"Error listing objects: {e}")
            raise

    def get_object(self, bucket: str, key: str) -> bytes:
        """Download object from S3.

        Args:
            bucket: S3 bucket name
            key: Object key

        Returns:
            Object content as bytes
        """
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except ClientError as e:
            logger.error(f"Error downloading object {key}: {e}")
            raise

    def download_file(self, bucket: str, key: str, filename: str):
        """Download file from S3 to local filesystem.

        Args:
            bucket: S3 bucket name
            key: Object key
            filename: Local filename to save to
        """
        try:
            self.s3.download_file(bucket, key, filename)
            logger.debug(f"Downloaded {key} to {filename}")
        except ClientError as e:
            logger.error(f"Error downloading file {key}: {e}")
            raise

    def head_object(self, bucket: str, key: str) -> Optional[dict[str, Any]]:
        """Get object metadata without downloading.

        Args:
            bucket: S3 bucket name
            key: Object key

        Returns:
            Object metadata or None if not found
        """
        try:
            return self.s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return None
            logger.error(f"Error checking object {key}: {e}")
            raise

    def object_exists(self, bucket: str, key: str) -> bool:
        """Check if object exists in S3.

        Args:
            bucket: S3 bucket name
            key: Object key

        Returns:
            True if object exists
        """
        return self.head_object(bucket, key) is not None
