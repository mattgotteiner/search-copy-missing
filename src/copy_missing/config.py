"""Configuration loading from environment variables."""

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


@dataclass
class ServiceConfig:
    """Configuration for a single Azure Search service."""
    endpoint: str
    admin_key: Optional[str] = None


@dataclass
class AppConfig:
    """Application configuration loaded from environment variables."""
    source: ServiceConfig
    destination: ServiceConfig
    timestamp_field: str
    key_field: Optional[str]  # None means auto-detect from index definition
    state_dir: str
    desired_partitions: int
    index_name: Optional[str]
    copy_parallelism: Optional[int]  # None means all partitions in parallel
    copy_batch_size: int  # Number of docs to copy at a time

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load configuration from environment variables."""
        load_dotenv(override=True)

        source_endpoint = os.environ.get("AZURE_SEARCH_SOURCE_ENDPOINT", "")
        source_key = os.environ.get("AZURE_SEARCH_SOURCE_KEY")
        dest_endpoint = os.environ.get("AZURE_SEARCH_DEST_ENDPOINT", "")
        dest_key = os.environ.get("AZURE_SEARCH_DEST_KEY")

        if not source_endpoint:
            raise ValueError("AZURE_SEARCH_SOURCE_ENDPOINT is required")
        if not dest_endpoint:
            raise ValueError("AZURE_SEARCH_DEST_ENDPOINT is required")

        timestamp_field = os.environ.get("TIMESTAMP_FIELD", "")
        if not timestamp_field:
            raise ValueError("TIMESTAMP_FIELD is required")

        key_field = os.environ.get("KEY_FIELD") or None  # Empty string -> None
        state_dir = os.environ.get("STATE_DIR", "state")
        desired_partitions = int(os.environ.get("DESIRED_PARTITIONS", "8"))
        index_name = os.environ.get("INDEX_NAME")
        copy_parallelism_str = os.environ.get("COPY_PARALLELISM")
        copy_parallelism = int(copy_parallelism_str) if copy_parallelism_str else None
        copy_batch_size = int(os.environ.get("COPY_BATCH_SIZE", "250"))

        return cls(
            source=ServiceConfig(endpoint=source_endpoint, admin_key=source_key),
            destination=ServiceConfig(endpoint=dest_endpoint, admin_key=dest_key),
            timestamp_field=timestamp_field,
            key_field=key_field,
            state_dir=state_dir,
            desired_partitions=desired_partitions,
            index_name=index_name,
            copy_parallelism=copy_parallelism,
            copy_batch_size=copy_batch_size,
        )
