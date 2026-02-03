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

    @classmethod
    def from_env(cls) -> "AppConfig":
        """Load configuration from environment variables."""
        load_dotenv()

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

        return cls(
            source=ServiceConfig(endpoint=source_endpoint, admin_key=source_key),
            destination=ServiceConfig(endpoint=dest_endpoint, admin_key=dest_key),
            timestamp_field=timestamp_field,
            key_field=key_field,
            state_dir=state_dir,
            desired_partitions=desired_partitions,
            index_name=index_name,
        )
