"""State persistence for scan results."""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class MissingDoc:
    """A document ID that is missing from the destination."""
    id: str
    partition_id: int
    timestamp: str  # ISO format - value of the timestamp field for this doc


@dataclass
class ScanResult:
    """Result of a scan operation."""
    index_name: str
    timestamp: str  # ISO format
    timestamp_field: str
    key_field: str
    source_endpoint: str
    dest_endpoint: str
    partition_count: int
    total_scanned: int
    missing_docs: List[MissingDoc] = field(default_factory=list)

    @property
    def missing_count(self) -> int:
        return len(self.missing_docs)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "index_name": self.index_name,
            "timestamp": self.timestamp,
            "timestamp_field": self.timestamp_field,
            "key_field": self.key_field,
            "source_endpoint": self.source_endpoint,
            "dest_endpoint": self.dest_endpoint,
            "partition_count": self.partition_count,
            "total_scanned": self.total_scanned,
            "missing_docs": [
                {"id": doc.id, "partition_id": doc.partition_id, "timestamp": doc.timestamp}
                for doc in self.missing_docs
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ScanResult":
        """Create from dictionary loaded from JSON."""
        missing_docs = [
            MissingDoc(
                id=doc["id"],
                partition_id=doc["partition_id"],
                timestamp=doc.get("timestamp", ""),  # Backwards compatible
            )
            for doc in data.get("missing_docs", [])
        ]
        return cls(
            index_name=data["index_name"],
            timestamp=data["timestamp"],
            timestamp_field=data["timestamp_field"],
            key_field=data["key_field"],
            source_endpoint=data["source_endpoint"],
            dest_endpoint=data["dest_endpoint"],
            partition_count=data["partition_count"],
            total_scanned=data["total_scanned"],
            missing_docs=missing_docs,
        )


@dataclass
class FailedDoc:
    """A document that failed to copy."""
    id: str
    partition_id: int
    error: str
    timestamp: str  # When the failure occurred (ISO format)


@dataclass
class FailureLog:
    """Durable log of failed document copies."""
    index_name: str
    scan_timestamp: str  # Links to original scan
    failures: List[FailedDoc] = field(default_factory=list)

    def add_failure(self, doc_id: str, partition_id: int, error: str) -> None:
        """Add a failed document to the log."""
        self.failures.append(FailedDoc(
            id=doc_id,
            partition_id=partition_id,
            error=error,
            timestamp=datetime.utcnow().isoformat() + "Z",
        ))

    def add_failures(self, doc_ids: List[str], partition_id: int, error: str) -> None:
        """Add multiple failed documents with the same error."""
        ts = datetime.utcnow().isoformat() + "Z"
        for doc_id in doc_ids:
            self.failures.append(FailedDoc(
                id=doc_id,
                partition_id=partition_id,
                error=error,
                timestamp=ts,
            ))

    @property
    def failure_count(self) -> int:
        return len(self.failures)

    def to_dict(self) -> dict:
        return {
            "index_name": self.index_name,
            "scan_timestamp": self.scan_timestamp,
            "failures": [
                {
                    "id": f.id,
                    "partition_id": f.partition_id,
                    "error": f.error,
                    "timestamp": f.timestamp,
                }
                for f in self.failures
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "FailureLog":
        failures = [
            FailedDoc(
                id=f["id"],
                partition_id=f["partition_id"],
                error=f["error"],
                timestamp=f["timestamp"],
            )
            for f in data.get("failures", [])
        ]
        return cls(
            index_name=data["index_name"],
            scan_timestamp=data["scan_timestamp"],
            failures=failures,
        )


@dataclass
class RebalancedPartitionInfo:
    """Metadata for a rebalanced partition."""
    id: int
    start: str  # First doc timestamp
    end: str    # Last doc timestamp
    count: int  # Number of documents


@dataclass
class CopyProgress:
    """Progress of a copy operation (for resume support)."""
    index_name: str
    scan_timestamp: str  # Links to original scan
    # Per-partition progress: partition_id (as string) -> last copied timestamp
    partition_timestamps: Dict[str, str] = field(default_factory=dict)
    copied_count: int = 0
    failed_count: int = 0
    # Rebalanced partition definitions (computed at copy start for even distribution)
    # Maps partition_id (as string) -> partition info
    rebalanced_partitions: Optional[Dict[str, RebalancedPartitionInfo]] = None

    def get_partition_timestamp(self, partition_id: int) -> Optional[str]:
        """Get the last copied timestamp for a partition."""
        return self.partition_timestamps.get(str(partition_id))

    def set_partition_timestamp(self, partition_id: int, timestamp: str) -> None:
        """Set the last copied timestamp for a partition."""
        self.partition_timestamps[str(partition_id)] = timestamp

    def to_dict(self) -> dict:
        result = {
            "index_name": self.index_name,
            "scan_timestamp": self.scan_timestamp,
            "partition_timestamps": self.partition_timestamps,
            "copied_count": self.copied_count,
            "failed_count": self.failed_count,
        }
        if self.rebalanced_partitions is not None:
            result["rebalanced_partitions"] = {
                k: {"id": v.id, "start": v.start, "end": v.end, "count": v.count}
                for k, v in self.rebalanced_partitions.items()
            }
        return result

    @classmethod
    def from_dict(cls, data: dict) -> "CopyProgress":
        rebalanced = None
        if "rebalanced_partitions" in data:
            rebalanced = {
                k: RebalancedPartitionInfo(
                    id=v["id"],
                    start=v["start"],
                    end=v["end"],
                    count=v["count"],
                )
                for k, v in data["rebalanced_partitions"].items()
            }
        return cls(
            index_name=data["index_name"],
            scan_timestamp=data["scan_timestamp"],
            partition_timestamps=data.get("partition_timestamps", {}),
            copied_count=data.get("copied_count", 0),
            failed_count=data.get("failed_count", 0),
            rebalanced_partitions=rebalanced,
        )


class StateStore:
    """JSON-based state persistence."""

    def __init__(self, state_dir: str):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _scan_path(self, index_name: str) -> Path:
        return self.state_dir / f"{index_name}_missing.json"

    def _copy_path(self, index_name: str) -> Path:
        return self.state_dir / f"{index_name}_copy_progress.json"

    def _failure_path(self, index_name: str) -> Path:
        return self.state_dir / f"{index_name}_failures.json"

    def write_scan(self, result: ScanResult) -> Path:
        """Write scan result to JSON file (atomic write)."""
        path = self._scan_path(result.index_name)
        temp_path = path.with_suffix(".json.tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, indent=2)
        temp_path.replace(path)
        return path

    def read_scan(self, index_name: str) -> Optional[ScanResult]:
        """Read scan result from JSON file."""
        path = self._scan_path(index_name)
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return ScanResult.from_dict(data)

    def write_copy_progress(self, progress: CopyProgress) -> None:
        """Write copy progress to JSON file (atomic write)."""
        path = self._copy_path(progress.index_name)
        temp_path = path.with_suffix(".json.tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(progress.to_dict(), f, indent=2)
        temp_path.replace(path)

    def read_copy_progress(self, index_name: str) -> Optional[CopyProgress]:
        """Read copy progress from JSON file."""
        path = self._copy_path(index_name)
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return CopyProgress.from_dict(data)

    def clear_copy_progress(self, index_name: str) -> None:
        """Remove copy progress file after successful completion."""
        path = self._copy_path(index_name)
        if path.exists():
            path.unlink()

    def write_failure_log(self, log: FailureLog) -> Path:
        """Write failure log to JSON file (atomic write)."""
        path = self._failure_path(log.index_name)
        temp_path = path.with_suffix(".json.tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(log.to_dict(), f, indent=2)
        temp_path.replace(path)
        return path

    def read_failure_log(self, index_name: str) -> Optional[FailureLog]:
        """Read failure log from JSON file."""
        path = self._failure_path(index_name)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return FailureLog.from_dict(data)
        except json.JSONDecodeError:
            # Corrupted file, remove it and start fresh
            path.unlink()
            return None

    def clear_failure_log(self, index_name: str) -> None:
        """Remove failure log file."""
        path = self._failure_path(index_name)
        if path.exists():
            path.unlink()
