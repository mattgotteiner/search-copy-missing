"""Scanner for finding missing documents between source and destination indexes."""

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, List, Optional, Tuple

from azure.search.documents import SearchClient
from rich.console import Console
from rich.progress import Progress, TaskID

from .state import MissingDoc, ScanResult


@dataclass
class Partition:
    """A time-based partition of the index."""
    id: int
    start: str  # ISO timestamp
    end: str    # ISO timestamp


def datetime_to_timestamp(dt: datetime) -> str:
    """Convert datetime to ISO timestamp string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def timestamp_to_datetime(ts: str) -> datetime:
    """Convert ISO timestamp string to datetime."""
    # Handle both with and without microseconds
    for fmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]:
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {ts}")


def build_time_slices(start: str, end: str, num_slices: int) -> List[Partition]:
    """Divide a time range into equal partitions."""
    start_dt = timestamp_to_datetime(start)
    end_dt = timestamp_to_datetime(end)
    
    total_seconds = (end_dt - start_dt).total_seconds()
    slice_seconds = total_seconds / num_slices
    
    partitions = []
    for i in range(num_slices):
        slice_start = start_dt.timestamp() + (i * slice_seconds)
        slice_end = start_dt.timestamp() + ((i + 1) * slice_seconds)
        
        partitions.append(Partition(
            id=i,
            start=datetime_to_timestamp(datetime.utcfromtimestamp(slice_start)),
            end=datetime_to_timestamp(datetime.utcfromtimestamp(slice_end)),
        ))
    
    return partitions


async def get_timestamp_range(
    client: SearchClient, 
    timestamp_field: str
) -> Optional[Tuple[str, str]]:
    """Get the min and max timestamps from an index."""
    # Get minimum
    min_result = client.search(
        search_text="*",
        select=[timestamp_field],
        order_by=[f"{timestamp_field} asc"],
        top=1,
    )
    min_ts = None
    for doc in min_result:
        min_ts = doc.get(timestamp_field)
        break
    
    if min_ts is None:
        return None
    
    # Get maximum
    max_result = client.search(
        search_text="*",
        select=[timestamp_field],
        order_by=[f"{timestamp_field} desc"],
        top=1,
    )
    max_ts = None
    for doc in max_result:
        max_ts = doc.get(timestamp_field)
        break
    
    if max_ts is None:
        return None
    
    # Convert datetime objects to strings if needed
    if isinstance(min_ts, datetime):
        min_ts = datetime_to_timestamp(min_ts)
    if isinstance(max_ts, datetime):
        max_ts = datetime_to_timestamp(max_ts)
    
    return (min_ts, max_ts)


def build_filter(
    timestamp_field: str,
    start: str,
    end: str,
    exclusive_start: bool = False,
) -> str:
    """Build an OData filter for a time range."""
    start_op = "gt" if exclusive_start else "ge"
    return f"{timestamp_field} {start_op} {start} and {timestamp_field} le {end}"


async def iterate_partition_docs(
    client: SearchClient,
    timestamp_field: str,
    key_field: str,
    partition: Partition,
    batch_size: int = 1000,
) -> AsyncIterator[List[str]]:
    """
    Iterate through all document IDs in a partition using timestamp-based pagination.
    Yields batches of document IDs.
    """
    session_id = str(uuid.uuid4())
    current_start = partition.start
    seen_at_boundary: set = set()
    exclusive_start = False
    
    while True:
        filter_str = build_filter(
            timestamp_field, current_start, partition.end, exclusive_start
        )
        
        results = client.search(
            search_text="*",
            filter=filter_str,
            select=[key_field, timestamp_field],
            order_by=[f"{timestamp_field} asc"],
            top=batch_size,
            session_id=session_id,
        )
        
        batch_ids = []
        batch_timestamps = []
        last_timestamp = None
        
        for doc in results:
            doc_id = doc.get(key_field)
            doc_ts = doc.get(timestamp_field)
            
            if isinstance(doc_ts, datetime):
                doc_ts = datetime_to_timestamp(doc_ts)
            
            # Skip duplicates at boundary
            if doc_ts == current_start and doc_id in seen_at_boundary:
                continue
            
            batch_ids.append(doc_id)
            batch_timestamps.append(doc_ts)
            last_timestamp = doc_ts
        
        if batch_ids:
            yield batch_ids
        
        # Check if we got a full batch (more results may exist)
        if len(batch_ids) < batch_size:
            break
        
        # Update for next iteration
        if last_timestamp:
            # Track IDs at the boundary timestamp to avoid duplicates
            seen_at_boundary = {
                batch_ids[i] 
                for i, ts in enumerate(batch_timestamps) 
                if ts == last_timestamp
            }
            current_start = last_timestamp
            exclusive_start = False  # Use >= because we track seen IDs
        else:
            break


async def check_existence_batch(
    client: SearchClient,
    key_field: str,
    ids: List[str],
) -> set:
    """
    Check which IDs exist in the destination index.
    Returns the set of IDs that exist.
    """
    if not ids:
        return set()
    
    # Build OR filter for batch existence check
    # Escape single quotes in IDs
    escaped_ids = [id.replace("'", "''") for id in ids]
    filter_parts = [f"{key_field} eq '{id}'" for id in escaped_ids]
    filter_str = " or ".join(filter_parts)
    
    try:
        results = client.search(
            search_text="*",
            filter=filter_str,
            select=[key_field],
            top=len(ids),
        )
        
        existing = set()
        for doc in results:
            existing.add(doc.get(key_field))
        
        return existing
    except Exception as e:
        # If filter is too long, fall back to smaller batches
        if "filter" in str(e).lower() and len(ids) > 10:
            mid = len(ids) // 2
            set1 = await check_existence_batch(client, key_field, ids[:mid])
            set2 = await check_existence_batch(client, key_field, ids[mid:])
            return set1 | set2
        raise


async def scan_partition(
    source_client: SearchClient,
    dest_client: SearchClient,
    timestamp_field: str,
    key_field: str,
    partition: Partition,
    progress: Progress,
    task_id: TaskID,
    existence_batch_size: int = 50,
) -> Tuple[int, List[MissingDoc]]:
    """
    Scan a single partition for missing documents.
    Returns (total_scanned, list of missing docs).
    """
    total_scanned = 0
    missing_docs = []
    
    async for batch_ids in iterate_partition_docs(
        source_client, timestamp_field, key_field, partition
    ):
        total_scanned += len(batch_ids)
        
        # Check existence in batches
        for i in range(0, len(batch_ids), existence_batch_size):
            chunk = batch_ids[i:i + existence_batch_size]
            existing = await check_existence_batch(dest_client, key_field, chunk)
            
            for doc_id in chunk:
                if doc_id not in existing:
                    missing_docs.append(MissingDoc(id=doc_id, partition_id=partition.id))
        
        progress.update(task_id, advance=len(batch_ids))
    
    return total_scanned, missing_docs


async def scan_index(
    source_client: SearchClient,
    dest_client: SearchClient,
    timestamp_field: str,
    key_field: str,
    index_name: str,
    source_endpoint: str,
    dest_endpoint: str,
    num_partitions: int,
    console: Console,
) -> ScanResult:
    """
    Scan an index to find all documents missing from destination.
    Uses parallel partition scanning for speed.
    """
    console.print(f"[bold]Scanning index:[/bold] {index_name}")
    
    # Get timestamp range
    console.print("Getting timestamp range...")
    ts_range = await get_timestamp_range(source_client, timestamp_field)
    
    if ts_range is None:
        console.print("[yellow]No documents found in source index[/yellow]")
        return ScanResult(
            index_name=index_name,
            timestamp=datetime_to_timestamp(datetime.utcnow()),
            timestamp_field=timestamp_field,
            key_field=key_field,
            source_endpoint=source_endpoint,
            dest_endpoint=dest_endpoint,
            partition_count=num_partitions,
            total_scanned=0,
            missing_docs=[],
        )
    
    min_ts, max_ts = ts_range
    console.print(f"  Timestamp range: {min_ts} to {max_ts}")
    
    # Build partitions
    partitions = build_time_slices(min_ts, max_ts, num_partitions)
    console.print(f"  Created {len(partitions)} partitions")
    
    # Scan partitions in parallel
    total_scanned = 0
    all_missing: List[MissingDoc] = []
    
    with Progress(console=console) as progress:
        # Create tasks for each partition
        tasks = []
        task_ids = []
        
        for partition in partitions:
            task_id = progress.add_task(
                f"[cyan]Partition {partition.id}",
                total=None,  # Unknown total
            )
            task_ids.append(task_id)
            
            tasks.append(scan_partition(
                source_client,
                dest_client,
                timestamp_field,
                key_field,
                partition,
                progress,
                task_id,
            ))
        
        # Run all partitions in parallel
        results = await asyncio.gather(*tasks)
        
        for scanned, missing in results:
            total_scanned += scanned
            all_missing.extend(missing)
    
    console.print(f"[bold green]Scan complete![/bold green]")
    console.print(f"  Total scanned: {total_scanned:,}")
    console.print(f"  Missing: {len(all_missing):,}")
    
    return ScanResult(
        index_name=index_name,
        timestamp=datetime_to_timestamp(datetime.utcnow()),
        timestamp_field=timestamp_field,
        key_field=key_field,
        source_endpoint=source_endpoint,
        dest_endpoint=dest_endpoint,
        partition_count=num_partitions,
        total_scanned=total_scanned,
        missing_docs=all_missing,
    )
