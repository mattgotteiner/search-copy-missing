"""Scanner for finding missing documents between source and destination indexes."""

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, List, Optional, Tuple

from azure.search.documents.aio import SearchClient
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .state import MissingDoc, ScanResult


@dataclass
class RebalancedPartition:
    """A partition with document count information for balanced distribution."""
    id: int
    start: str  # ISO timestamp of first doc
    end: str    # ISO timestamp of last doc
    count: int  # Number of documents in this partition


def rebalance_by_count(
    docs: List[MissingDoc],
    num_partitions: int,
) -> Tuple[dict[int, List[MissingDoc]], List[RebalancedPartition]]:
    """
    Rebalance documents into partitions with roughly equal document counts.
    
    Instead of using the scan-time partition_id (which is based on equal time slices),
    this redistributes documents so each partition has approximately the same number
    of documents, improving parallel copy performance.
    
    Args:
        docs: List of missing documents from scan result
        num_partitions: Desired number of partitions
        
    Returns:
        Tuple of:
        - Dict mapping new partition_id -> list of documents
        - List of RebalancedPartition metadata for persistence
    """
    if not docs:
        return {}, []
    
    # Sort all documents by timestamp for ordered chunking
    sorted_docs = sorted(docs, key=lambda d: d.timestamp)
    
    # Calculate target docs per partition
    total_docs = len(sorted_docs)
    base_size = total_docs // num_partitions
    remainder = total_docs % num_partitions
    
    docs_by_partition: dict[int, List[MissingDoc]] = {}
    partition_metadata: List[RebalancedPartition] = []
    
    start_idx = 0
    for partition_id in range(num_partitions):
        # Distribute remainder across first partitions
        partition_size = base_size + (1 if partition_id < remainder else 0)
        
        if partition_size == 0:
            continue
            
        end_idx = start_idx + partition_size
        partition_docs = sorted_docs[start_idx:end_idx]
        
        docs_by_partition[partition_id] = partition_docs
        partition_metadata.append(RebalancedPartition(
            id=partition_id,
            start=partition_docs[0].timestamp,
            end=partition_docs[-1].timestamp,
            count=len(partition_docs),
        ))
        
        start_idx = end_idx
    
    return docs_by_partition, partition_metadata


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
    """Divide a time range into equal partitions.
    
    Uses the original start/end timestamps for first/last partition boundaries
    to avoid floating-point precision issues that could exclude edge documents.
    """
    start_dt = timestamp_to_datetime(start)
    end_dt = timestamp_to_datetime(end)
    
    total_seconds = (end_dt - start_dt).total_seconds()
    slice_seconds = total_seconds / num_slices
    
    partitions = []
    for i in range(num_slices):
        # Use original timestamps for first/last boundaries to avoid precision loss
        if i == 0:
            slice_start_str = start
        else:
            slice_start = start_dt.timestamp() + (i * slice_seconds)
            slice_start_str = datetime_to_timestamp(datetime.utcfromtimestamp(slice_start))
        
        if i == num_slices - 1:
            slice_end_str = end
        else:
            slice_end = start_dt.timestamp() + ((i + 1) * slice_seconds)
            slice_end_str = datetime_to_timestamp(datetime.utcfromtimestamp(slice_end))
        
        partitions.append(Partition(
            id=i,
            start=slice_start_str,
            end=slice_end_str,
        ))
    
    return partitions


async def get_timestamp_range(
    client: SearchClient, 
    timestamp_field: str
) -> Optional[Tuple[str, str]]:
    """Get the min and max timestamps from an index."""
    # Get minimum
    min_result = await client.search(
        search_text="*",
        filter=f"{timestamp_field} ne null",
        select=[timestamp_field],
        order_by=[f"{timestamp_field} asc"],
        top=1,
    )
    min_ts = None
    async for doc in min_result:
        min_ts = doc.get(timestamp_field)
        break
    
    if min_ts is None:
        return None
    
    # Get maximum
    max_result = await client.search(
        search_text="*",
        filter=f"{timestamp_field} ne null",
        select=[timestamp_field],
        order_by=[f"{timestamp_field} desc"],
        top=1,
    )
    max_ts = None
    async for doc in max_result:
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


async def get_document_count(client: SearchClient) -> int:
    """Get the total document count from an index."""
    results = await client.search(
        search_text="*",
        include_total_count=True,
        top=0,
    )
    # Consume iterator to get count
    async for _ in results:
        pass
    return await results.get_count() or 0


async def count_docs_with_field(client: SearchClient, field_name: str) -> int:
    """Count documents that have a non-null value for the specified field."""
    results = await client.search(
        search_text="*",
        filter=f"{field_name} ne null",
        include_total_count=True,
        top=0,
    )
    # Consume iterator to get count
    async for _ in results:
        pass
    return await results.get_count() or 0


async def count_partition_docs(
    client: SearchClient,
    timestamp_field: str,
    partition: Partition,
) -> int:
    """Count documents in a specific partition."""
    filter_str = build_filter(timestamp_field, partition.start, partition.end)
    results = await client.search(
        search_text="*",
        filter=filter_str,
        include_total_count=True,
        top=0,
    )
    # Consume iterator to get count
    async for _ in results:
        pass
    return await results.get_count() or 0


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
    batch_size: int = 10000,
) -> AsyncIterator[List[tuple[str, str]]]:
    """
    Iterate through all documents in a partition using timestamp-based pagination.
    Yields batches of (document_id, timestamp) tuples.
    
    Uses the same pagination strategy as the backup tool:
    - Track seen keys at timestamp boundary to handle duplicates
    - Always use ge (>=) and rely on seen_keys tracking for deduplication
    - Check raw_fetched count to determine if more results exist
    """
    session_id = str(uuid.uuid4())
    
    # For partition 0, use inclusive start (ge). For other partitions, use exclusive
    # start (gt) on first query to avoid duplicates at partition boundaries.
    use_exclusive_start = partition.id > 0
    
    filter_expression = build_filter(
        timestamp_field, partition.start, partition.end, exclusive_start=use_exclusive_start
    )
    
    # Track keys of documents we have seen at the current timestamp boundary
    seen_keys_at_boundary: set = set()
    boundary_timestamp: Optional[str] = None
    
    while True:
        results = await client.search(
            search_text="*",
            filter=filter_expression,
            select=[key_field, timestamp_field],
            order_by=[f"{timestamp_field} asc"],
            top=batch_size,
            session_id=session_id,
        )
        
        # Collect raw items and track count before filtering
        raw_items = [item async for item in results]
        raw_fetched = len(raw_items)
        
        if raw_fetched == 0:
            break
        
        # Filter out documents we have already processed (from previous batch with same timestamp)
        if seen_keys_at_boundary and boundary_timestamp:
            items = []
            for item in raw_items:
                item_ts = item.get(timestamp_field)
                if isinstance(item_ts, datetime):
                    item_ts = datetime_to_timestamp(item_ts)
                item_key = item.get(key_field)
                
                # Skip if this is at the boundary timestamp and we have seen this key
                if item_ts == boundary_timestamp and item_key in seen_keys_at_boundary:
                    continue
                items.append(item)
        else:
            items = raw_items
        
        if not items:
            # All items were duplicates, but there might be more
            if raw_fetched >= batch_size:
                # Need to move the filter forward - use the last raw item timestamp
                last_raw_ts = raw_items[-1].get(timestamp_field)
                if isinstance(last_raw_ts, datetime):
                    last_raw_ts = datetime_to_timestamp(last_raw_ts)
                filter_expression = build_filter(
                    timestamp_field, last_raw_ts, partition.end, exclusive_start=True
                )
                seen_keys_at_boundary.clear()
                boundary_timestamp = None
                continue
            else:
                break
        
        # Extract IDs and timestamps
        batch_items = []
        for item in items:
            item_key = item.get(key_field)
            item_ts = item.get(timestamp_field)
            if isinstance(item_ts, datetime):
                item_ts = datetime_to_timestamp(item_ts)
            batch_items.append((item_key, item_ts))
        
        yield batch_items
        
        # Get last timestamp from the filtered items
        last_item = items[-1]
        last_timestamp = last_item.get(timestamp_field)
        if isinstance(last_timestamp, datetime):
            last_timestamp = datetime_to_timestamp(last_timestamp)
        
        # Update boundary tracking
        if last_timestamp != boundary_timestamp:
            seen_keys_at_boundary.clear()
            boundary_timestamp = last_timestamp
        
        # Add all keys with the boundary timestamp to our tracking set
        for item in items:
            item_ts = item.get(timestamp_field)
            if isinstance(item_ts, datetime):
                item_ts = datetime_to_timestamp(item_ts)
            if item_ts == boundary_timestamp:
                item_key = item.get(key_field)
                if item_key is not None:
                    seen_keys_at_boundary.add(item_key)
        
        # Update filter for next iteration - always use inclusive (ge)
        # We rely on seen_keys_at_boundary to filter duplicates
        filter_expression = build_filter(
            timestamp_field, last_timestamp, partition.end, exclusive_start=False
        )
        
        # Check if we got fewer items than requested (no more results)
        if raw_fetched < batch_size:
            break


def build_search_in_filter(key_field: str, ids: List[str]) -> str:
    """
    Build a search.in filter for checking multiple IDs.
    Format: search.in(key_field, 'id1|id2|id3', '|')
    
    Uses pipe as delimiter since it is unlikely to appear in doc IDs.
    Single quotes in IDs are escaped by doubling.
    """
    escaped_ids = []
    for id_val in ids:
        # Escape single quotes for the OData string
        escaped = id_val.replace("'", "''")
        escaped_ids.append(escaped)
    
    ids_str = "|".join(escaped_ids)
    return f"search.in({key_field}, '{ids_str}', '|')"


async def check_existence_batch(
    client: SearchClient,
    key_field: str,
    ids: List[str],
) -> set:
    """
    Check which IDs exist in the destination index using search.in filter.
    Returns the set of IDs that exist.
    """
    if not ids:
        return set()
    
    filter_str = build_search_in_filter(key_field, ids)
    
    try:
        results = await client.search(
            search_text="*",
            filter=filter_str,
            select=[key_field],
            top=len(ids),
        )
        
        existing = set()
        async for doc in results:
            existing.add(doc.get(key_field))
        
        return existing
    except Exception as e:
        # If filter is too long, fall back to smaller batches
        if len(ids) > 10:
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
    overall_task_id: Optional[TaskID] = None,
    existence_batch_size: int = 10000,  # Check full page at once with search.in
) -> Tuple[int, List[MissingDoc]]:
    """
    Scan a single partition for missing documents.
    Returns (total_scanned, list of missing docs).
    """
    total_scanned = 0
    missing_docs = []
    
    async for batch_items in iterate_partition_docs(
        source_client, timestamp_field, key_field, partition
    ):
        total_scanned += len(batch_items)
        
        # Extract just IDs for existence check
        batch_ids = [doc_id for doc_id, _ in batch_items]
        
        # Check existence - with search.in we can check full batch at once
        existing = await check_existence_batch(dest_client, key_field, batch_ids)
        
        for doc_id, doc_ts in batch_items:
            if doc_id not in existing:
                missing_docs.append(MissingDoc(id=doc_id, partition_id=partition.id, timestamp=doc_ts))
        
        progress.update(task_id, advance=len(batch_items))
        if overall_task_id is not None:
            progress.update(overall_task_id, advance=len(batch_items))
    
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
    
    # Get total document count for diagnostics
    total_index_count = await get_document_count(source_client)
    console.print(f"  Total documents in source index: {total_index_count:,}")
    
    # Count documents missing the timestamp field
    docs_with_timestamp = await count_docs_with_field(source_client, timestamp_field)
    docs_missing_timestamp = total_index_count - docs_with_timestamp
    console.print(f"  [bold]Documents to scan (have {timestamp_field}): {docs_with_timestamp:,}[/bold]")
    if docs_missing_timestamp > 0:
        console.print(
            f"  [yellow]WARNING: {docs_missing_timestamp:,} documents are missing "
            f"the timestamp field and will NOT be scanned![/yellow]"
        )
    
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
    
    # Count documents per partition for progress bars
    console.print("Counting documents per partition...")
    partition_counts = await asyncio.gather(*[
        count_partition_docs(source_client, timestamp_field, p)
        for p in partitions
    ])
    
    # Filter out empty partitions and log gaps
    active_partitions = []
    active_counts = []
    for p, count in zip(partitions, partition_counts):
        if count == 0:
            console.print(f"  [dim]Partition {p.id}: Skipped (gap in data: {p.start} to {p.end})[/dim]")
        else:
            active_partitions.append(p)
            active_counts.append(count)
    
    total_docs = sum(active_counts)
    console.print(f"  Total documents: {total_docs:,} across {len(active_partitions)} active partitions")
    for p, count in zip(active_partitions, active_counts):
        console.print(f"    Partition {p.id}: {count:,}  [{p.start} to {p.end}]")
    
    # Scan partitions in parallel
    total_scanned = 0
    all_missing: List[MissingDoc] = []
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("["),
        TimeElapsedColumn(),
        TextColumn("<"),
        TimeRemainingColumn(),
        TextColumn("]"),
        console=console,
        refresh_per_second=2,
    ) as progress:
        # Create overall progress task
        overall_task_id = progress.add_task(
            "[bold green]Overall",
            total=total_docs,
        )
        
        # Create tasks for each partition with known totals
        tasks = []
        task_ids = []
        
        # Track which partitions are active vs skipped
        partition_idx = 0
        for p, count in zip(partitions, partition_counts):
            if count == 0:
                # Add a completed task for skipped partitions
                task_id = progress.add_task(
                    f"[dim]Partition {p.id} Skipped",
                    total=1,
                    completed=1,
                )
            else:
                task_id = progress.add_task(
                    f"[cyan]Partition {p.id}",
                    total=count,
                )
                task_ids.append(task_id)
                
                tasks.append(scan_partition(
                    source_client,
                    dest_client,
                    timestamp_field,
                    key_field,
                    active_partitions[partition_idx],
                    progress,
                    task_id,
                    overall_task_id,
                ))
                partition_idx += 1
        
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
