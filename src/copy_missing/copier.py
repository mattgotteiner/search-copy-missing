"""Parallel document copier from persisted scan results."""

import asyncio
from collections import defaultdict
from typing import Callable, Dict, List, Optional, TypeVar

from azure.core.exceptions import (
    HttpResponseError,
    ServiceRequestError,
    ServiceResponseError,
)
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

from .state import CopyProgress, FailureLog, MissingDoc, ScanResult, StateStore

T = TypeVar("T")

# Retry configuration
RETRY_MAX_ATTEMPTS = 5
RETRY_BASE_DELAY = 2.0
RETRY_MAX_DELAY = 60.0


def is_retryable_exception(exc: Exception) -> bool:
    """Check if an exception is transient and can be retried."""
    if isinstance(exc, ServiceRequestError):
        return True
    if isinstance(exc, ServiceResponseError):
        return True
    if isinstance(exc, HttpResponseError):
        # Retry on throttling (429) and server errors (5xx)
        if exc.status_code == 429:
            return True
        if exc.status_code and 500 <= exc.status_code < 600:
            return True
    return False


async def retry_with_backoff(
    operation_name: str,
    operation: Callable[[], T],
    console: Console,
    max_attempts: int = RETRY_MAX_ATTEMPTS,
    base_delay: float = RETRY_BASE_DELAY,
    max_delay: float = RETRY_MAX_DELAY,
) -> T:
    """Execute an async operation with exponential backoff retry."""
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return await operation()
        except Exception as exc:
            last_exception = exc
            
            if not is_retryable_exception(exc):
                raise
            
            if attempt < max_attempts - 1:
                delay = min(base_delay * (2 ** attempt), max_delay)
                console.print(
                    f"[yellow]{operation_name} failed (attempt {attempt + 1}/{max_attempts}): "
                    f"{exc}. Retrying in {delay:.1f}s...[/yellow]"
                )
                await asyncio.sleep(delay)
    
    raise last_exception


async def copy_document(
    source_client: SearchClient,
    dest_client: SearchClient,
    key_field: str,
    doc_id: str,
    console: Console,
) -> bool:
    """
    Copy a single document from source to destination.
    Returns True on success, False on failure.
    """
    try:
        # Fetch document from source
        doc = await retry_with_backoff(
            f"Fetch {doc_id}",
            lambda: source_client.get_document(key=doc_id),
            console,
        )
        
        # Upload to destination
        await retry_with_backoff(
            f"Upload {doc_id}",
            lambda: dest_client.upload_documents(documents=[doc]),
            console,
        )
        
        return True
    except Exception as exc:
        console.print(f"[red]Failed to copy {doc_id}: {exc}[/red]")
        return False


async def copy_batch(
    source_client: SearchClient,
    dest_client: SearchClient,
    key_field: str,
    doc_ids: List[str],
    partition_id: int,
    failure_log: FailureLog,
    console: Console,
) -> tuple[int, int]:
    """
    Copy a batch of documents from source to destination.
    Returns (success_count, failure_count).
    """
    success_count = 0
    failure_count = 0
    
    if not doc_ids:
        return success_count, failure_count
    
    # Fetch all documents in a single search using search.in filter
    docs = []
    try:
        # Doc IDs only contain alphanumeric, dash, underscore, equals - no escaping needed
        # See: https://learn.microsoft.com/en-us/rest/api/searchservice/naming-rules
        ids_string = ",".join(doc_ids)
        filter_expr = f"search.in({key_field}, '{ids_string}', ',')"
        
        async def fetch_batch():
            results = []
            search_results = await source_client.search(
                search_text="*",
                filter=filter_expr,
                top=len(doc_ids),
            )
            async for doc in search_results:
                results.append(doc)
            return results
        
        docs = await retry_with_backoff(
            f"Fetch batch of {len(doc_ids)}",
            fetch_batch,
            console,
        )
        
        # Track any IDs not found
        found_ids = {doc[key_field] for doc in docs}
        missing_ids = list(set(doc_ids) - found_ids)
        if missing_ids:
            console.print(f"[yellow]Warning: {len(missing_ids)} documents not found in source[/yellow]")
            failure_log.add_failures(missing_ids, partition_id, "Document not found in source")
            failure_count += len(missing_ids)
            
    except Exception as exc:
        error_msg = str(exc)
        console.print(f"[red]Failed to fetch batch: {exc}[/red]")
        failure_log.add_failures(doc_ids, partition_id, f"Fetch failed: {error_msg}")
        failure_count += len(doc_ids)
        return success_count, failure_count
    
    if not docs:
        return success_count, failure_count
    
    # Upload batch to destination
    try:
        await retry_with_backoff(
            f"Upload batch of {len(docs)}",
            lambda: dest_client.upload_documents(documents=docs),
            console,
        )
        success_count = len(docs)
    except Exception as exc:
        error_msg = str(exc)
        console.print(f"[red]Failed to upload batch: {exc}[/red]")
        uploaded_ids = [doc[key_field] for doc in docs]
        failure_log.add_failures(uploaded_ids, partition_id, f"Upload failed: {error_msg}")
        failure_count += len(docs)
    
    return success_count, failure_count


async def copy_partition(
    source_client: SearchClient,
    dest_client: SearchClient,
    key_field: str,
    partition_id: int,
    docs: List[MissingDoc],
    progress_state: CopyProgress,
    failure_log: FailureLog,
    state_store: StateStore,
    state_lock: asyncio.Lock,
    console: Console,
    progress: Progress,
    task_id: TaskID,
    overall_task_id: TaskID,
    batch_size: int = 250,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> tuple[int, int]:
    """
    Copy all documents in a single partition using batch operations.
    Returns (success_count, failure_count).
    """
    async def _do_copy() -> tuple[int, int]:
        success_count = 0
        failure_count = 0
        
        # Sort by timestamp within partition
        sorted_docs = sorted(docs, key=lambda d: d.timestamp)
        
        # Get resume point for this partition
        last_ts = progress_state.get_partition_timestamp(partition_id)
        if last_ts:
            to_copy = [doc for doc in sorted_docs if doc.timestamp > last_ts]
        else:
            to_copy = sorted_docs
        
        skipped = len(sorted_docs) - len(to_copy)
        if skipped > 0:
            progress.update(task_id, advance=skipped)
            progress.update(overall_task_id, advance=skipped)
        
        # Process in batches
        for i in range(0, len(to_copy), batch_size):
            batch_docs = to_copy[i:i + batch_size]
            batch_ids = [doc.id for doc in batch_docs]
            
            batch_success, batch_failure = await copy_batch(
                source_client,
                dest_client,
                key_field,
                batch_ids,
                partition_id,
                failure_log,
                console,
            )
            
            async with state_lock:
                success_count += batch_success
                failure_count += batch_failure
                progress_state.copied_count += batch_success
                progress_state.failed_count += batch_failure
                
                # Update timestamp to last doc in batch if any succeeded
                if batch_success > 0:
                    progress_state.set_partition_timestamp(partition_id, batch_docs[-1].timestamp)
                
                # Persist progress and failure log
                state_store.write_copy_progress(progress_state)
                if failure_log.failure_count > 0:
                    state_store.write_failure_log(failure_log)
            
            progress.update(task_id, advance=len(batch_docs))
            progress.update(overall_task_id, advance=len(batch_docs))
        
        return success_count, failure_count
    
    if semaphore:
        async with semaphore:
            return await _do_copy()
    else:
        return await _do_copy()


async def copy_missing_docs(
    source_client: SearchClient,
    dest_client: SearchClient,
    key_field: str,
    scan_result: ScanResult,
    state_store: StateStore,
    console: Console,
    resume: bool = True,
    batch_size: int = 250,
    max_parallelism: Optional[int] = None,
) -> tuple[int, int]:
    """
    Copy all missing documents from source to destination, in parallel by partition.
    Returns (success_count, failure_count).
    
    Args:
        batch_size: Number of documents to copy at a time (default 250)
        max_parallelism: Max concurrent partitions (None = all partitions in parallel)
    
    If resume=True, will skip documents with timestamp <= last_copied_timestamp per partition.
    """
    index_name = scan_result.index_name
    
    # Load or create copy progress
    progress_state: Optional[CopyProgress] = None
    if resume:
        progress_state = state_store.read_copy_progress(index_name)
        if progress_state and progress_state.scan_timestamp != scan_result.timestamp:
            console.print(
                "[yellow]Scan timestamp changed, starting fresh copy[/yellow]"
            )
            progress_state = None
    
    if progress_state is None:
        progress_state = CopyProgress(
            index_name=index_name,
            scan_timestamp=scan_result.timestamp,
        )
    
    # Load or create failure log
    failure_log = state_store.read_failure_log(index_name)
    if failure_log is None or failure_log.scan_timestamp != scan_result.timestamp:
        failure_log = FailureLog(
            index_name=index_name,
            scan_timestamp=scan_result.timestamp,
        )
    
    # Group documents by partition
    docs_by_partition: Dict[int, List[MissingDoc]] = defaultdict(list)
    for doc in scan_result.missing_docs:
        docs_by_partition[doc.partition_id].append(doc)
    
    if not docs_by_partition:
        console.print("[green]No documents to copy[/green]")
        return 0, 0
    
    total_docs = len(scan_result.missing_docs)
    num_partitions = len(docs_by_partition)
    
    console.print(f"[bold]Copying {total_docs:,} documents across {num_partitions} partitions[/bold]")
    console.print(f"  Batch size: {batch_size}, Parallelism: {max_parallelism or 'unlimited'}")
    if progress_state.copied_count > 0:
        console.print(f"  (resuming, {progress_state.copied_count} already copied)")
    
    state_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(max_parallelism) if max_parallelism else None
    
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
        # Overall progress
        overall_task_id = progress.add_task(
            "[bold green]Overall",
            total=total_docs,
        )
        
        # Create tasks for each partition
        tasks = []
        for partition_id in sorted(docs_by_partition.keys()):
            docs = docs_by_partition[partition_id]
            task_id = progress.add_task(
                f"[cyan]Partition {partition_id}",
                total=len(docs),
            )
            
            tasks.append(copy_partition(
                source_client,
                dest_client,
                key_field,
                partition_id,
                docs,
                progress_state,
                failure_log,
                state_store,
                state_lock,
                console,
                progress,
                task_id,
                overall_task_id,
                batch_size,
                semaphore,
            ))
        
        # Run all partitions in parallel
        results = await asyncio.gather(*tasks)
    
    # Sum up results
    total_success = sum(s for s, _ in results)
    total_failure = sum(f for _, f in results)
    
    # Summary
    console.print(f"[bold green]Copy complete![/bold green]")
    console.print(f"  Successful: {progress_state.copied_count:,}")
    if progress_state.failed_count > 0:
        console.print(f"  [red]Failed: {progress_state.failed_count:,}[/red]")
    
    # Clear progress file if all succeeded
    if progress_state.failed_count == 0:
        state_store.clear_copy_progress(index_name)
        state_store.clear_failure_log(index_name)
        console.print("  Progress file cleared")
    else:
        failure_path = state_store.write_failure_log(failure_log)
        console.print(f"  [yellow]Failure log: {failure_path}[/yellow]")
    
    return progress_state.copied_count, progress_state.failed_count
