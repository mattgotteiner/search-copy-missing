"""Serial document copier from persisted scan results."""

import asyncio
import time
from typing import Callable, Optional, TypeVar

from azure.core.exceptions import (
    HttpResponseError,
    ServiceRequestError,
    ServiceResponseError,
)
from azure.search.documents import SearchClient
from rich.console import Console
from rich.progress import Progress, TaskID

from .state import CopyProgress, ScanResult, StateStore

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
    """Execute an operation with exponential backoff retry."""
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return operation()
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


async def copy_missing_docs(
    source_client: SearchClient,
    dest_client: SearchClient,
    key_field: str,
    scan_result: ScanResult,
    state_store: StateStore,
    console: Console,
    resume: bool = True,
) -> tuple[int, int]:
    """
    Copy all missing documents from source to destination, one at a time.
    Returns (success_count, failure_count).
    
    If resume=True, will skip documents already copied in a previous run.
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
    
    # Determine which docs still need to be copied
    already_copied = set(progress_state.copied_ids)
    already_failed = set(progress_state.failed_ids)
    
    to_copy = [
        doc for doc in scan_result.missing_docs
        if doc.id not in already_copied and doc.id not in already_failed
    ]
    
    if not to_copy:
        if already_copied:
            console.print(f"[green]All {len(already_copied)} documents already copied[/green]")
        else:
            console.print("[green]No documents to copy[/green]")
        return len(already_copied), len(already_failed)
    
    console.print(f"[bold]Copying {len(to_copy)} documents[/bold]")
    if already_copied:
        console.print(f"  (skipping {len(already_copied)} already copied)")
    
    success_count = len(already_copied)
    failure_count = len(already_failed)
    
    with Progress(console=console) as progress:
        task = progress.add_task("[cyan]Copying...", total=len(to_copy))
        
        for doc in to_copy:
            success = await copy_document(
                source_client,
                dest_client,
                key_field,
                doc.id,
                console,
            )
            
            if success:
                success_count += 1
                progress_state.copied_ids.append(doc.id)
            else:
                failure_count += 1
                progress_state.failed_ids.append(doc.id)
            
            # Persist progress after each document (for resume)
            state_store.write_copy_progress(progress_state)
            progress.update(task, advance=1)
    
    # Summary
    console.print(f"[bold green]Copy complete![/bold green]")
    console.print(f"  Successful: {success_count:,}")
    if failure_count > 0:
        console.print(f"  [red]Failed: {failure_count:,}[/red]")
    
    # Clear progress file if all succeeded
    if failure_count == 0:
        state_store.clear_copy_progress(index_name)
        console.print("  Progress file cleared")
    
    return success_count, failure_count
