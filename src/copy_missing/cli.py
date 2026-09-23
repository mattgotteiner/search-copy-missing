"""Command-line interface for copy-missing tool."""

import argparse
import asyncio
import sys
from typing import Optional

from rich.console import Console

from .clients import ClientFactory, get_key_field
from .config import AppConfig
from .copier import copy_missing_docs
from .scanner import scan_index
from .state import StateStore
from .timestamp_population import (
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    MIN_PAGE_SIZE,
    populate_timestamps,
)


def parse_page_size(value: str) -> int:
    """Parse a Search response page size within Azure AI Search limits."""
    try:
        page_size = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("page size must be an integer") from exc

    if not MIN_PAGE_SIZE <= page_size <= MAX_PAGE_SIZE:
        raise argparse.ArgumentTypeError(
            f"page size must be between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}"
        )
    return page_size


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser."""
    parser = argparse.ArgumentParser(
        prog="copy-missing",
        description="Find and copy missing documents between Azure AI Search indexes",
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Scan command
    scan_parser = subparsers.add_parser(
        "scan",
        help="Scan to find documents missing from destination",
    )
    scan_parser.add_argument(
        "--index", "-i",
        help="Index name (or set INDEX_NAME env var)",
    )
    scan_parser.add_argument(
        "--partitions", "-p",
        type=int,
        help="Number of partitions for parallel scanning (or set DESIRED_PARTITIONS env var)",
    )
    scan_parser.add_argument(
        "--state-dir", "-s",
        help="Directory to store scan results (or set STATE_DIR env var)",
    )

    # Populate timestamps command
    populate_parser = subparsers.add_parser(
        "populate-timestamps",
        help="Add a timestamp field and populate documents that do not have one",
    )
    populate_parser.add_argument(
        "--index", "-i",
        help="Index name (or set INDEX_NAME env var)",
    )
    populate_parser.add_argument(
        "--page-size",
        type=parse_page_size,
        default=DEFAULT_PAGE_SIZE,
        metavar="N",
        help=(
            f"Documents per response (valid range: {MIN_PAGE_SIZE}-{MAX_PAGE_SIZE}; "
            f"default: {DEFAULT_PAGE_SIZE})"
        ),
    )
    
    # Copy command
    copy_parser = subparsers.add_parser(
        "copy",
        help="Copy missing documents found by scan",
    )
    copy_parser.add_argument(
        "--index", "-i",
        help="Index name (or set INDEX_NAME env var)",
    )
    copy_parser.add_argument(
        "--state-dir", "-s",
        help="Directory where scan results are stored (or set STATE_DIR env var)",
    )
    copy_parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh instead of resuming from previous copy progress",
    )
    
    return parser


async def run_scan(
    config: AppConfig,
    index_name: str,
    num_partitions: int,
    console: Console,
) -> int:
    """Run the scan command."""
    factory = ClientFactory()
    bundle = factory.create_bundle(config.source, config.destination, index_name)
    
    # Auto-detect key field if not configured
    key_field = config.key_field
    if key_field is None:
        console.print("Auto-detecting key field from index definition...")
        key_field = await get_key_field(bundle.source_index, index_name)
        console.print(f"  Key field: {key_field}")
    
    # Create async clients for parallel scanning
    source_async = factory.create_async_search_client(config.source, index_name)
    dest_async = factory.create_async_search_client(config.destination, index_name)
    
    # Run scan with async clients
    async with source_async, dest_async:
        result = await scan_index(
            source_client=source_async,
            dest_client=dest_async,
            timestamp_field=config.timestamp_field,
            key_field=key_field,
            index_name=index_name,
            source_endpoint=config.source.endpoint,
            dest_endpoint=config.destination.endpoint,
            num_partitions=num_partitions,
            console=console,
        )
    
    # Persist results
    state_store = StateStore(config.state_dir)
    path = state_store.write_scan(result)
    console.print(f"Results saved to: {path}")
    
    return 0 if result.missing_count == 0 else 1


async def run_copy(
    config: AppConfig,
    index_name: str,
    resume: bool,
    console: Console,
) -> int:
    """Run the copy command."""
    state_store = StateStore(config.state_dir)
    
    # Load scan results
    scan_result = state_store.read_scan(index_name)
    if scan_result is None:
        console.print(f"[red]No scan results found for index '{index_name}'[/red]")
        console.print(f"  Run 'copy-missing scan --index {index_name}' first")
        return 1
    
    console.print(f"[bold]Loaded scan from:[/bold] {scan_result.timestamp}")
    console.print(f"  Missing docs: {scan_result.missing_count:,}")
    
    if scan_result.missing_count == 0:
        console.print("[green]No missing documents to copy[/green]")
        return 0
    
    # Create async clients for parallel copying
    factory = ClientFactory()
    source_async = factory.create_async_search_client(config.source, index_name)
    dest_async = factory.create_async_search_client(config.destination, index_name)
    
    # Run copy with async clients
    async with source_async, dest_async:
        success, failure = await copy_missing_docs(
            source_client=source_async,
            dest_client=dest_async,
            key_field=scan_result.key_field,
            scan_result=scan_result,
            state_store=state_store,
            console=console,
            resume=resume,
            batch_size=config.copy_batch_size,
            max_parallelism=config.copy_parallelism,
        )
    
    return 0 if failure == 0 else 1


async def run_populate_timestamps(
    config: AppConfig,
    index_name: str,
    page_size: int,
    console: Console,
) -> int:
    """Add synthetic timestamps to documents in the source index that lack them."""
    factory = ClientFactory()
    source_index_client = factory.create_index_client(config.source)
    destination_index_client = None
    try:
        destination_index_client = factory.create_index_client(config.destination)
        search_client = factory.create_async_search_client(config.source, index_name)
        async with search_client:
            updated_count = await populate_timestamps(
                source_index_client=source_index_client,
                destination_index_client=destination_index_client,
                search_client=search_client,
                index_name=index_name,
                timestamp_field=config.timestamp_field,
                page_size=page_size,
            )
    finally:
        source_index_client.close()
        if destination_index_client is not None:
            destination_index_client.close()

    console.print(
        f"[green]Populated timestamps on {updated_count:,} documents "
        f"in index '{index_name}'[/green]"
    )
    return 0


def main() -> int:
    """Main entry point."""
    console = Console()
    parser = create_parser()
    args = parser.parse_args()
    
    try:
        config = AppConfig.from_env()
    except ValueError as e:
        console.print(f"[red]Configuration error: {e}[/red]")
        return 1
    
    # Override config with command-line args
    index_name = args.index or config.index_name
    if not index_name:
        console.print("[red]Index name required. Use --index or set INDEX_NAME env var[/red]")
        return 1
    
    if hasattr(args, "state_dir") and args.state_dir:
        config.state_dir = args.state_dir
    
    if args.command == "scan":
        num_partitions = args.partitions or config.desired_partitions
        return asyncio.run(run_scan(config, index_name, num_partitions, console))
    
    elif args.command == "copy":
        resume = not args.no_resume
        return asyncio.run(run_copy(config, index_name, resume, console))

    elif args.command == "populate-timestamps":
        return asyncio.run(
            run_populate_timestamps(config, index_name, args.page_size, console)
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
