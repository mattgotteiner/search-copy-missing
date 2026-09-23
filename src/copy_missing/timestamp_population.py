"""Populate missing synthetic timestamps in an Azure AI Search index."""

import asyncio
import hashlib
from collections import deque
from datetime import datetime, time, timedelta, timezone
from typing import Any
from uuid import uuid4

from azure.search.documents.aio import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchField, SearchFieldDataType

DEFAULT_PAGE_SIZE = 1000
MIN_PAGE_SIZE = 1
MAX_PAGE_SIZE = 1000
MAX_NO_PROGRESS_RETRIES = 30
NO_PROGRESS_RETRY_DELAY = 0.5


def _validate_timestamp_field(
    index_client: SearchIndexClient,
    index_name: str,
    field_name: str,
    require_retrievable: bool = True,
) -> str:
    """Ensure the timestamp field has the capabilities required by its use."""
    index = index_client.get_index(index_name)
    key_field = next((field for field in index.fields if field.key), None)
    if key_field is None:
        raise ValueError(f"No key field found in index '{index_name}'")

    timestamp_field = next((field for field in index.fields if field.name == field_name), None)
    if timestamp_field is None:
        index.fields.append(
            SearchField(
                name=field_name,
                type=SearchFieldDataType.DateTimeOffset,
                facetable=False,
                filterable=True,
                sortable=True,
                hidden=False,
            )
        )
        index_client.create_or_update_index(index)
    else:
        if timestamp_field.type != SearchFieldDataType.DateTimeOffset:
            raise ValueError(
                f"Field '{field_name}' in index '{index_name}' must have type "
                "Edm.DateTimeOffset"
            )
        if not timestamp_field.filterable or not timestamp_field.sortable:
            raise ValueError(
                f"Field '{field_name}' in index '{index_name}' must be filterable "
                "and sortable; update the index definition before populating it"
            )
        if require_retrievable and getattr(timestamp_field, "hidden", False):
            raise ValueError(
                f"Field '{field_name}' in index '{index_name}' must be retrievable "
                "for timestamp scanning; update the index definition"
            )

    return key_field.name


def _timestamp_for_key(key: str, start: datetime, end: datetime) -> str:
    """Return a stable, uniformly distributed timestamp for a key within a range."""
    duration_ms = int((end - start).total_seconds() * 1000)
    digest = hashlib.sha256(key.encode("utf-8")).digest()
    offset_ms = int.from_bytes(digest[:8], "big") % (duration_ms + 1)
    timestamp = start + timedelta(milliseconds=offset_ms)
    return timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


async def populate_timestamps(
    source_index_client: SearchIndexClient,
    destination_index_client: SearchIndexClient,
    search_client: SearchClient,
    index_name: str,
    timestamp_field: str,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> int:
    """Add the timestamp field if needed and merge timestamps into documents missing it.

    Generated values are synthetic and distributed across the current UTC day. They
    provide a sortable partition key but do not represent document modification times.
    """
    if not MIN_PAGE_SIZE <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(
            f"Page size must be between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}"
        )

    key_field = _validate_timestamp_field(
        source_index_client, index_name, timestamp_field
    )
    _validate_timestamp_field(
        destination_index_client,
        index_name,
        timestamp_field,
        require_retrievable=False,
    )
    now = datetime.now(timezone.utc)
    start = datetime.combine(now.date(), time.min, tzinfo=timezone.utc)
    end = datetime.combine(now.date(), time.max, tzinfo=timezone.utc)
    updated_count = 0
    initial_missing_count: int | None = None
    initial_count_captured = False
    recent_page_fingerprints: deque[bytes] = deque(
        maxlen=MAX_NO_PROGRESS_RETRIES
    )
    no_progress_retries = 0

    while True:
        results = await search_client.search(
            search_text="*",
            filter=f"{timestamp_field} eq null",
            select=[key_field],
            session_id=str(uuid4()),
            top=page_size,
            include_total_count=not initial_count_captured,
        )
        if not initial_count_captured:
            initial_missing_count = await results.get_count()
            initial_count_captured = True

        updates: list[dict[str, Any]] = []
        page_keys: list[str] = []
        async for page in results.by_page():
            async for document in page:
                key = document.get(key_field)
                if not isinstance(key, str):
                    raise ValueError(
                        f"Document in index '{index_name}' is missing string key "
                        f"field '{key_field}'"
                    )
                page_keys.append(key)
                updates.append(
                    {
                        key_field: key,
                        timestamp_field: _timestamp_for_key(key, start, end),
                    }
                )

        if not page_keys:
            break

        page_hash = hashlib.sha256()
        for key in sorted(page_keys):
            encoded_key = key.encode("utf-8")
            page_hash.update(len(encoded_key).to_bytes(8, "big"))
            page_hash.update(encoded_key)
        fingerprint = page_hash.digest()
        if fingerprint in recent_page_fingerprints:
            no_progress_retries += 1
            if no_progress_retries >= MAX_NO_PROGRESS_RETRIES:
                raise RuntimeError(
                    "Search continued returning the same documents for timestamp "
                    f"field '{timestamp_field}' after {MAX_NO_PROGRESS_RETRIES} retries"
                )
            await asyncio.sleep(NO_PROGRESS_RETRY_DELAY)
            continue

        recent_page_fingerprints.append(fingerprint)
        no_progress_retries = 0

        indexing_results = await search_client.merge_documents(documents=updates)
        if len(indexing_results) != len(updates):
            raise RuntimeError(
                f"Expected {len(updates)} indexing results but received "
                f"{len(indexing_results)}"
            )
        failures = [result for result in indexing_results if not result.succeeded]
        if failures:
            details = "; ".join(
                f"{result.key}: {result.error_message}" for result in failures[:5]
            )
            raise RuntimeError(
                f"Failed to populate timestamps on {len(failures)} documents: {details}"
            )

        updated_count += len(indexing_results)

    return (
        initial_missing_count
        if initial_missing_count is not None
        else updated_count
    )
