"""Populate missing synthetic timestamps in an Azure AI Search index."""

import random
from datetime import datetime, time, timedelta, timezone
from typing import Any
from uuid import uuid4

from azure.search.documents.aio import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import SearchField, SearchFieldDataType


def _validate_timestamp_field(
    index_client: SearchIndexClient,
    index_name: str,
    field_name: str,
) -> str:
    """Ensure the timestamp field exists with the capabilities required by scan."""
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

    return key_field.name


def _random_timestamp(start: datetime, end: datetime) -> str:
    """Return a millisecond-precision UTC timestamp uniformly distributed in a range."""
    duration_ms = int((end - start).total_seconds() * 1000)
    timestamp = start + timedelta(milliseconds=random.randint(0, duration_ms))
    return timestamp.isoformat(timespec="milliseconds").replace("+00:00", "Z")


async def populate_timestamps(
    index_client: SearchIndexClient,
    search_client: SearchClient,
    index_name: str,
    timestamp_field: str,
) -> int:
    """Add the timestamp field if needed and merge timestamps into documents missing it.

    Generated values are synthetic and distributed across the current UTC day. They
    provide a sortable partition key but do not represent document modification times.
    """
    key_field = _validate_timestamp_field(index_client, index_name, timestamp_field)
    now = datetime.now(timezone.utc)
    start = datetime.combine(now.date(), time.min, tzinfo=timezone.utc)
    end = datetime.combine(now.date(), time.max, tzinfo=timezone.utc)
    session_id = str(uuid4())
    updated_count = 0

    results = await search_client.search(
        search_text="*",
        filter=f"{timestamp_field} eq null",
        select=[key_field],
        session_id=session_id,
    )

    async for page in results.by_page():
        updates: list[dict[str, Any]] = []
        async for document in page:
            key = document.get(key_field)
            if key is None:
                raise ValueError(
                    f"Document in index '{index_name}' is missing key field '{key_field}'"
                )
            updates.append(
                {
                    key_field: key,
                    timestamp_field: _random_timestamp(start, end),
                }
            )

        if not updates:
            continue

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

    return updated_count
