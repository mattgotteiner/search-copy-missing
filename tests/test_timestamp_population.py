"""Tests for synthetic timestamp population."""

import argparse
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from azure.search.documents.indexes.models import SearchFieldDataType

from copy_missing.cli import create_parser, parse_page_size
from copy_missing.timestamp_population import (
    _random_timestamp,
    _validate_timestamp_field,
    populate_timestamps,
)


class TimestampPopulationTests(unittest.IsolatedAsyncioTestCase):
    def test_page_size_cli_parameter_defaults_to_1000_and_is_configurable(self):
        parser = create_parser()
        default_args = parser.parse_args(["populate-timestamps", "--index", "items"])
        custom_args = parser.parse_args(
            ["populate-timestamps", "--index", "items", "--page-size", "256"]
        )

        self.assertEqual(default_args.page_size, 1000)
        self.assertEqual(custom_args.page_size, 256)

    def test_page_size_rejects_values_outside_supported_range(self):
        for value in ("0", "1001"):
            with self.subTest(value=value):
                with self.assertRaises(argparse.ArgumentTypeError):
                    parse_page_size(value)

    def test_page_size_accepts_both_range_boundaries(self):
        self.assertEqual(parse_page_size("1"), 1)
        self.assertEqual(parse_page_size("1000"), 1000)

    def test_random_timestamp_is_utc_with_millisecond_precision(self):
        start = datetime(2026, 9, 23, tzinfo=timezone.utc)
        end = datetime(2026, 9, 23, 23, 59, 59, 999000, tzinfo=timezone.utc)

        timestamp = _random_timestamp(start, end)
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

        self.assertGreaterEqual(parsed, start)
        self.assertLessEqual(parsed, end)
        self.assertRegex(timestamp, r"\.\d{3}Z$")
        self.assertEqual(parsed.microsecond % 1000, 0)

    def test_adds_filterable_sortable_datetime_field_when_missing(self):
        index = SimpleNamespace(
            fields=[SimpleNamespace(name="id", key=True)]
        )
        index_client = Mock()
        index_client.get_index.return_value = index

        key_field = _validate_timestamp_field(index_client, "items", "createdAt")

        self.assertEqual(key_field, "id")
        self.assertEqual(index.fields[-1].name, "createdAt")
        self.assertEqual(index.fields[-1].type, SearchFieldDataType.DateTimeOffset)
        self.assertTrue(index.fields[-1].filterable)
        self.assertTrue(index.fields[-1].sortable)
        index_client.create_or_update_index.assert_called_once_with(index)

    def test_rejects_existing_timestamp_field_that_is_not_filterable(self):
        index = SimpleNamespace(
            fields=[
                SimpleNamespace(name="id", key=True),
                SimpleNamespace(
                    name="createdAt",
                    type=SearchFieldDataType.DateTimeOffset,
                    filterable=False,
                    sortable=True,
                ),
            ]
        )
        index_client = Mock()
        index_client.get_index.return_value = index

        with self.assertRaisesRegex(ValueError, "filterable and sortable"):
            _validate_timestamp_field(index_client, "items", "createdAt")

        index_client.create_or_update_index.assert_not_called()

    async def test_merges_only_missing_timestamps(self):
        search_client = AsyncMock()
        search_client.search.side_effect = [
            _search_results([{"id": "one"}, {"id": "two"}]),
            _search_results(),
        ]
        search_client.merge_documents.return_value = [
            SimpleNamespace(succeeded=True, key="one", error_message=None),
            SimpleNamespace(succeeded=True, key="two", error_message=None),
        ]

        index = SimpleNamespace(fields=[SimpleNamespace(name="id", key=True)])
        index_client = Mock()
        index_client.get_index.return_value = index

        count = await populate_timestamps(
            index_client,
            search_client,
            "items",
            "createdAt",
            page_size=256,
        )

        self.assertEqual(count, 2)
        self.assertEqual(search_client.search.await_count, 2)
        self.assertEqual(
            search_client.search.await_args_list[0].kwargs["filter"],
            "createdAt eq null",
        )
        self.assertEqual(search_client.search.await_args_list[0].kwargs["top"], 256)
        updates = search_client.merge_documents.await_args.kwargs["documents"]
        self.assertEqual([item["id"] for item in updates], ["one", "two"])
        self.assertTrue(all(item["createdAt"].endswith("Z") for item in updates))

    async def test_retries_stale_results_without_merging_a_document_twice(self):
        search_client = AsyncMock()
        search_client.search.side_effect = [
            _search_results([{"id": "one"}]),
            _search_results([{"id": "one"}]),
            _search_results(),
        ]
        search_client.merge_documents.return_value = [
            SimpleNamespace(succeeded=True, key="one", error_message=None)
        ]
        index = SimpleNamespace(fields=[SimpleNamespace(name="id", key=True)])
        index_client = Mock()
        index_client.get_index.return_value = index

        with patch(
            "copy_missing.timestamp_population.asyncio.sleep",
            new_callable=AsyncMock,
        ) as sleep:
            count = await populate_timestamps(
                index_client,
                search_client,
                "items",
                "createdAt",
                page_size=1,
            )

        self.assertEqual(count, 1)
        self.assertEqual(search_client.search.await_count, 3)
        search_client.merge_documents.assert_awaited_once()
        sleep.assert_awaited_once_with(0.5)

    async def test_reports_partial_indexing_failures(self):
        search_client = AsyncMock()
        search_client.search.return_value = _search_results([{"id": "one"}])
        search_client.merge_documents.return_value = [
            SimpleNamespace(succeeded=False, key="one", error_message="invalid field")
        ]
        index = SimpleNamespace(fields=[SimpleNamespace(name="id", key=True)])
        index_client = Mock()
        index_client.get_index.return_value = index

        with self.assertRaisesRegex(RuntimeError, "invalid field"):
            await populate_timestamps(index_client, search_client, "items", "createdAt")


async def _async_iter(items):
    for item in items:
        yield item


def _search_results(*pages):
    return SimpleNamespace(
        by_page=lambda: _async_iter([_async_iter(page) for page in pages])
    )


if __name__ == "__main__":
    unittest.main()
