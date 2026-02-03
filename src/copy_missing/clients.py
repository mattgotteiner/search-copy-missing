"""Azure Search client factory."""

from dataclasses import dataclass
from typing import Optional

from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient

from .config import ServiceConfig


@dataclass
class ClientBundle:
    """Bundle of Azure Search clients for source and destination."""
    source_index: SearchIndexClient
    source_search: SearchClient
    destination_index: SearchIndexClient
    destination_search: SearchClient


class ClientFactory:
    """Factory for creating Azure Search clients."""

    def __init__(self):
        self._default_credential: Optional[DefaultAzureCredential] = None

    def _get_credential(self, service: ServiceConfig):
        """Get credential for a service (key or identity-based)."""
        if service.admin_key:
            return AzureKeyCredential(service.admin_key)
        # Use DefaultAzureCredential for identity-based auth
        if self._default_credential is None:
            self._default_credential = DefaultAzureCredential()
        return self._default_credential

    def create_index_client(self, service: ServiceConfig) -> SearchIndexClient:
        """Create a SearchIndexClient for index management operations."""
        credential = self._get_credential(service)
        return SearchIndexClient(endpoint=service.endpoint, credential=credential)

    def create_search_client(
        self, service: ServiceConfig, index_name: str
    ) -> SearchClient:
        """Create a SearchClient for document operations."""
        credential = self._get_credential(service)
        return SearchClient(
            endpoint=service.endpoint,
            index_name=index_name,
            credential=credential,
        )

    def create_bundle(
        self,
        source: ServiceConfig,
        destination: ServiceConfig,
        index_name: str,
    ) -> ClientBundle:
        """Create a complete bundle of clients for source and destination."""
        return ClientBundle(
            source_index=self.create_index_client(source),
            source_search=self.create_search_client(source, index_name),
            destination_index=self.create_index_client(destination),
            destination_search=self.create_search_client(destination, index_name),
        )


async def get_key_field(index_client: SearchIndexClient, index_name: str) -> str:
    """Get the key field name from an index definition."""
    index = index_client.get_index(index_name)
    for field in index.fields:
        if field.key:
            return field.name
    raise ValueError(f"No key field found in index {index_name}")
