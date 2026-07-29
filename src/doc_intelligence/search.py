"""Databricks Vector Search helpers — equivalent to Snowflake's
CORTEX_SEARCH service. Creates/refreshes a Delta Sync Index over the
gold_document_chunks table and exposes a simple similarity-search call
used both by the RAG agent and the Streamlit app.
"""

from databricks.vector_search.client import VectorSearchClient


def get_or_create_endpoint(client: VectorSearchClient, endpoint_name: str):
    existing = [e["name"] for e in client.list_endpoints().get("endpoints", [])]
    if endpoint_name not in existing:
        client.create_endpoint(name=endpoint_name, endpoint_type="STANDARD")
    return endpoint_name


def get_or_create_delta_sync_index(
    client: VectorSearchClient,
    endpoint_name: str,
    source_table_fullname: str,
    index_fullname: str,
    primary_key: str = "chunk_id",
    embedding_source_column: str = "chunk_text",
    embedding_model_endpoint: str = "databricks-gte-large-en",
):
    """Creates a Delta Sync (managed embeddings) index if it doesn't
    already exist, then triggers a sync. Delta Sync indexes stay in
    sync with the source Delta table automatically once created with
    pipeline_type='TRIGGERED' or 'CONTINUOUS'.
    """
    existing = [i["name"] for i in client.list_indexes(endpoint_name).get("vector_indexes", [])]

    if index_fullname not in existing:
        index = client.create_delta_sync_index(
            endpoint_name=endpoint_name,
            source_table_name=source_table_fullname,
            index_name=index_fullname,
            pipeline_type="TRIGGERED",
            primary_key=primary_key,
            embedding_source_column=embedding_source_column,
            embedding_model_endpoint_name=embedding_model_endpoint,
        )
    else:
        index = client.get_index(endpoint_name=endpoint_name, index_name=index_fullname)
        index.sync()

    return index


def similarity_search(
    client: VectorSearchClient,
    endpoint_name: str,
    index_fullname: str,
    query_text: str,
    num_results: int = 5,
    filters: dict | None = None,
    columns: list | None = None,
):
    index = client.get_index(endpoint_name=endpoint_name, index_name=index_fullname)
    columns = columns or ["chunk_id", "doc_id", "file_name", "document_type", "chunk_text"]
    return index.similarity_search(
        query_text=query_text,
        columns=columns,
        num_results=num_results,
        filters=filters,
    )
