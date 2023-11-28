from __future__ import annotations

from airflow.providers.pinecone.hooks.pinecone import PineconeHook
from airflow.providers.openai.hooks.openai import OpenAIHook
from include.utils.weaviate.hooks.weaviate import _WeaviateHook
import logging
import pandas as pd
import uuid

logger = logging.getLogger("airflow.task")

def import_data_weaviate(
    weaviate_conn_id: str,
    dfs: list[pd.DataFrame],
    class_name: str,
    existing: str = "skip",
    doc_key: str = None,
    uuid_column: str = None,
    vector_column: str = None,
    batch_params: dict = None,
    verbose: bool = True,
):
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and vectorizes with import to weaviate.

    Upsert logic relies on a 'doc_key' which is a uniue representation of the document.  Because documents can
    be represented as multiple chunks (each with a UUID which is unique in the DB) the doc_key is a way to represent
    all chunks associated with an ingested document.

    :param dfs: A list of dataframes from downstream dynamic tasks
    :param class_name: The name of the class to import data.  Class should be created with weaviate schema.
        type class_name: str
    :param existing: Whether to 'upsert', 'skip' or 'replace' any existing documents.  Default is 'skip'.
    :param doc_key: If using upsert you must specify a doc_key which uniquely identifies a document which may or
        may not include multiple (unique) chunks.
    :param vector_column: For pre-embedded data specify the name of the column containing the embedding vector
    :param uuid_column: For data with pre-genenerated UUID specify the name of the column containing the UUID
    """

    weaviate_hook = _WeaviateHook(weaviate_conn_id)

    df = pd.concat(dfs, ignore_index=True)

    df, uuid_column = weaviate_hook.generate_uuids(
        df=df, class_name=class_name, uuid_column=uuid_column, vector_column=vector_column
        )

    duplicates = df[df[uuid_column].duplicated()]
    if len(duplicates) > 0:
        logger.error(f"Duplicate rows found. {duplicates}")

    weaviate_hook.ingest_data(
        df=df, 
        class_name=class_name, 
        existing=existing,
        doc_key=doc_key,
        uuid_column=uuid_column,
        vector_column=vector_column,
        batch_params=batch_params,
        verbose=verbose
    )

def import_data_pinecone(
    pinecone_hook: PineconeHook,
    openai_hook: OpenAIHook,
    dfs: list[pd.DataFrame],
    index_name: str,
):
    """
    This task concatenates multiple dataframes from upstream dynamic tasks and vectorizes with import to pinecone.

    :param dfs: A list of dataframes from downstream dynamic tasks
    :param index_name: The name of the index to import data. 
    :param pinecone_hook: An instantiated PineconeHook
    :param openai_hook: An instantiated OpenAIHook
    """

    df = pd.concat(dfs, ignore_index=True)

    df["metadata"] = df.drop(["content"], axis=1).to_dict('records')

    df["id"] = df.content.apply(lambda x: str(uuid.uuid5(name=x+index_name, namespace=uuid.NAMESPACE_DNS)))

    df["values"] = df.content.apply(
        lambda x: openai_hook.create_embeddings(text=x, model="text-embedding-ada-002")
        )
    
    data = list(df[["id", "values", "metadata"]].itertuples(index=False, name=None))
    
    pinecone_hook.upsert_data_async(
        data=data,
        index_name=index_name, 
        async_req=True, 
        pool_threads=30,
        )

