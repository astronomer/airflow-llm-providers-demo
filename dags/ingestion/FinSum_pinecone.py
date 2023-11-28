import datetime
import json
from pathlib import Path

import pandas as pd
from include.tasks import split, ingest
from include.tasks.extract import html

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.pinecone.hooks.pinecone import PineconeHook
from airflow.providers.openai.hooks.openai import OpenAIHook
from pinecone import IndexDescription


edgar_headers={"User-Agent": "test1@test1.com"}

pinecone_hook = PineconeHook("pinecone_default")
openai_hook = OpenAIHook("openai_default")

index_name="tenq"

tickers = ["f", "tsla"]

default_args = {"retries": 3, "retry_delay": 30}


@dag(
    schedule_interval=None,
    start_date=datetime.datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
)
def FinSum_Pinecone():
    """
    This DAG loads financial reporting data from the US 
    [Securities and Exchanges Commision (SEC) EDGAR database](https://www.sec.gov/edgar).

    """

    @task.branch
    def check_index() -> [str]:
        """
        Check if the current schema includes the requested schema.  The current schema could be a superset
        so check_schema_subset is used recursively to check that all objects in the requested schema are
        represented in the current schema.
        """

        if index_name in pinecone_hook.list_indexes():
            tenq_index = pinecone_hook.describe_index(index_name=index_name)
            try:
                assert tenq_index.metric == "cosine"
                assert tenq_index.replicas == 1 
                assert tenq_index.dimension == 1536
                assert tenq_index.shards == 1
                assert tenq_index.pods == 1
                assert tenq_index.pod_type == "starter"
                assert tenq_index.status["ready"]
                assert tenq_index.status["state"] == "Ready"
                return ["extract_edgar_html"]
            except Exception as e:
                raise AirflowException(f"Index {index_name} exists and differs from requested index.")
        else:
            return ["create_index"]

    @task(trigger_rule="none_failed")
    def create_index(existing: str = "ignore"):
        
        if index_name in pinecone_hook.list_indexes():
            if existing == "replace":
                pinecone_hook.delete_index(index_name=index_name)
            elif existing == "ignore":
                return 
        else:
            pinecone_hook.create_index(
                index_name=index_name, 
                metric="cosine", 
                replicas=1, 
                dimension=1536, 
                shards=1, 
                pods=1, 
                pod_type='starter', 
                source_collection='',
            )

    @task(trigger_rule="none_failed")
    def extract_edgar_html(ticker: str):

        parquet_file = Path(f"include/data/html/{ticker}.parquet")
        parquet_file.parent.mkdir(parents=True, exist_ok=True)

        try:
            df = pd.read_parquet(parquet_file)
        except Exception:
            df = html.extract_edgar_html(ticker=ticker, headers=edgar_headers)
            df.to_parquet(parquet_file)

        return df

    html_docs = extract_edgar_html.expand(ticker=tickers)

    _check_index = check_index()
    _create_index = create_index(existing="ignore")

    split_html_docs = task(split.split_html).expand(dfs=[html_docs])


    _import_data = (
        task(ingest.import_data_pinecone, retries=10)
        .partial(
            pinecone_hook=pinecone_hook,
            openai_hook=openai_hook,
            index_name=index_name,
        )
        .expand(dfs=[split_html_docs])
    )

    _check_index >> _create_index >> html_docs

FinSum_Pinecone()
