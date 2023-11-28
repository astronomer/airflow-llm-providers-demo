import datetime
import json
from pathlib import Path

import pandas as pd
from include.tasks import split, ingest
from include.tasks.extract import html

from airflow.decorators import dag, task
# from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from include.utils.weaviate.hooks.weaviate import _WeaviateHook


edgar_headers={"User-Agent": "test1@test1.com"}

_WEAVIATE_CONN_ID = "weaviate_default"

weaviate_hook = _WeaviateHook(_WEAVIATE_CONN_ID)
weaviate_client = weaviate_hook.get_client()

tickers = ['f', 'tsla']

schema_file = Path("include/data/schema.json")

default_args = {"retries": 3, "retry_delay": 30}


@dag(
    schedule_interval=None,
    start_date=datetime.datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
)
def FinSum_Weaviate():
    """
    This DAG loads financial reporting data from the US 
    [Securities and Exchanges Commision (SEC) EDGAR database](https://www.sec.gov/edgar).

    """

    @task.branch
    def check_schema() -> str:
        """
        Check if the current schema includes the requested schema.  The current schema could be a superset
        so check_schema_subset is used recursively to check that all objects in the requested schema are
        represented in the current schema.
        """

        class_objects = json.loads(schema_file.read_text())

        return (
            ["extract_edgar_html"]
            if weaviate_hook.check_schema(class_objects=class_objects)
            else ["create_schema"]
        )

    @task(trigger_rule="none_failed")
    def create_schema(existing: str = "ignore"):
        class_objects = json.loads(schema_file.read_text())
        weaviate_hook.create_schema(class_objects=class_objects, existing=existing)

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

    _check_schema = check_schema()
    _create_schema = create_schema(existing="ignore")

    split_html_docs = task(split.split_html).expand(dfs=[html_docs])

    _import_data = (
        task(ingest.import_data_weaviate, retries=10)
        .partial(
            weaviate_conn_id=_WEAVIATE_CONN_ID,
            class_name="tenQ",
            existing="skip",
            batch_params={"batch_size": 1000},
            verbose=True,
        )
        .expand(dfs=[split_html_docs])
    )

    _check_schema >> _create_schema >> html_docs

FinSum_Weaviate()
