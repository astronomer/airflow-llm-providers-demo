"""
## Summarize and search financial documents using OpenAI's LLMs and Weaviate vector database

This DAG extracts and splits financial reporting data from the US 
[Securities and Exchanges Commision (SEC) EDGAR database](https://www.sec.gov/edgar) and ingests 
the data to a Weaviate vector database for generative question answering.  The DAG also 
creates and vectorizes summarizations of the 10-Q document.

This DAG is accompanied by a Streamlit UI.  To run the UI follow the instructions in:
https://github.com/astronomer/airflow-llm-providers-demo/blob/main/README.md
"""
from __future__ import annotations

from airflow.decorators import dag, task
from airflow.models.param import Param
from include.utils.weaviate.hooks.weaviate import _WeaviateHook
from include.tasks import extract, split, summarize
import datetime

import logging
import pandas as pd

OPENAI_CONN_ID = "openai_default"
WEAVIATE_CONN_ID = "weaviate_default"

logger = logging.getLogger("airflow.task")

edgar_headers={"User-Agent": "test1@test1.com"}

class_names = ["TenQ", "TenQSummary"]

default_args = {"retries": 3, "retry_delay": 30, "trigger_rule": "none_failed"}


@dag(
    schedule_interval=None,
    start_date=datetime.datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=False,
    default_args=default_args,
    params={
        "ticker": Param(
            default="",
            title="Ticker symbol from a US-listed public company.",
            type="string",
            description="US-listed companies can be found at https://www.sec.gov/file/company-tickers"
        )
    }
)
def FinSum_Weaviate(ticker: str = None):
    """
    This DAG extracts and splits financial reporting data from the US 
    [Securities and Exchanges Commision (SEC) EDGAR database](https://www.sec.gov/edgar) and ingests 
    the data to a Weaviate vector database for generative question answering.  The DAG also 
    creates and vectorizes summarizations of the 10-Q document.

    This DAG is accompanied by a Streamlit UI.  To run the UI follow the instructions in:
    https://github.com/astronomer/airflow-llm-providers-demo/blob/main/README.md
    """

    def check_schemas() -> str:
        """
        Check if the current schema includes the requested schemas.  The current schema could be a superset
        so check_schema_subset is used recursively to check that all objects in the requested schema are
        represented in the current schema.
        """
        
        weaviate_hook = _WeaviateHook("weaviate_default")

        class_objects = get_schema()

        return (
            ["extract_10q"]
            if weaviate_hook.check_schema(class_objects=class_objects)
            else ["create_schemas"]
        )

    def create_schemas():
        """
        Creates the weaviate class schemas.
        """
        
        weaviate_hook = _WeaviateHook("weaviate_default")

        class_objects = get_schema()
        weaviate_hook.create_schema(class_objects=class_objects, existing="ignore")

    def weaviate_ingest(df: pd.DataFrame, class_name: str):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and vectorizes with import to weaviate.

        Upsert logic relies on a 'doc_key' which is a uniue representation of the document.  Because documents can
        be represented as multiple chunks (each with a UUID which is unique in the DB) the doc_key is a way to represent
        all chunks associated with an ingested document.

        :param df: A dataframe from an upstream split task
        :param class_name: The name of the class to import data.  Class should be created with weaviate schema.
            type class_name: str
        """

        weaviate_hook = _WeaviateHook(WEAVIATE_CONN_ID)

        df, uuid_column = weaviate_hook.generate_uuids(df=df, class_name=class_name)

        weaviate_hook.ingest_data(
            df=df, 
            class_name=class_name, 
            existing="skip",
            doc_key="docLink",
            uuid_column=uuid_column,
            batch_params={"batch_size": 100},
            verbose=True
        )

    _check_schema = task.branch(check_schemas)()
    
    _create_schema = task(create_schemas)()

    edgar_docs = task(extract.extract_10q)(ticker=ticker, edgar_headers=edgar_headers)

    split_docs = task(split.split_html)(df=edgar_docs)
    
    task(weaviate_ingest, task_id="import_chunks")(class_name=class_names[0], df=split_docs)

    generate_summary = task(summarize.summarize_openai)(df=split_docs, openai_conn_id=OPENAI_CONN_ID)

    task(weaviate_ingest, task_id="import_summary")(class_name=class_names[1], df=generate_summary)

    _check_schema >> _create_schema >> edgar_docs

FinSum_Weaviate(ticker="")

def get_schema() -> [dict]:
    return [
        {
            "class": "TenQ",
            "description": "SEC filing data form 10-Q",
            "vectorizer": "text2vec-openai",
            "properties": [
                {
                    "name": "content",
                    "dataType": ["text"],
                    "description": "10-Q content extracted as text and split"
                },
                {
                    "name": "docLink",
                    "dataType": ["text"],
                    "description": "Link to the 10-Q filing.",
                    "tokenization": "field"
                },
                {
                    "name": "tickerSymbol",
                    "dataType": ["text"],
                    "description": "SEC ticker symbol for a registered company."
                },
                {
                    "name": "cikNumber",
                    "dataType": ["text"],
                    "description": "The SEC Central Index Key number."
                },
                {
                    "name": "fiscalYear",
                    "dataType": ["int"],
                    "description": "Fiscal year of the filing."
                },
                {
                    "name": "fiscalPeriod",
                    "dataType": ["text"],
                    "description": "Fiscal period (ie quarter name) of the filing."
                }
            ]
        },
        {
            "class": "TenQSummary",
            "description": "Summarization of SEC filing data form 10-Q",
            "vectorizer": "text2vec-openai",
            "properties": [
                {
                    "name": "summary",
                    "dataType": ["text"],
                    "description": "Summary of 10-Q recursively generated from chunks"
                },
                {
                    "name": "docLink",
                    "dataType": ["text"],
                    "description": "Link to the 10-Q filing.",
                    "tokenization": "field"
                },
                {
                    "name": "tickerSymbol",
                    "dataType": ["text"],
                    "description": "SEC ticker symbol for a registered company."
                },
                {
                    "name": "cikNumber",
                    "dataType": ["text"],
                    "description": "The SEC Central Index Key number."
                },
                {
                    "name": "fiscalYear",
                    "dataType": ["int"],
                    "description": "Fiscal year of the filing."
                },
                {
                    "name": "fiscalPeriod",
                    "dataType": ["text"],
                    "description": "Fiscal period (ie quarter name) of the filing."
                }
            ]
        }
    ]