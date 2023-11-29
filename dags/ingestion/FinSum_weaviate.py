from __future__ import annotations

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from include.utils.weaviate.hooks.weaviate import _WeaviateHook
from airflow.providers.openai.hooks.openai import OpenAIHook

from bs4 import BeautifulSoup
import datetime
import json
from langchain.schema import Document
from langchain.text_splitter import (
    HTMLHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)
import logging
import openai as openai_client
import pandas as pd
from pathlib import Path
import requests
import unicodedata

logger = logging.getLogger("airflow.task")

edgar_headers={"User-Agent": "test1@test1.com"}

openai_hook = OpenAIHook("openai_default")
openai_client.api_key = openai_hook._get_api_key()

weaviate_hook = _WeaviateHook("weaviate_default")
weaviate_client = weaviate_hook.get_client()

class_names = ["TenQ", "TenQSummary"]

tickers = ["f", "tsla"]

schema_file = Path("include/data/weaviate_schema.json")

default_args = {"retries": 3, "retry_delay": 30, "trigger_rule": "none_failed"}


@dag(
    schedule_interval=None,
    start_date=datetime.datetime(2023, 9, 27),
    catchup=False,
    is_paused_upon_creation=True,
    default_args=default_args,
)
def FinSum_Weaviate():
    """
    This DAG extracts and splits financial reporting data from the US 
    [Securities and Exchanges Commision (SEC) EDGAR database](https://www.sec.gov/edgar) and ingests 
    the data to a Weaviate vector database for generative question answering.  The DAG also 
    creates and vectorizes summarizations of the 10-Q document.
    """

    def check_schemas() -> str:
        """
        Check if the current schema includes the requested schemas.  The current schema could be a superset
        so check_schema_subset is used recursively to check that all objects in the requested schema are
        represented in the current schema.
        """

        class_objects = json.loads(schema_file.read_text())

        return (
            ["extract"]
            if weaviate_hook.check_schema(class_objects=class_objects)
            else ["create_schemas"]
        )

    def create_schemas():
        """
        Creates the weaviate class schemas.
        """
        class_objects = json.loads(schema_file.read_text())
        weaviate_hook.create_schema(class_objects=class_objects, existing="ignore")

    def remove_tables(content:str):
        """
        Remove all "table" tags from html content leaving only text.

        :param content: html content
        :return: A string of extracted text from html without tables.
        """
        soup = BeautifulSoup(content, "lxml")

        for table in soup.find_all("table"):
            _ = table.replace_with(" ")
        soup.smooth()
        
        clean_text = unicodedata.normalize("NFKD", soup.text)

        return clean_text

    def get_html_content(doc_link: str) -> str:
        """
        A helper function to support pandas apply. Scrapes doc_link for html content.

        :param doc_link: Page url
        :return: Extracted plain text from html without any tables.
        """
        content = requests.get(doc_link, headers=edgar_headers)
        
        if content.ok:
            content_type = content.headers['Content-Type']
            if content_type == 'text/html':
                content = remove_tables(content.text)
            else:
                logger.warning(f"Unsupported content type ({content_type}) for doc {doc_link}.  Skipping.")
                content = None
        else:
            logger.warning(f"Unable to get content.  Skipping. Reason: {content.status_code} {content.reason}")
            content = None
        
        return content

    def get_10q_link(accn: str, cik_number: str) -> str:
        """
        Given an Accn number from SEC filings index, returns the URL of the 10-Q document.

        :param accn: account number for the filing
        :param cik_number: SEC Central Index Key for the company
        :return: Fully-qualified url pointing to a 10-Q filing document.
        """
        
        url_base = f"https://www.sec.gov/Archives/edgar/data/"

        link_base = f"{url_base}{cik_number}/{accn.replace('-','')}/"

        filing_summary = requests.get(f"{link_base}{accn}-index.html", headers=edgar_headers)

        link = None
        if filing_summary.ok:

            soup = BeautifulSoup(filing_summary.content, "lxml")

            for tr in soup.find("table", {"class": "tableFile"}).find_all("tr"):
                for td in tr.find_all('td'):
                    if td.text == "10-Q":
                        link = link_base + tr.find('a').text
        else:
            logger.warn(f"Error extracting accn index. Reason: {filing_summary.status_code} {filing_summary.reason}")

        return link

    def extract(ticker: str) -> pd.DataFrame:
        """
        This task pulls 10-Q statements from the [SEC Edgar database](https://www.sec.gov/edgar/searchedgar/companysearch)

        :param ticker: ticker symbol of company 
        :param cik_number: optionally cik_number instead of ticker symbol
        :return: A dataframe
        """

        company_list = requests.get(
            url="https://www.sec.gov/files/company_tickers.json", 
            headers=edgar_headers)

        if company_list.ok:
            company_list = list(company_list.json().values())
            cik_numbers = [item for item in company_list if item.get("ticker") == ticker.upper()]

            if len(cik_numbers) != 1:
                raise ValueError("Provided ticker symbol is not available.")
            else:
                cik_number = str(cik_numbers[0]['cik_str'])

        else:
            logger.error("Could not access ticker database.")
            logger.error(f"Reason: {company_list.status_code} {company_list.reason}")
            raise AirflowException("Could not access ticker database.")
        
        company_facts = requests.get(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_number.zfill(10)}.json", 
            headers=edgar_headers
            )
            
        if company_facts.ok:
            forms_10q = []
            for fact in company_facts.json()['facts']['us-gaap'].values():
                for currency, units in fact['units'].items():
                    for unit in units:
                        if unit["form"] == "10-Q":
                            forms_10q.append(unit)

            forms_10q = pd.DataFrame(forms_10q)[["accn", "fy", "fp"]].drop_duplicates().to_dict('records')

        else:
            logger.error(f"Could not get company filing information for ticker: {ticker}, cik: {cik_number}.")
            logger.error(f"Reason: {company_facts.status_code} {company_facts.reason}")
            raise AirflowException(f"Could not get company filing information for ticker: {ticker}, cik: {cik_number}.")
            
        docs = []
        for form in forms_10q:
            link_10q = get_10q_link(accn=form.get("accn"), cik_number=cik_number)
            docs.append({
                "docLink": link_10q, 
                "ticker": ticker,
                "cik_number": cik_number,
                "fiscal_year": form.get("fy"), 
                "fiscal_period": form.get("fp")
                })
            
        df = pd.DataFrame(docs)

        df["content"] = df.docLink.apply(lambda x: get_html_content(doc_link=x))
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        return df

    def split(dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and splits the content 
        first with an html splitter and then with a text splitter.

        :param dfs: A list of dataframes from downstream dynamic tasks
        :return: A dataframe 
        """

        headers_to_split_on = [
            ("h2", "h2"),
        ]

        df = pd.concat(dfs, axis=0, ignore_index=True)

        html_splitter = HTMLHeaderTextSplitter(headers_to_split_on)
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=10000, chunk_overlap=200, separators=["\n\n", "\n", " ", ""]
            )

        df["doc_chunks"] = df["content"].apply(lambda x: html_splitter.split_text(text=x))
        df = df.explode("doc_chunks", ignore_index=True)
        df["content"] = df["doc_chunks"].apply(lambda x: x.page_content)

        df["doc_chunks"] = df["content"].apply(
            lambda x: text_splitter.split_documents([Document(page_content=x)])
            )
        df = df.explode("doc_chunks", ignore_index=True)
        df["content"] = df["doc_chunks"].apply(lambda x: x.page_content)

        df.drop(["doc_chunks"], inplace=True, axis=1)
        df.drop_duplicates(subset=["docLink", "content"], keep="first", inplace=True)
        df.reset_index(inplace=True, drop=True)

        return df

    def weaviate_ingest(
        dfs: list[pd.DataFrame],
        class_name: str,
    ):
        """
        This task concatenates multiple dataframes from upstream dynamic tasks and vectorizes with import to weaviate.

        Upsert logic relies on a 'doc_key' which is a uniue representation of the document.  Because documents can
        be represented as multiple chunks (each with a UUID which is unique in the DB) the doc_key is a way to represent
        all chunks associated with an ingested document.

        :param dfs: A list of dataframes from downstream dynamic tasks
        :param class_name: The name of the class to import data.  Class should be created with weaviate schema.
            type class_name: str
        """

        df = pd.concat(dfs, ignore_index=True)

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

        return df[["docLink", "content", uuid_column]]

    def chunk_summarization_openai(content: str):
        
        response = openai_client.ChatCompletion().create(
            model="gpt-3.5-turbo-1106",
            messages=[
                {
                    "role": "system",
                    "content": "You are a highly skilled AI trained in language comprehension and summarization. I would like you to read the following text and summarize it into a concise abstract paragraph. Aim to retain the most important points, providing a coherent and readable summary that could help a person understand the main points of the discussion without needing to read the entire text. Please avoid unnecessary details or tangential points."
                },
                {
                    "role": "user",
                    "content": content
                }
            ],
            temperature=0,
            max_tokens=1000
            )
        if content:=response.get("choices")[0].get("message").get("content"):
            return content
        else:
            return None
    
    def doc_summarization_openai(content: str):
        
        response = openai_client.ChatCompletion().create(
            model="gpt-4-1106-preview",
            messages=[
                {
                    "role": "system",
                    "content": "You are a highly skilled AI trained in language comprehension and summarization. I would like you to read the following text and summarize it into a concise abstract paragraph. Aim to retain the most important points, providing a coherent and readable summary that could help a person understand the main points of the discussion without needing to read the entire text. Please avoid unnecessary details or tangential points."
                },
                {
                    "role": "user",
                    "content": content
                }
            ],
            temperature=0,
            max_tokens=1000
            )
        if content:=response.get("choices")[0].get("message").get("content"):
            return content
        else:
            return None
        
    def summarize_openai(dfs: [pd.DataFrame]) -> pd.DataFrame:

        df = pd.concat(dfs, ignore_index=True)

        df["chunk_summary"] = df.content.apply(chunk_summarization_openai)

        summaries_df = df.groupby("docLink").chunk_summary.apply("\n".join).reset_index()
        summaries_df["summary"] = summaries_df.chunk_summary.apply(doc_summarization_openai)
        summaries_df.drop("chunk_summary", axis=1, inplace=True)

        summary_df = df.drop(["content", "chunk_summary", "id"], axis=1).drop_duplicates().merge(summaries_df)
        summary_df.iloc[0]

    _check_schema = task.branch(check_schemas)()
    
    _create_schema = task(create_schemas)()

    edgar_docs = task(extract).expand(ticker=tickers)

    split_docs = task(split).expand(dfs=[edgar_docs])
    
    task(weaviate_ingest, task_id="import_chunks", retries=10)\
        .partial(class_name=class_names[0])\
        .expand(dfs=[split_docs])

    generate_summary = task(summarize_openai).expand(dfs=[split_docs])

    task(weaviate_ingest, task_id="import_summary", retries=10)\
        .partial(class_name=class_names[1])\
        .expand(dfs=[generate_summary])

    _check_schema >> _create_schema >> edgar_docs

FinSum_Weaviate()
