from __future__ import annotations

from bs4 import BeautifulSoup
import logging
import requests
import unicodedata

logger = logging.getLogger("airflow.task")

def remove_html_tables(content:str):
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

def get_html_content(doc_link: str, edgar_headers:dict) -> str:
    """
    A helper function to support pandas apply. Scrapes doc_link for html content.

    :param doc_link: Page url
    :param edgar_headers: A dictionary of html headers to pass to the EDGAR endpoint.
    :return: Extracted plain text from html without any tables.
    """
    content = requests.get(doc_link, headers=edgar_headers)
    
    if content.ok:
        content_type = content.headers['Content-Type']
        if content_type == 'text/html':
            content = remove_html_tables(content.text)
        else:
            logger.warning(f"Unsupported content type ({content_type}) for doc {doc_link}.  Skipping.")
            content = None
    else:
        logger.warning(f"Unable to get content.  Skipping. Reason: {content.status_code} {content.reason}")
        content = None
    
    return content

def get_10q_link(accn: str, cik_number: str, edgar_headers:dict) -> str:
    """
    Given an Accn number from SEC filings index, returns the URL of the 10-Q document.

    :param accn: account number for the filing
    :param cik_number: SEC Central Index Key for the company
    :param edgar_headers: A dictionary of html headers to pass to the EDGAR endpoint.
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
