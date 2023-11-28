from __future__ import annotations

from bs4 import BeautifulSoup
import logging
import pandas as pd
import requests
from airflow.exceptions import AirflowException
import unicodedata
logger = logging.getLogger("airflow.task")

def remove_tables(content:str):
    soup = BeautifulSoup(content, "lxml")

    for table in soup.find_all("table"):
        _ = table.replace_with(" ")
    soup.smooth()
    
    clean_text = unicodedata.normalize("NFKD", soup.text)

    return clean_text

def get_html_content(doc_link: str, headers: dict) -> str:
    content = requests.get(doc_link, headers=headers)
    
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

def get_10q_link(accn: str, url_base: str, cik_number: str, headers: dict) -> str:
    
    link_base = f"{url_base}{cik_number}/{accn.replace('-','')}/"

    filing_summary = requests.get(f"{link_base}{accn}-index.html", headers=headers)

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

def extract_edgar_html(headers: dict, ticker: str | None = None, cik_number: str | None = None) -> pd.DataFrame:
    """
    This task pulls 10-Q statements from the [SEC Edgar database](https://www.sec.gov/edgar/searchedgar/companysearch)
    

    :param: ticker 

    :return: A dataframe
    """

    url_base = f"https://www.sec.gov/Archives/edgar/data/"

    if not (ticker or cik_number):
        logger.error("Must provide either ticker or CIK number.")
        raise AirflowException("Must provide either ticker or CIK number.")

    company_list = requests.get(url="https://www.sec.gov/files/company_tickers.json", headers=headers)

    if company_list.ok:
        company_list = list(company_list.json().values())
    else:
        raise AirflowException()
    
    if not cik_number:
        cik_numbers = [item for item in company_list if item.get("ticker") == ticker.upper()]
    
        if len(cik_numbers) != 1:
            raise ValueError("Provided ticker symbol is not available.")
        else:
            cik_number = str(cik_numbers[0]['cik_str'])
    
    if not ticker:
        tickers = [item for item in company_list if item.get("cik_str") == int(cik_number)]
    
        if len(tickers) != 1:
            raise ValueError("Provided cik_number symbol is not available.")
        else:
            ticker = tickers[0]['ticker']

    company_facts = requests.get(
        f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_number.zfill(10)}.json", headers=headers
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
        logger.error(f"Could not get company filing information for ticker: {ticker}, cik: {cik_number}. Reason: {company_facts.status_code} {company_facts.reason}")
        raise AirflowException(f"Could not get company filing information for ticker: {ticker}, cik: {cik_number}.")
        
    docs = []
    for form in forms_10q:
        link_10q = get_10q_link(
            accn=form.get("accn"), url_base=url_base, cik_number=cik_number, headers=headers
            )
        docs.append({
            "docLink": link_10q, 
            "ticker": ticker,
            "cik_number": cik_number,
            "fiscal_year": form.get("fy"), 
            "fiscal_period": form.get("fp")
            })
        
    df = pd.DataFrame(docs)

    df["content"] = df.docLink.apply(lambda x: get_html_content(doc_link=x, headers=headers))
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    return df