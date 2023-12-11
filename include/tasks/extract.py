from __future__ import annotations

from airflow.exceptions import AirflowException
from include.tasks import html_helpers
import logging
import requests
import pandas as pd

logger = logging.getLogger("airflow.task")


def extract_10q(ticker: str, edgar_headers:dict) -> pd.DataFrame:
        """
        This task pulls 10-Q statements from the [SEC Edgar database](https://www.sec.gov/edgar/searchedgar/companysearch)

        :param ticker: ticker symbol of company 
        :param cik_number: optionally cik_number instead of ticker symbol
        :param edgar_headers: A dictionary of html headers to pass to the EDGAR endpoint.
        :return: A dataframe
        """

        logger.info(f"Extracting documents for ticker {ticker}.")

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
            link_10q = html_helpers.get_10q_link(
                accn=form.get("accn"), cik_number=cik_number, edgar_headers=edgar_headers
            )
            docs.append({
                "docLink": link_10q, 
                "tickerSymbol": ticker,
                "cikNumber": cik_number,
                "fiscalYear": form.get("fy"), 
                "fiscalPeriod": form.get("fp")
                })
            
        df = pd.DataFrame(docs)

        df["content"] = df.docLink.apply(lambda x: html_helpers.get_html_content(
            doc_link=x, edgar_headers=edgar_headers)
        )
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        return df

    