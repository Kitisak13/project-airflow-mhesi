"""
Module: extract.py
Description: ทำ Web scraper for World Bank Commodity Markets page เพื่อ download ไฟล์ Monthly Prices ("Pink Sheet") Excel file.
"""

import os
import sys
import logging
import urllib.parse
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

TARGET_URL = "https://www.worldbank.org/en/research/commodity-markets"
DEFAULT_RAW_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw", "latest_monthly_prices.xlsx")
FALLBACK_EXCEL = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "World Bank Commodities Price (Pink Sheet).xlsx")


def get_monthly_prices_url(page_url=TARGET_URL):
    """
    Scrapes the World Bank Commodity Markets page เพื่อ extract ไฟล์ Monthly Prices Excel link.
    """
    logger.info(f"Navigating to Target URL: {page_url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }

    session = requests.Session()
    response = session.get(page_url, headers=headers, timeout=20)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    download_url = None

    # 1. Search within table or link containing "Monthly prices"
    for a in soup.find_all("a"):
        text = a.get_text(strip=True)
        href = a.get("href", "")
        if "monthly prices" in text.lower():
            download_url = href
            logger.info(f"Found matching anchor tag with text: '{text}' -> {href}")
            break

    # 2. Fallback check for Pink Sheet monthly link if exact text match wasn't found
    if not download_url:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ("cmo-historical-data-monthly" in href.lower() or "pinksheet" in href.lower()) and (href.endswith(".xlsx") or href.endswith(".xls")):
                download_url = href
                logger.info(f"Found matching href fallback: {href}")
                break

    if not download_url:
        raise ValueError(f"Could not locate 'Monthly prices' link on page {page_url}. Website structure may have changed.")

    # Resolve relative URL if needed
    if not download_url.startswith("http"):
        download_url = urllib.parse.urljoin(page_url, download_url)

    return download_url


def download_world_bank_data(output_path=DEFAULT_RAW_PATH):
    """
    Downloads the Excel file from World Bank and saves locally.
    Uses fallback local file if internet connection fails.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    try:
        url = get_monthly_prices_url()
        logger.info(f"Starting download from: {url}")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        res = requests.get(url, headers=headers, timeout=60, stream=True)
        res.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in res.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        file_size = os.path.getsize(output_path)
        logger.info(f"Download complete: {output_path} ({file_size / 1024:.2f} KB)")
        return output_path

    except Exception as e:
        logger.warning(f"Failed to scrape/download latest file: {e}")
        if os.path.exists(FALLBACK_EXCEL):
            logger.info(f"Using local master fallback file: {FALLBACK_EXCEL}")
            import shutil
            shutil.copy2(FALLBACK_EXCEL, output_path)
            logger.info(f"Copied fallback file to: {output_path}")
            return output_path
        else:
            raise


if __name__ == "__main__":
    download_world_bank_data()
