import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
import pandas as pd
from urllib.parse import urlparse, urlencode, parse_qs, urljoin
from datetime import datetime
import time
import random
import os

# ========== CONFIGURATION ==========
INPUT_EXCEL = 'Book1.xlsx'  # first column contains search terms
OUTPUT_SHEET_NAME = 'AKC Rankings'  # results sheet inside INPUT_EXCEL
PAGE_READY_TIMEOUT_SECONDS = 10
GOOGLE_RESULTS_PAGES = 1  # 1 page = top 10, 2 pages = top 20, etc.
RESULTS_PER_PAGE = 10

# Domain(s) to detect as the AKC site
TARGET_DOMAINS = ['sg-akc.com']


def setup_driver():
    # Prefer Selenium Manager (auto-matches installed Chrome) and fall back to undetected-chromedriver
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(options=options)
        driver.maximize_window()
        return driver
    except Exception:
        options = uc.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        uc.Chrome.__del__ = lambda self: None
        driver = uc.Chrome(options=options)
        driver.maximize_window()
        return driver


def wait_until_ready(driver, timeout_seconds=PAGE_READY_TIMEOUT_SECONDS):
    try:
        WebDriverWait(driver, timeout_seconds).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
    except TimeoutException:
        pass
    time.sleep(random.uniform(0.5, 1.2))


def is_unusual_traffic(driver) -> bool:
    try:
        return 'unusual traffic' in driver.page_source.lower()
    except Exception:
        return False


def get_base_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower()
        return netloc[4:] if netloc.startswith('www.') else netloc
    except Exception:
        return ''


def is_target_domain(url: str) -> bool:
    base = get_base_domain(url)
    return any(base.endswith(td) for td in TARGET_DOMAINS)


def _normalize_google_result_href(href: str) -> str:
    # Convert Google redirect links like /url?q=https://target... to the target URL
    try:
        if href.startswith('/url?') or href.startswith('https://www.google.') and '/url?' in href:
            # Build absolute then parse q
            absolute = urljoin('https://www.google.com', href)
            q = parse_qs(urlparse(absolute).query).get('q', [])
            if q:
                return q[0]
        return href
    except Exception:
        return href


def google_search_collect_results(driver, query: str, pages: int) -> list[str]:
    collected_urls: list[str] = []
    for page_index in range(pages):
        start = page_index * RESULTS_PER_PAGE
        params = {'q': query, 'start': start, 'num': RESULTS_PER_PAGE, 'hl': 'en'}
        google_url = f"https://www.google.com/search?{urlencode(params)}"

        try:
            driver.set_page_load_timeout(20)
            driver.get(google_url)
        except TimeoutException:
            try:
                driver.execute_script('window.stop()')
            except Exception:
                pass
        except WebDriverException:
            # transient; try next page or stop
            break

        wait_until_ready(driver)

        if is_unusual_traffic(driver):
            # Back off instead of hammering
            time.sleep(random.randint(30, 60))
            continue

        # Collect anchors from the main results area; normalize redirect links
        anchors = driver.find_elements(By.CSS_SELECTOR, '#search a')
        page_urls = []
        seen = set()
        for a in anchors:
            try:
                href = a.get_attribute('href') or ''
                href = _normalize_google_result_href(href)
                if not href.startswith('http'):
                    continue
                domain = get_base_domain(href)
                # Filter out Google-owned or non-organic domains
                if any(x in domain for x in ['google.', 'gstatic.', 'youtube.', 'webcache.googleusercontent']):
                    continue
                if '/maps' in href or '/search?' in href:
                    continue
                if href not in seen:
                    seen.add(href)
                    page_urls.append(href)
            except Exception:
                continue

        collected_urls.extend(page_urls)
        time.sleep(random.uniform(0.8, 1.6))

    return collected_urls


def find_rank_for_query(driver, query: str, pages: int) -> int | None:
    urls = google_search_collect_results(driver, query, pages)
    for index, url in enumerate(urls, start=1):
        if is_target_domain(url):
            return index
    return None


def read_search_terms_from_excel(path: str) -> list[str]:
    df = pd.read_excel(path)
    if df.empty:
        return []
    return df.iloc[:, 0].dropna().astype(str).tolist()


def write_results_to_excel(path: str, rows: list[dict]):
    # Append-or-create sheet `AKC Rankings`
    if not rows:
        return

    try:
        with pd.ExcelWriter(path, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            try:
                existing = pd.read_excel(path, sheet_name=OUTPUT_SHEET_NAME)
            except Exception:
                existing = pd.DataFrame()

            new_df = pd.DataFrame(rows, columns=['Search term', 'Results Ranking', 'Date'])
            combined = pd.concat([existing, new_df], ignore_index=True)
            # Rewrite the sheet to keep it clean
            writer.book.remove(writer.book[OUTPUT_SHEET_NAME]) if OUTPUT_SHEET_NAME in writer.book.sheetnames else None
            combined.to_excel(writer, sheet_name=OUTPUT_SHEET_NAME, index=False)
    except FileNotFoundError:
        # Create workbook if it does not exist
        with pd.ExcelWriter(path, engine='openpyxl', mode='w') as writer:
            new_df = pd.DataFrame(rows, columns=['Search term', 'Results Ranking', 'Date'])
            new_df.to_excel(writer, sheet_name=OUTPUT_SHEET_NAME, index=False)


def main():
    terms = read_search_terms_from_excel(INPUT_EXCEL)
    if not terms:
        print('No search terms found in the first column of the Excel file.')
        return

    driver = setup_driver()
    results_rows: list[dict] = []
    today = datetime.now().strftime('%Y-%m-%d')

    try:
        for term in terms:
            rank = find_rank_for_query(driver, term, GOOGLE_RESULTS_PAGES)
            results_rows.append({
                'Search term': term,
                'Results Ranking': rank if rank is not None else 'Not Found',
                'Date': today,
            })
            # small human-like pause
            time.sleep(random.uniform(1.0, 2.0))
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    write_results_to_excel(INPUT_EXCEL, results_rows)
    print(f"Wrote {len(results_rows)} rows to sheet '{OUTPUT_SHEET_NAME}' in {INPUT_EXCEL}")


if __name__ == '__main__':
    main()


