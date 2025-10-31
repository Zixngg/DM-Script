# FDW Email scraper using Selenium (Visible Browser, Reliable Navigation & CAPTCHA Handling)

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, WebDriverException
from multiprocessing import Process
import undetected_chromedriver as uc
import pandas as pd
import time
import random
import re
import sys
import glob
import os
import signal
import sys
from urllib.parse import urlparse
from datetime import datetime, timedelta
from multiprocessing import Event
terminate_event = Event()

# ========== CONFIGURATION ==========
INPUT_EXCEL = 'Book1.xlsx'
OUTPUT_EXCEL = 'rename_Contacts_Emails.xlsx'
DELAY_RANGE = (5, 10)
LONG_BREAK_SEARCH_RANGE = (15, 20)
LONG_BREAK_DURATION_RANGE = (60, 240)
RECAPTCHA_SLEEP_RANGE = (600, 1200)  # backoff when CAPTCHA detected
PAGE_READY_TIMEOUT = 10
PAGE_LOAD_TIMEOUT = 20

# Domains to skip
SEARCH_RESULT_BLACKLIST = [
    'cloudwaysapps.com', 'sgpbusiness.com', 'directory.sg',
    'yellowpages.com.sg', 'yellowpages.sg', 'hotfrog.sg',
    'locanto.sg', 'bizdir.sg'
]
EMAIL_BLACKLIST_DOMAINS = SEARCH_RESULT_BLACKLIST.copy()

import openpyxl

def save_checkpoint(local_results, local_skipped, worker_id):
    filename = f"worker_output_{worker_id}.xlsx"
    if not local_results and not local_skipped:
        return  # Nothing new to save

    try:
        # Read existing contacts
        if os.path.exists(filename):
            try:
                contacts_df = pd.read_excel(filename, sheet_name="Contacts")
            except:
                contacts_df = pd.DataFrame()
            try:
                skipped_df = pd.read_excel(filename, sheet_name="Skipped URL")
            except:
                skipped_df = pd.DataFrame()
        else:
            contacts_df = pd.DataFrame()
            skipped_df = pd.DataFrame()

        # Combine and deduplicate
        all_contacts = pd.concat([contacts_df, pd.DataFrame(local_results)], ignore_index=True).drop_duplicates()
        all_skipped = pd.concat([skipped_df, pd.DataFrame(local_skipped)], ignore_index=True).drop_duplicates()

        with pd.ExcelWriter(filename, engine='openpyxl', mode='w') as writer:
            if not all_contacts.empty:
                all_contacts.to_excel(writer, sheet_name="Contacts", index=False)
            if not all_skipped.empty:
                all_skipped.to_excel(writer, sheet_name="Skipped URL", index=False)

        print(f"[Worker {worker_id}] Saved results to {filename}")
        print(f"[Worker {worker_id}] Saving {len(all_contacts)} results, {len(all_skipped)} skipped entries")

    except Exception as e:
        print(f"[Worker {worker_id}] Failed to save: {e}")

def process_company(name, local_skipped, worker_id, visited_websites_set):

    """Process a single company and return its results"""
    all_rows = []

    def save_callback(domain_data):
        # Capture rows for return
        for domain, data in domain_data.items():
            urls = list(sorted(data['urls']))
            emails = list(sorted(data['emails']))
            contacts = list(sorted(data['contacts']))
            addresses = list(sorted(data['addresses']))
            max_len = max(len(urls), len(emails), len(contacts), len(addresses))
            for i in range(max_len):
                row = {
                    'Search Term': name if i == 0 else '',
                    'Company Name': (
                        data.get('company_name', extract_company_name_from_url(urls[i]))
                        if i == 0 and i < len(urls) and urls[i]
                        else ''
                    ),
                    'Website': urls[i] if i < len(urls) else '',
                    'Emails': emails[i] if i < len(emails) else '',
                    'Contacts': contacts[i] if i < len(contacts) else '',
                    'Address': addresses[i] if i < len(addresses) else ''
                }
                all_rows.append(row)

        # Save with new rows
        save_checkpoint(all_rows, local_skipped, worker_id)

    # Set up driver for this company
    driver = setup_driver()

    try:
        print(f"{timestamp()} Processing: {name}")
        base = re.sub(r"Pte\.? Ltd\.?|Limited", "", name, flags=re.I).strip()
        driver, visited, domain_data = google_search_and_navigate(
            driver, f"{base}", local_skipped,
            save_callback=save_callback, visited_sites=visited_websites_set, worker_id=worker_id
        )

    except Exception as e:
        print(f"[ERROR] Error processing company {name}: {e}")

    finally:
        try:
            driver.quit()
        except:
            pass

    print(f"[DEBUG] Returning {len(all_rows)} rows for '{name}'")
    return all_rows


# Load additional blacklist sheets if present
def load_blacklists():
    global SEARCH_RESULT_BLACKLIST, EMAIL_BLACKLIST_DOMAINS
    try:
        sheets = pd.read_excel(INPUT_EXCEL, sheet_name=['SearchBlacklist','EmailBlacklist'])
        sb = sheets.get('SearchBlacklist', pd.DataFrame())
        eb = sheets.get('EmailBlacklist', pd.DataFrame())
        if not sb.empty:
            SEARCH_RESULT_BLACKLIST = sb.iloc[:, 0].dropna().astype(str).tolist()
        if not eb.empty:
            EMAIL_BLACKLIST_DOMAINS = eb.iloc[:, 0].dropna().astype(str).tolist()
        print(f"[INFO] Loaded {len(SEARCH_RESULT_BLACKLIST)} search blacklist entries and {len(EMAIL_BLACKLIST_DOMAINS)} email blacklist entries from Excel")
    except Exception as e:
        print(f"[WARN] Blacklist load failed: {e}")

# Helpers
def timestamp():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def countdown_timer(seconds):
    end_time = datetime.now() + timedelta(seconds=seconds)
    while seconds > 0:
        hrs, rem = divmod(seconds, 3600)
        mins, secs = divmod(rem, 60)
        print(f"\rWaiting {hrs:02d}:{mins:02d}:{secs:02d} until {end_time.strftime('%H:%M:%S')}", end='', flush=True)
        time.sleep(1)
        seconds -= 1
    print(f"\rWait complete at {timestamp()}")

def monitor_visited_sites(visited_sites, interval=5):
    from urllib.parse import urlparse
    seen_domains = set()
    while not terminate_event.is_set():
        try:
            current_domains = {
                urlparse(url).netloc.lower().replace("www.", "")
                for url in visited_sites if url
            }
            new_domains = current_domains - seen_domains
            if new_domains:
                print(f"\n[Monitor] 🧭 Total Unique Domains Visited: {len(current_domains)}", flush=True)
                seen_domains = current_domains
            time.sleep(interval)
        except Exception as e:
            print(f"\n[Monitor] Error: {e}")
            continue

# Browser setup
def setup_driver():
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    uc.Chrome.__del__ = lambda self: None  # prevent shutdown errors
    driver = uc.Chrome(options=options)  # auto-detect latest Chrome
    driver.maximize_window()
    return driver

# Navigation safety
def wait_ready(driver, timeout=PAGE_READY_TIMEOUT):
    try:
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script('return document.readyState') == 'complete')
    except TimeoutException:
        print(f"[WARN] {timestamp()} page load timed out")
    time.sleep(random.uniform(0.5, 1.5))

def safe_get(driver, url, local_skipped):
    try:
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        driver.get(url)
    except TimeoutException:
        print(f"[WARN] {timestamp()} timeout loading {url}")
        local_skipped.append({"URL": url, "Reason": "Timeout"})
        try:
            driver.execute_script('window.stop()')
        except:
            pass
    except WebDriverException as e:
        print(f"[ERROR] {timestamp()} webdriver error: {e}")
        local_skipped.append({"URL": url, "Reason": f"WebDriverException: {str(e)}"})
    return driver

# CAPTCHA detection
def detect_google_captcha(driver):
    try:
        return 'our systems have detected unusual traffic' in driver.page_source.lower()
    except:
        return False

def extract_company_name_from_url(url):
    try:
        domain = urlparse(url).netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        domain = re.sub(r'\.com(\.\w+)?$', '', domain)
        return domain.strip().title()
    except:
        return ''

# Extraction: emails, contacts, address
def extract_emails(driver, current_url=None, local_skipped=None):
    emails = set()
    try:
        # Extract from mailto
        for a in driver.find_elements(By.XPATH, "//a[contains(@href,'mailto:')]"):
            href = a.get_attribute('href')
            if href:
                email = href.split(':', 1)[1].split('?')[0].strip()
                if re.match(r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$", email):
                    emails.add(email)

        # Extract from text and HTML
        try:
            text = driver.execute_script('return document.body.innerText')
            html = driver.page_source
        except Exception as e:
            if "timeout" in str(e).lower():
                print(f"[WARN] {timestamp()} Page render timeout for {current_url}")
                if current_url and local_skipped is not None:
                    local_skipped.append({"URL": current_url, "Reason": "Page render timeout"})
                return []
            raise  # Re-raise if it's not a timeout error

        candidates = set(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text + html))

        for e in candidates:
            # Skip static files and encoded links
            if re.search(r"\.(jpg|jpeg|png|gif|svg|webp|css|js|woff|ico)(\?|$)", e, re.IGNORECASE):
                continue
            if "%" in e or ".." in e:
                continue
            if not re.match(r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$", e):
                continue
            emails.add(e)

        # Final filter: remove blacklisted domains
        filtered = {
            e for e in emails
            if not any(bad in e.split('@')[-1].lower() for bad in EMAIL_BLACKLIST_DOMAINS)
        }

        return sorted(filtered)

    except Exception as e:
        print(f"[WARN] email extraction error: {e}")
        if current_url and local_skipped is not None:
            local_skipped.append({"URL": current_url, "Reason": f"Email extraction error: {e}"})
        return []

def extract_contacts(driver):
    contacts = set()
    try:
        for a in driver.find_elements(By.XPATH, "//a[contains(@href,'tel:')]"):
            href = a.get_attribute('href')
            if href: contacts.add(href.split(':',1)[1].strip())
        if not contacts:
            text = driver.execute_script('return document.body.innerText')
            contacts.update(re.findall(r"(?:\+65\s?)?[689]\d{3}[-\s]?\d{4}", text))
    except:
        pass
    return list(contacts)

def extract_address(driver):
    try:
        elems = driver.find_elements(By.TAG_NAME,'address')
        if elems: return elems[0].text.strip()
    except:
        pass
    try:
        text = driver.execute_script('return document.body.innerText')
        for line in text.splitlines():
            if re.search(r"\b\d{6}\b", line): return line.strip()
        for line in text.splitlines():
            if 'Singapore' in line: return line.strip()
    except:
        pass
    return ''

def extract_company_name_from_google_result(driver, target_url):
    base_target = get_base_domain(target_url)
    
    # 1. Try right-hand Google knowledge panel (sidebar)
    try:
        ca5rn_blocks = driver.find_elements(By.CSS_SELECTOR, 'div.CA5RN')
        for block in ca5rn_blocks:
            try:
                cite = block.find_element(By.CSS_SELECTOR, 'cite').text.strip()
                if get_base_domain(cite) == base_target:
                    name_elem = block.find_element(By.CSS_SELECTOR, 'span.VuuXrf')
                    if name_elem:
                        return name_elem.text.strip()
            except:
                continue
    except Exception as e:
        print(f"[WARN] Sidebar CA5RN check failed for {target_url}: {e}")

    # 2. Fallback: Main search result match
    try:
        boxes = driver.find_elements(By.CSS_SELECTOR, 'div.yuRUbf')
        for box in boxes:
            try:
                a_tag = box.find_element(By.CSS_SELECTOR, 'a')
                href = a_tag.get_attribute('href')
                if href and get_base_domain(href) == base_target:
                    try:
                        name_elem = box.find_element(By.XPATH, ".//ancestor::div[contains(@class, 'tF2Cxc')]//span[contains(@class, 'VuuXrf')]")
                        if name_elem:
                            return name_elem.text.strip()
                    except:
                        pass
            except:
                continue
    except Exception as e:
        print(f"[WARN] Main results check failed for {target_url}: {e}")

    return ''

def get_base_domain(url):
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except:
        return ""

# Core navigation with retry
def google_search_and_navigate(driver, query, local_skipped, save_callback=None, visited_sites=None, worker_id=None):
    try:
        raw_urls = []
        visited_domains = set()
        visit_counter = 0
        domain_data = {}

        page = 0
        # max_empty_pages = 2
        # empty_page_count = 0

        for page in range(1):
            start = page * 10
            google_url = f"https://www.google.com/search?q={query}&start={start}"
            print(f"{timestamp()} [Worker {worker_id}] loading Google page {page + 1}: {google_url}")
            driver = safe_get(driver, google_url, local_skipped)
            wait_ready(driver)

            # 🆕 Extract company names from Google result blocks before navigation
            company_names_by_domain = {}

            try:
                result_blocks = driver.find_elements(By.CSS_SELECTOR, 'div.CA5RN')
                for block in result_blocks:
                    try:
                        cite_elem = block.find_element(By.CSS_SELECTOR, 'cite')
                        name_elem = block.find_element(By.CSS_SELECTOR, 'span.VuuXrf')
                        if cite_elem and name_elem:
                            href = cite_elem.text.strip().split('›')[0].strip()  # get left side of the ›
                            domain = get_base_domain(href)
                            name = name_elem.text.strip()
                            if domain and name:
                                company_names_by_domain[domain] = name
                                print(f"[DEBUG] 🏷️ Preloaded company name '{name}' for {domain}")
                    except:
                        continue
            except Exception as e:
                print(f"[WARN] Couldn't parse CA5RN blocks: {e}")

            if detect_google_captcha(driver):
                sl = random.randint(*RECAPTCHA_SLEEP_RANGE)
                print(f"[INFO] {timestamp()} captcha detected, sleeping {sl}s")
                countdown_timer(sl)
                try:
                    driver.quit()
                except:
                    pass
                return google_search_and_navigate(setup_driver(), query, local_skipped, save_callback)

            try:
                driver.execute_script("document.querySelectorAll('.sfbg, .S3Uucc, [aria-modal]').forEach(el => el.remove());")
            except Exception as e:
                print(f"[WARN] {timestamp()} failed to remove overlays: {e}")

            elems = driver.find_elements(By.CSS_SELECTOR, 'div.yuRUbf a')
            page_urls = [e.get_attribute('href') for e in elems if e.is_displayed()][:3]  # Only take first 3
            
            # if not page_urls:
            #     empty_page_count += 1
            #     if empty_page_count >= max_empty_pages:
            #         print(f"[INFO] No more results after page {page + 1}. Exiting loop.")
            #         break
            # else:
            #     empty_page_count = 0
            raw_urls.extend(page_urls)

            page += 1
            time.sleep(random.uniform(1.0, 2.5))

        content_blacklist = [
            'vulcanpost.com', 'timeout.com', 'mustsharenews.com', 'thehoneycombers.com',
            'sethlui.com', 'todayonline.com', 'mothership.sg', 'straitstimes.com',
            'tripadvisor.com', 'facebook.com', 'tiktok.com', 'wikipedia.org', 'fairprice.com.sg'
        ]
        company_keywords = query.lower().split()

        valid_urls = [
            u for u in raw_urls
            if u and not any(b in u for b in SEARCH_RESULT_BLACKLIST + content_blacklist)
        ]

        def score_domain(u):
            domain = get_base_domain(u)
            return 0 if any(k in domain for k in company_keywords) else 1

        valid_urls.sort(key=score_domain)

        for url in valid_urls:
            domain = get_base_domain(url)
            if visited_sites is not None:
                visited_sites.append(url)

            if domain not in domain_data:
                domain_data[domain] = {
                    'urls': set(),
                    'emails': set(),
                    'contacts': set(),
                    'addresses': set()
                }

            print(f"{timestamp()} navigating to {url}")
            start_time = time.time()
            driver = safe_get(driver, url, local_skipped)
            wait_ready(driver)
            elapsed = time.time() - start_time

            visit_counter += 1
            print(f"[VISITED #{visit_counter}] {url}")

            domain_data[domain]['urls'].add(url)

            # 🆕 Extract company name from Google result (only if not already set)
            if 'company_name' not in domain_data[domain] and domain in company_names_by_domain:
                domain_data[domain]['company_name'] = company_names_by_domain[domain]
                print(f"[DEBUG] 🏷️ Retrieved from CA5RN preload: {domain_data[domain]['company_name']} for {domain}")

            emails = extract_emails(driver, url, local_skipped)
            contacts = extract_contacts(driver)
            addr = extract_address(driver)

            print(f"[INFO] Emails: {emails}")
            print(f"[INFO] Contacts: {contacts}")
            print(f"[INFO] Address: {addr}")
            total_leads = len(emails) + len(contacts)
            if total_leads > 0:
                print(f"[TIME PER LEAD ⏱️ ] {elapsed / total_leads:.2f}s (for {total_leads} leads)")
            else:
                print(f"[TIME PER LEAD ⏱️ ] No leads found in {elapsed:.2f}s")

            domain_data[domain]['emails'].update(emails)
            domain_data[domain]['contacts'].update(contacts)
            if addr:
                domain_data[domain]['addresses'].add(addr)

            visited_domains.add(domain)

            if save_callback:
                save_callback(domain_data)

            # Follow subpages
            try:
                sublinks = driver.find_elements(By.XPATH,
                    "//a[contains(translate(@href, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'contact') or "
                    "contains(translate(@href, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'locate')]")
                sub_urls = []
                for link in sublinks:
                    sub_url = link.get_attribute('href')
                    if sub_url and sub_url.startswith("http") and get_base_domain(sub_url) == domain:
                        sub_urls.append(sub_url)

                seen = set()
                for sub_url in sub_urls:
                    if visited_sites is not None:
                        visited_sites.append(sub_url)
                    if sub_url not in seen:
                        seen.add(sub_url)
                        print(f"{timestamp()} navigating to {sub_url}")
                        sub_start = time.time()
                        driver = safe_get(driver, sub_url, local_skipped)
                        wait_ready(driver)
                        sub_elapsed = time.time() - sub_start

                        visit_counter += 1
                        print(f"[VISITED #{visit_counter}] {sub_url}")

                        domain_data[domain]['urls'].add(sub_url)
                        emails = extract_emails(driver, sub_url, local_skipped)
                        contacts = extract_contacts(driver)
                        addr = extract_address(driver)

                        print(f"[INFO] (subpage) Emails: {emails}")
                        print(f"[INFO] (subpage) Contacts: {contacts}")
                        print(f"[INFO] (subpage) Address: {addr}")
                        print(f"[TIME TAKEN ⏱️ ] {sub_elapsed:.2f}s")

                        domain_data[domain]['emails'].update(emails)
                        domain_data[domain]['contacts'].update(contacts)
                        if addr:
                            domain_data[domain]['addresses'].add(addr)

                        if save_callback:
                            save_callback(domain_data)

            except Exception as e:
                print(f"[WARN] {timestamp()} error following subpages: {e}")

        return driver, visited_domains, domain_data

    except TimeoutException:
        print(f"[WARN] {timestamp()} navigation to {query} timed out, restarting browser and retrying")
        try:
            driver.quit()
        except:
            pass
        return google_search_and_navigate(setup_driver(), query, local_skipped, save_callback)

    except Exception as e:
        print(f"[ERROR] {timestamp()} error navigating '{query}': {e}, restarting browser...")
        try:
            driver.quit()
        except:
            pass
        return google_search_and_navigate(setup_driver(), query, local_skipped, save_callback)

def worker_run(sublist, worker_id, terminate_event, visited_websites_set):
    start_time = time.time()

    local_skipped = []
    local_results = []

    def save_local_checkpoint():
        save_checkpoint(local_results, local_skipped, worker_id)

    def signal_handler(sig, frame):
        print(f"[Worker {worker_id}] Caught signal {sig}, saving progress...")
        terminate_event.set()
        save_local_checkpoint()
        sys.exit(0)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        total = len(sublist)
        for index, name in enumerate(sublist, start=1):
            if terminate_event.is_set():
                break
            print(f"[Worker {worker_id}] Progress: {index}/{total} — {name}")
            if terminate_event.is_set():
                break
            company_results = process_company(name, local_skipped, worker_id, visited_websites_set)
            print(f"[DEBUG] {timestamp()} {name}: got {len(company_results)} rows from process_company")
            if company_results:
                local_results.extend(company_results)
                save_local_checkpoint()  # Save after each company
                print(f"[Worker {worker_id}] Scraped {len(company_results)} rows from '{name}' and saved.")
    except Exception as e:
        print(f"[Worker {worker_id}] crashed: {e}")
    finally:
        save_local_checkpoint()

if __name__ == '__main__':
    from multiprocessing import Manager
    manager = Manager()
    visited_websites_set = manager.list()

    import threading
    monitor_thread = threading.Thread(
        target=monitor_visited_sites,
        args=(visited_websites_set,),
        daemon=True
    )
    monitor_thread.start()

    df = pd.read_excel(INPUT_EXCEL)
    names = df.iloc[:, 0].dropna().astype(str).tolist()
    chunks = [names[i::3] for i in range(3)]
    processes = []

    def main_signal_handler(sig, frame):
        print(f"\n[Main] Caught signal {sig}, signaling workers to exit...")
        terminate_event.set()

    signal.signal(signal.SIGINT, main_signal_handler)
    signal.signal(signal.SIGTERM, main_signal_handler)

    try:
        load_blacklists()

        for i, chunk in enumerate(chunks):
            p = Process(target=worker_run, args=(chunk, i + 1, terminate_event, visited_websites_set))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

    except KeyboardInterrupt:
        print("\n[Main] KeyboardInterrupt caught — initiating graceful shutdown.")
        main_signal_handler(signal.SIGINT, None)

    print(f"{timestamp()} All workers finished. Merging outputs...")

    all_contacts = []
    all_skipped = []

    for file in glob.glob("worker_output_*.xlsx"):
        try:
            xls = pd.ExcelFile(file)
            if 'Contacts' in xls.sheet_names:
                df = pd.read_excel(xls, 'Contacts')
                if not df.empty:
                    all_contacts.append(df)
            if 'Skipped URL' in xls.sheet_names:
                df = pd.read_excel(xls, 'Skipped URL')
                if not df.empty:
                    all_skipped.append(df)
        except Exception as e:
            print(f"[WARN] Could not read {file}: {e}")

    combined_contacts = pd.concat(all_contacts, ignore_index=True).drop_duplicates()

    email_leads = combined_contacts['Emails'].dropna().astype(str).apply(lambda x: x.strip() != "").sum()
    contact_leads = combined_contacts['Contacts'].dropna().astype(str).apply(lambda x: x.strip() != "").sum()
    total_leads = email_leads + contact_leads

    def extract_domain(url):
        try:
            return urlparse(url).netloc.lower().replace("www.", "")
        except:
            return None

    all_domains = combined_contacts['Website'].dropna().astype(str).map(extract_domain)
    unique_sites = all_domains.dropna().nunique()
    avg_yield = email_leads / unique_sites if unique_sites > 0 else 0
    print(f"[SUMMARY] 📈 Avg. Yield: {avg_yield:.2f} per Unique Site (Emails: {email_leads} / Unique Sites: {unique_sites})")

    print(f"[SUMMARY] ✅ Total Leads Retrieved: {total_leads} (Emails: {email_leads}, Contacts: {contact_leads})")

    if all_contacts or all_skipped:
        with pd.ExcelWriter(OUTPUT_EXCEL, engine='openpyxl') as writer:
            if not combined_contacts.empty:
                combined_contacts.to_excel(writer, sheet_name="Contacts", index=False)
            if all_skipped:
                pd.concat(all_skipped, ignore_index=True).drop_duplicates().to_excel(writer, sheet_name="Skipped URL", index=False)
        print(f"{timestamp()} Merge complete. Final results saved to {OUTPUT_EXCEL}")
    else:
        print(f"{timestamp()} No data files created by workers. Nothing to merge.")
