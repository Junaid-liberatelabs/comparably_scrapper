
import os
import json
import re
import time
import random
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import sys

# --- Pydantic Models ---
from pydantic import BaseModel, Field, HttpUrl, ValidationError

class Review(BaseModel):
    text: str
    date: datetime

class ReviewSection(BaseModel):
    section_name: str
    reviews: List[Review]

class Question(BaseModel):
    question_text: str
    review_section: ReviewSection

class ScrapeRequest(BaseModel):
    urls: List[HttpUrl]
    start_date_str: Optional[str] = Field(None, description="Optional start date (YYYY-MM-DD) for reviews.")
    end_date_str: Optional[str] = Field(None, description="Optional end date (YYYY-MM-DD) for reviews.")

# --- FastAPI ---
from fastapi import FastAPI, HTTPException, Body
app = FastAPI(
    title="Comparably Scraper API - Selenium CatNav, Curl-CFFI Q-ReviewNav",
    description="Selenium for Category page navigation, Curl-CFFI for Q-Review pagination. Optimized.",
    version="2.1.0" # Updated version for optimizations
)


from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- curl_cffi for robust HTTP requests ---
from curl_cffi.requests import Session as CurlCffiSession
from curl_cffi.requests import RequestsError

# --- User Agent ---
try:
    from fake_useragent import UserAgent
    ua = UserAgent()
except ImportError:
    print("Warning: fake-useragent not installed. Using a generic User-Agent.")
    ua = None

# --- HTML Parser Choice ---
try:
    import lxml
    DEFAULT_HTML_PARSER = "lxml"
    print("INFO: Using lxml for HTML parsing.")
except ImportError:
    print("Warning: lxml not installed. Falling back to html.parser. For potentially faster parsing, consider installing lxml: pip install lxml")
    DEFAULT_HTML_PARSER = "html.parser"


# --- Constants ---
REVIEW_CATEGORIES = ["leadership", "compensation", "team", "environment", "outlook"]
MAX_CATEGORY_PAGES = 15
MAX_REVIEW_PAGES_PER_QUESTION = 10
SELENIUM_PAGE_TIMEOUT_S = 30
SELENIUM_ELEMENT_TIMEOUT_S = 18 # For waiting for elements on a page
CURL_REQUEST_TIMEOUT_S = 30
CURL_IMPERSONATE_BROWSER = "chrome110"

NEXT_PAGE_SELECTORS = [ # Order can matter; more specific or reliable ones first
    "a.qa-PaginationPageLink-Next", # Comparably specific observed
    "a.pagination-link[rel='next']", "a[aria-label*='Next Page' i]", "a[title*='Next Page' i]",
    "li.pagination-next > a", "a.pagination-next", "a.NextPageLink",
    "nav[aria-label*='pagination' i] li:last-child a[href]",
    ".page-next > a", "a.next"
]
REVIEW_BLOCK_CSS_SELECTOR = "div.cppRH"

# --- Helper: Extract Section Name (Unchanged) ---
def extract_section_name_from_url(href: Optional[str]) -> str:
    if not href: return "unknown_section"
    try:
        path_parts = urlparse(href).path.strip('/').split('/')
        if len(path_parts) >= 4 and path_parts[2] == 'reviews': return path_parts[3]
    except Exception: pass
    match = re.search(r'/reviews/(\w+)', href)
    return match.group(1) if match else "unknown_section"

# --- _parse_reviews_from_block (Uses DEFAULT_HTML_PARSER if BeautifulSoup created inside) ---
def _parse_reviews_from_block(
    review_container_soup: BeautifulSoup,
    start_date_filter: Optional[datetime],
    end_date_filter: Optional[datetime]
) -> List[Review]:
    reviews_found: List[Review] = []
    review_blocks = review_container_soup.find_all('div', class_='cppRH') # Or specific class for review blocks
    for block_idx, block in enumerate(review_blocks):
        quote = block.find('p', class_='cppRH-review-quote')
        if not quote: continue
        text = quote.get_text(strip=True).replace('\u0000', '') # Remove null characters
        cite_block = block.find('cite', class_='cppRH-review-cite')
        date_meta_tag = None
        if cite_block:
            date_meta_tag = cite_block.find('meta', {'itemprop': 'datePublished'}) or \
                         cite_block.find('meta', attrs={'content': re.compile(r'^\d{4}-\d{2}-\d{2}$')})
        if not date_meta_tag or not date_meta_tag.get('content'): continue
        try: date_val = datetime.strptime(date_meta_tag['content'], '%Y-%m-%d')
        except ValueError: continue
        if start_date_filter and date_val < start_date_filter: continue
        if end_date_filter and date_val > end_date_filter: continue
        reviews_found.append(Review(text=text, date=date_val))
    return reviews_found

# --- Function to Extract Basic Company Info (Unchanged from logic, uses DEFAULT_HTML_PARSER via soup) ---
def extract_company_info(soup: BeautifulSoup, company_base_url_str: str) -> Dict:
    details = {}
    try:
        parsed_base_url = urlparse(str(company_base_url_str)); path_parts = parsed_base_url.path.strip('/').split('/')
        company_slug_from_base = path_parts[1] if len(path_parts) > 1 else "unknown_company"
        default_name = company_slug_from_base.replace('-', ' ').title(); details['company_name'] = default_name
        details['comparably_url'] = str(company_base_url_str)
        name_tag_h1 = soup.find('h1')
        if name_tag_h1:
            h1_text = name_tag_h1.get_text(strip=True)
            name_candidate = h1_text
            if " Reviews" in h1_text: name_candidate = h1_text.split(" Reviews")[0].strip()
            if name_candidate and name_candidate.lower() not in REVIEW_CATEGORIES and len(name_candidate) > 3:
                details['company_name'] = name_candidate
        if details['company_name'] == default_name or details['company_name'].lower() in REVIEW_CATEGORIES:
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text(strip=True); name_from_title = title_text.split(" Reviews")[0].split(" | Comparably")[0].strip()
                if name_from_title and len(name_from_title) > 3 and name_from_title.lower() not in REVIEW_CATEGORIES:
                     details['company_name'] = name_from_title
    except Exception as e: print(f"Error extracting company details for {company_base_url_str}: {e}")
    return details

# --- Selenium Setup (Optimized) ---
def setup_selenium_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--blink-settings=imagesEnabled=false") # OPTIMIZATION: Disable images
    options.add_argument("--disable-extensions") # OPTIMIZATION: Disable extensions
    options.add_argument("window-size=1920,1080")
    user_agent_str = ua.random if ua else "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent_str}')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT_S)
        return driver
    except Exception as e:
        print(f"  [Selenium Setup] CRITICAL ERROR during Selenium setup: {e}")
        traceback.print_exc()
        raise RuntimeError(f"Failed to setup Selenium WebDriver: {e}")

# --- NEW Scraper: Selenium for Category Pages, Curl-CFFI for Q-Review Pages ---
def _scrape_category_deep_reviews_selenium_curl(
    company_base_url_str: str,
    category_name_arg: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    thread_name = f"SelCatCurlQ-{category_name_arg}-{company_slug[:10]}"
    category_scrape_start_time = time.perf_counter()
    print(f"  [{thread_name}] Started for category: {category_name_arg}")

    collected_questions_for_this_category: List[Question] = []
    processed_reviews_keys_globally_for_category = set()

    category_driver = None
    try:
        category_driver = setup_selenium_driver()
        category_wait = WebDriverWait(category_driver, SELENIUM_ELEMENT_TIMEOUT_S)
        category_button_wait = WebDriverWait(category_driver, max(5, SELENIUM_ELEMENT_TIMEOUT_S // 3))

        category_url_start = urljoin(company_base_url_str.rstrip('/') + "/", f"reviews/{category_name_arg}/")
        print(f"  [{thread_name}] Selenium navigating to initial Cat Page: {category_url_start}")
        category_driver.get(category_url_start)
        time.sleep(random.uniform(1.0, 1.8)) # OPTIMIZATION: Reduced sleep, relying more on WebDriverWait

        user_agent_hdr = category_driver.execute_script("return navigator.userAgent;")
        base_curl_headers = {
            'User-Agent': user_agent_hdr,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        category_page_count = 0
        while category_page_count < MAX_CATEGORY_PAGES:
            category_page_count += 1
            current_category_page_url = category_driver.current_url
            print(f"  [{thread_name}] Selenium on Cat Page {category_page_count} (URL: {current_category_page_url})")

            try:
                category_wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.reviewsList, div.cppRH"))
                )
            except TimeoutException:
                print(f"  [{thread_name}] Timeout waiting for review content on Cat Page {category_page_count}.")
                if category_page_count == 1:
                    print(f"  [{thread_name}] Initial category page for '{category_name_arg}' appears empty or inaccessible.")
                break

            current_selenium_cookies = {c['name']: c['value'] for c in category_driver.get_cookies()}
            soup_current_category_page = BeautifulSoup(category_driver.page_source, DEFAULT_HTML_PARSER)

            question_blocks_on_cat_page = soup_current_category_page.find_all('div', class_='reviewsList')
            if not question_blocks_on_cat_page and category_page_count > 1 :
                 print(f"  [{thread_name}] No 'div.reviewsList' found on Cat Page {category_page_count}, was likely end.")
                 break

            for q_block_idx, q_block_soup in enumerate(question_blocks_on_cat_page):
                q_elem = q_block_soup.find('h2', class_='section-subtitle')
                if not q_elem: continue
                question_text = q_elem.get_text(strip=True)
                print(f"    [{thread_name}] Q{q_block_idx+1}: '{question_text[:60]}...'")

                all_reviews_for_this_q: List[Review] = []
                current_q_reviews_html_segment = q_block_soup
                current_q_reviews_source_url = current_category_page_url

                with CurlCffiSession(impersonate=CURL_IMPERSONATE_BROWSER) as curl_q_session:
                    curl_q_session.cookies.update(current_selenium_cookies)

                    q_review_page_num = 0
                    while q_review_page_num < MAX_REVIEW_PAGES_PER_QUESTION:
                        q_review_page_num += 1

                        reviews_from_current_segment = _parse_reviews_from_block(
                            current_q_reviews_html_segment, start_date_filter, end_date_filter
                        )

                        newly_added_this_q_sub_page_count = 0
                        for r_parsed in reviews_from_current_segment:
                            r_key = (hash(question_text), hash(r_parsed.text), r_parsed.date)
                            if r_key not in processed_reviews_keys_globally_for_category:
                                all_reviews_for_this_q.append(r_parsed)
                                processed_reviews_keys_globally_for_category.add(r_key)
                                newly_added_this_q_sub_page_count +=1

                        if newly_added_this_q_sub_page_count > 0:
                            print(f"        [{thread_name}] Added {newly_added_this_q_sub_page_count} unique reviews for this Q (Q-Page {q_review_page_num}).")
                        elif q_review_page_num > 1 and not reviews_from_current_segment:
                            print(f"        [{thread_name}] No reviews on Q-Page {q_review_page_num} for '{question_text[:30]}...'.")

                        next_q_review_page_href = None
                        pagination_scope_for_q = current_q_reviews_html_segment.find(['nav', 'ul', 'div'],
                            class_=lambda x: x and any(p in x.lower() for p in ['pagination', 'pager', 'page-links', 'qa-Pagination', 'cp-Pagination']),
                            recursive=True
                        ) or current_q_reviews_html_segment

                        for sel in NEXT_PAGE_SELECTORS:
                            buttons = pagination_scope_for_q.select(sel)
                            for btn_tag in buttons:
                                href = btn_tag.get('href')

                                aria_label_str_q_btn = btn_tag.get("aria-label", "")
                                rel_value_q_btn = btn_tag.get("rel")
                                rel_str_q_btn = ""
                                if isinstance(rel_value_q_btn, list):
                                    rel_str_q_btn = " ".join(rel_value_q_btn)
                                elif rel_value_q_btn:
                                    rel_str_q_btn = rel_value_q_btn
                                text_content_str_q_btn = btn_tag.get_text(strip=True)
                                combined_test_str_q_btn = f"{aria_label_str_q_btn} {rel_str_q_btn} {text_content_str_q_btn}".lower()
                                current_btn_is_prev = "prev" in combined_test_str_q_btn

                                current_btn_is_disabled = any(cls in (btn_tag.get('class', [])) for cls in ['disabled', 'inactive']) or btn_tag.has_attr('disabled')

                                if current_btn_is_prev or current_btn_is_disabled: continue

                                if href and href != "#" and not href.startswith("javascript:"):
                                    next_q_review_page_href = urljoin(current_q_reviews_source_url, href)
                                    break
                            if next_q_review_page_href: break

                        if not next_q_review_page_href: break

                        try:
                            time.sleep(random.uniform(0.7, 1.5))
                            q_review_fetch_headers = base_curl_headers.copy()
                            q_review_fetch_headers['Referer'] = current_q_reviews_source_url

                            response_q_review_page = curl_q_session.get(next_q_review_page_href, headers=q_review_fetch_headers, timeout=CURL_REQUEST_TIMEOUT_S)
                            response_q_review_page.raise_for_status()

                            current_q_reviews_html_segment = BeautifulSoup(response_q_review_page.text, DEFAULT_HTML_PARSER)
                            current_q_reviews_source_url = str(response_q_review_page.url)
                            if current_q_reviews_html_segment.find('h2', class_='section-subtitle'):
                                print(f"        [{thread_name}] WARNING: Fetched Q-review page {next_q_review_page_href} looks like a full category page. Stopping Q-pagination.")
                                current_q_reviews_html_segment = BeautifulSoup("", DEFAULT_HTML_PARSER)
                                break
                        except RequestsError as e_q_rev_req:
                            status_code_msg = f" (Status: {e_q_rev_req.response.status_code})" if hasattr(e_q_rev_req, 'response') and e_q_rev_req.response else ""
                            print(f"        [{thread_name}] curl_cffi Error{status_code_msg} fetching Q-REVIEW page {next_q_review_page_href}: {e_q_rev_req}")
                            break
                        except Exception as e_gen:
                            print(f"        [{thread_name}] Generic Error Q-REVIEW page {next_q_review_page_href}: {e_gen}")
                            traceback.print_exc()
                            break

                if all_reviews_for_this_q:
                    all_reviews_for_this_q.sort(key=lambda r: r.date, reverse=True)
                    existing_q_obj = next((q for q in collected_questions_for_this_category if q.question_text == question_text), None)
                    if existing_q_obj:
                        for r_new in all_reviews_for_this_q:
                            if not any(er.text == r_new.text and er.date == r_new.date for er in existing_q_obj.review_section.reviews):
                                existing_q_obj.review_section.reviews.append(r_new)
                        existing_q_obj.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
                    else:
                        print(f"    [{thread_name}] Creating ReviewSection with section_name: {category_name_arg} for Q: '{question_text[:30]}'")
                        review_section = ReviewSection(section_name=category_name_arg, reviews=all_reviews_for_this_q)
                        question_obj = Question(question_text=question_text, review_section=review_section)
                        collected_questions_for_this_category.append(question_obj)

            next_category_page_button = None
            cat_page_nav_scope = soup_current_category_page.find('nav', attrs={'aria-label': lambda x: x and 'pagination' in x.lower()}) or \
                                 soup_current_category_page.find('ul', class_=lambda x: x and 'pagination' in x.lower()) or \
                                 soup_current_category_page

            for sel in NEXT_PAGE_SELECTORS:
                potential_btns_soup = cat_page_nav_scope.select(sel)
                for btn_s in potential_btns_soup:
                    if btn_s.find_parent('div', class_='reviewsList'): continue

                    aria_label_str_cat_btn = btn_s.get("aria-label", "")
                    rel_value_cat_btn = btn_s.get("rel")
                    rel_str_cat_btn = ""
                    if isinstance(rel_value_cat_btn, list):
                        rel_str_cat_btn = " ".join(rel_value_cat_btn)
                    elif rel_value_cat_btn:
                        rel_str_cat_btn = rel_value_cat_btn
                    text_content_str_cat_btn = btn_s.get_text(strip=True)
                    combined_test_str_cat_btn = f"{aria_label_str_cat_btn} {rel_str_cat_btn} {text_content_str_cat_btn}".lower()
                    current_cat_btn_is_prev = "prev" in combined_test_str_cat_btn
                    current_cat_btn_is_disabled = any(cls in (btn_s.get('class', [])) for cls in ['disabled', 'inactive']) or btn_s.has_attr('disabled')

                    if not current_cat_btn_is_prev and not current_cat_btn_is_disabled and btn_s.get('href') and btn_s.get('href') != '#':
                        try:
                            selenium_btns = category_driver.find_elements(By.CSS_SELECTOR, sel)
                            for sel_btn in selenium_btns:
                                if sel_btn.is_displayed() and sel_btn.get_attribute('href') == urljoin(current_category_page_url, btn_s.get('href')):
                                    try:
                                        sel_btn.find_element(By.XPATH, "./ancestor::div[contains(@class, 'reviewsList')]")
                                        continue
                                    except NoSuchElementException:
                                        pass

                                    sel_aria_label = (sel_btn.get_attribute("aria-label") or "").lower()
                                    sel_rel = (sel_btn.get_attribute("rel") or "").lower()
                                    if "prev" not in sel_aria_label and "prev" not in sel_rel :
                                        next_category_page_button = category_button_wait.until(EC.element_to_be_clickable(sel_btn))
                                        if next_category_page_button: break
                            if next_category_page_button: break
                        except (NoSuchElementException, TimeoutException): continue
                if next_category_page_button: break

            if not next_category_page_button :
                print(f"  [{thread_name}] No clickable 'Next Category Page' button found by Selenium after Cat Page {category_page_count}.")
                break

            print(f"  [{thread_name}] Selenium clicking 'Next Category Page'...")
            try:
                category_driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});", next_category_page_button)
                time.sleep(0.3)
                next_category_page_button.click()
                time.sleep(random.uniform(1.5, 2.5)) # OPTIMIZATION: Reduced sleep, WebDriverWait follows
            except ElementClickInterceptedException:
                print(f"  [{thread_name}] Click intercepted, trying JS click for 'Next Category Page'...")
                category_driver.execute_script("arguments[0].click();", next_category_page_button)
                time.sleep(random.uniform(1.5, 2.5)) # OPTIMIZATION: Reduced sleep, WebDriverWait follows
            except Exception as e_click:
                print(f"  [{thread_name}] Error clicking 'Next Category Page': {e_click}")
                break

    except Exception as e_cat_main:
        print(f"  [{thread_name}] MAJOR ERROR in category '{category_name_arg}': {e_cat_main}")
        traceback.print_exc()
    finally:
        if category_driver:
            category_driver.quit()

    category_scrape_duration = time.perf_counter() - category_scrape_start_time
    print(f"  [{thread_name}] Finished category '{category_name_arg}' in {category_scrape_duration:.2f}s. Total Qs: {len(collected_questions_for_this_category)}")
    return category_name_arg, collected_questions_for_this_category


# --- Main Orchestrator ---
def scrape_comparably_sync(
    company_base_url_str: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating SELENIUM_CAT_CURL_Q_REVIEW parallel category scrape for: {company_slug}")
    orchestration_start_time = time.perf_counter() # TIMER Start
    all_questions_for_company: List[Question] = []
    company_details_overall: Dict[str, Any] = {}
    initial_info_driver = None

    try:
        initial_info_start_time = time.perf_counter()
        print(f"  [{company_slug}] Fetching initial company info with Selenium...")
        initial_info_driver = setup_selenium_driver()
        info_fetch_url = urljoin(company_base_url_str.rstrip('/') + "/", "reviews/")
        initial_info_driver.get(info_fetch_url)
        time.sleep(random.uniform(1.2, 2.0)) # OPTIMIZATION: Slightly reduced sleep
        if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title or "Access Denied" in initial_info_driver.page_source:
            print(f"  [{company_slug}] /reviews/ page for info failed (Title: {initial_info_driver.title}), trying base URL: {company_base_url_str}")
            initial_info_driver.get(company_base_url_str)
            time.sleep(random.uniform(1.2, 2.0)) # OPTIMIZATION: Slightly reduced sleep
            if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title or "Access Denied" in initial_info_driver.page_source:
                raise Exception(f"Could not load a valid page for company info (Title: {initial_info_driver.title})")
        info_html = initial_info_driver.page_source
        info_soup = BeautifulSoup(info_html, DEFAULT_HTML_PARSER)
        company_details_overall = extract_company_info(info_soup, company_base_url_str)
        initial_info_duration = time.perf_counter() - initial_info_start_time
        print(f"  [{company_slug}] Initial company info fetched in {initial_info_duration:.2f}s. Name='{company_details_overall.get('company_name')}'")
    except Exception as e_info:
        initial_info_duration = time.perf_counter() - initial_info_start_time
        print(f"  [{company_slug}] Error fetching initial company info (took {initial_info_duration:.2f}s): {e_info}")
        company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": f"Initial info fetch error: {str(e_info)}"}
    finally:
        if initial_info_driver:
            try: initial_info_driver.quit()
            except Exception as e_close: print(f"  [{company_slug}] Error closing Selenium info browser: {e_close}")

    max_concurrent_categories = min(len(REVIEW_CATEGORIES), 4) # OPTIMIZATION: Increased from 3. Tune based on CPU/memory.
    print(f"  [{company_slug}] Starting SELENIUM_CAT_CURL_Q_REVIEW parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")
    
    category_processing_start_time = time.perf_counter()
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories, thread_name_prefix="SelCatCurlQPool") as executor:
        for cat_name_from_list in REVIEW_CATEGORIES:
            future = executor.submit(
                _scrape_category_deep_reviews_selenium_curl,
                company_base_url_str,
                cat_name_from_list,
                company_slug,
                start_date_filter,
                end_date_filter
            )
            futures_map[future] = cat_name_from_list

        for future in as_completed(futures_map):
            original_category_name_processed = futures_map[future]
            try:
                processed_cat_name, questions_from_category = future.result()
                if processed_cat_name != original_category_name_processed:
                     print(f"  [{company_slug}] WARNING: Mismatch in returned category name. Expected '{original_category_name_processed}', got '{processed_cat_name}'.")

                if questions_from_category:
                    print(f"  [{company_slug}] Received {len(questions_from_category)} Qs from cat '{original_category_name_processed}'. Merging...")
                    all_questions_for_company.extend(questions_from_category)
                else:
                    print(f"  [{company_slug}] Cat '{original_category_name_processed}' returned no Qs.")
            except Exception as e_future_exc:
                print(f"  [{company_slug}] SELENIUM_CAT_CURL_Q_REVIEW Category task for '{original_category_name_processed}' FAILED in executor: {e_future_exc}")
                traceback.print_exc()
    
    category_processing_duration = time.perf_counter() - category_processing_start_time
    print(f"  [{company_slug}] All category threads completed processing in {category_processing_duration:.2f}s.")

    total_duration = time.perf_counter() - orchestration_start_time # TIMER End
    print(f"\nFinished ALL SELENIUM_CAT_CURL_Q_REVIEW scrapes for {company_slug} in {total_duration:.2f}s. Total Qs collected: {len(all_questions_for_company)}")

    if not company_details_overall.get("company_name") or company_details_overall.get("company_name", "").lower() == company_slug.lower() or company_details_overall.get("company_name", "") == "unknown_company":
        current_name = company_details_overall.get("company_name", "unknown_company")
        fallback_name = company_slug.replace('-', ' ').title()
        if current_name.lower() in REVIEW_CATEGORIES or current_name == "unknown_company":
             company_details_overall["company_name"] = fallback_name
             if "status_note" not in company_details_overall: company_details_overall["status_note"] = "Name set to fallback slug-based name."

    return {
        "status": "success" if all_questions_for_company or (company_details_overall.get("company_name") != company_slug.replace('-', ' ').title() and company_details_overall.get("company_name") != "unknown_company") else "partial_success_no_reviews",
        "data": {
            "company_info": company_details_overall,
            "reviews": [q.model_dump(mode='json') for q in all_questions_for_company]
        }
    }

# --- FastAPI Endpoint (Unchanged, but note the version in app.title) ---
@app.post("/scrape")
async def scrape_companies(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    urls = request.urls; start_date_filter: Optional[datetime] = None; end_date_filter: Optional[datetime] = None
    if request.start_date_str:
        try: start_date_filter = datetime.strptime(request.start_date_str, "%Y-%m-%d")
        except ValueError: raise HTTPException(status_code=400, detail="Invalid start_date_str. Use YYYY-MM-DD.")
    if request.end_date_str:
        try: end_date_filter = datetime.strptime(request.end_date_str, "%Y-%m-%d"); end_date_filter = end_date_filter.replace(hour=23, minute=59, second=59)
        except ValueError: raise HTTPException(status_code=400, detail="Invalid end_date_str. Use YYYY-MM-DD.")
    if start_date_filter and end_date_filter and start_date_filter > end_date_filter: raise HTTPException(status_code=400, detail="start_date_str cannot be after end_date_str.")
    if not urls: raise HTTPException(status_code=400, detail="No URLs provided.")
    results: Dict[str, Dict[str, Any]] = {}; valid_scrape_params = []
    print(f"API request: {len(urls)} URLs, Selenium CatNav & Curl Q-ReviewNav (v{app.version}).") # Will print v2.1.0
    for url_obj in urls:
        url_str = str(url_obj)
        try:
            parsed_url = urlparse(url_str); path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
            if not (parsed_url.scheme and parsed_url.netloc and len(path_segments) >= 2 and path_segments[0] == "companies"):
                raise ValueError("URL format error or incomplete URL")
            company_slug = path_segments[1]; company_base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/companies/{company_slug}"
            valid_scrape_params.append({'original_url': url_str, 'base_url': company_base_url, 'slug': company_slug})
        except Exception as e_slug:
            print(f"Error parsing slug/URL '{url_str}': {e_slug}")
            results[url_str] = {"status": "error", "message": f"Invalid Comparably company URL format: {url_str}. Error: {e_slug}"}
    tasks = []
    if valid_scrape_params:
        for params in valid_scrape_params:
            tasks.append(asyncio.to_thread(scrape_comparably_sync, params['base_url'], params['slug'], start_date_filter, end_date_filter))
    scraped_results_or_exceptions = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
    task_idx = 0
    for params in valid_scrape_params:
        original_url_str = params['original_url']
        if original_url_str in results: continue
        if task_idx < len(scraped_results_or_exceptions):
            result_or_exc = scraped_results_or_exceptions[task_idx]
            if isinstance(result_or_exc, Exception):
                print(f"Task for {original_url_str} EXCEPTION (type: {type(result_or_exc).__name__}): {result_or_exc}")
                tb_str = "".join(traceback.format_exception(None, result_or_exc, result_or_exc.__traceback__))
                print(f"FULL TRACEBACK for {original_url_str} (SelCatCurlQ):\n{tb_str}")
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed. Type: {type(result_or_exc).__name__}. Check logs."}
            elif isinstance(result_or_exc, dict): results[original_url_str] = result_or_exc
            else: results[original_url_str] = {"status": "error", "message": "Unexpected internal result type from scraping task"}
            task_idx +=1
        else: results[original_url_str] = {"status": "error", "message": "Scraping task result missing (logic error)."}
    print(f"Finished API request processing (SelCatCurlQ v{app.version}).")
    return results
