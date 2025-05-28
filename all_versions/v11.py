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

# --- Pydantic Models --- (Unchanged)
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
    title="Comparably Scraper API - Hybrid Optimized (Fine-tuned)",
    description="Optimized Hybrid: Selenium (tuned clicks/popups), Curl-CFFI Q-Rev.",
    version="2.3.1" # Version bump for fine-tuning
)

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, ElementClickInterceptedException,
    StaleElementReferenceException, ElementNotInteractableException
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from curl_cffi.requests import Session as CurlCffiSession, RequestsError

try: from fake_useragent import UserAgent; ua = UserAgent()
except ImportError: print("Warning: fake-useragent not installed."); ua = None

try: import lxml; DEFAULT_HTML_PARSER = "lxml"
except ImportError: print("Warning: lxml not installed, using html.parser."); DEFAULT_HTML_PARSER = "html.parser"

# --- Constants ---
REVIEW_CATEGORIES = ["leadership", "compensation", "team", "environment", "outlook"]
MAX_CATEGORY_PAGES = 15
MAX_REVIEW_PAGES_PER_QUESTION = 10
SELENIUM_PAGE_TIMEOUT_S = 20 # Slightly more aggressive
SELENIUM_ELEMENT_TIMEOUT_S = 12 # Slightly more aggressive
SELENIUM_INTERACTION_TIMEOUT_S = 7 # For Selenium clicks
CURL_REQUEST_TIMEOUT_S = 15
CURL_IMPERSONATE_BROWSER = "chrome110" # Consider updating to "chrome120" if available

NEXT_PAGE_SELECTORS = [
    "a.qa-PaginationPageLink-Next", "a.pagination-link[rel='next']",
    "a[aria-label*='Next Page' i]", "a[title*='Next Page' i]",
    "li.pagination-next > a", "a.pagination-next", "a.NextPageLink",
    "nav[aria-label*='pagination' i] li:last-child a[href]",
    ".page-next > a", "a.next"
]
REVIEW_BLOCK_CSS_SELECTOR_BS = "div.cppRH"
QUESTION_BLOCK_SELECTOR_BS = "div.reviewsList"

POPUP_CLOSE_SELECTORS = [ # More targeted, assuming some common patterns
    "button[class*='modal__close' i]", "button[aria-label*='Dismiss' i]",
    "button[aria-label*='close' i]", "div[role='dialog'] button[class*='close']",
    "svg[data-testid*='close' i]", "i[class*='icon-close' i]",
    # Less specific, try last if others fail
    "button[class*='close' i]", "span[class*='close' i]"
]
INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(1.0, 1.5) # Reduced
AFTER_CATEGORY_CLICK_SLEEP_S = random.uniform(1.2, 1.8) # Reduced
CURL_FETCH_DELAY_S = random.uniform(0.4, 0.8) # Reduced

# --- Helper: Extract Section Name (Unchanged) ---
def extract_section_name_from_url(href: Optional[str]) -> str:
    if not href: return "unknown_section"
    try:
        path_parts = urlparse(href).path.strip('/').split('/')
        if len(path_parts) >= 4 and path_parts[2] == 'reviews': return path_parts[3]
    except Exception: pass
    match = re.search(r'/reviews/(\w+)', href)
    return match.group(1) if match else "unknown_section"

# --- _parse_reviews_from_block (Unchanged from 2.3.0) ---
def _parse_reviews_from_block(
    review_container_soup: BeautifulSoup,
    start_date_filter: Optional[datetime],
    end_date_filter: Optional[datetime]
) -> List[Review]:
    reviews_found: List[Review] = []
    review_blocks = review_container_soup.find_all('div', class_=REVIEW_BLOCK_CSS_SELECTOR_BS.split('.')[-1])
    for block_idx, block in enumerate(review_blocks):
        quote = block.find('p', class_='cppRH-review-quote')
        if not quote: continue
        text = quote.get_text(strip=True).replace('\u0000', '')
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

# --- Selenium Setup (Unchanged from 2.3.0) ---
def setup_selenium_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox"); options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu"); options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-extensions"); options.add_argument("window-size=1920,1080")
    user_agent_str = ua.random if ua else "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent_str}')
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--log-level=3')
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT_S)
        return driver
    except Exception as e: print(f"  [Selenium Setup] CRITICAL ERROR: {e}"); traceback.print_exc(); raise RuntimeError(f"Failed: {e}")

# --- try_click (Unchanged from 2.3.0) ---
def try_click(driver: webdriver.Chrome, element, timeout: int = SELENIUM_INTERACTION_TIMEOUT_S):
    try:
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element))
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center', inline: 'nearest'});", element)
        time.sleep(0.2 + random.uniform(0.1, 0.2)) # Slightly reduced scroll pause
        element.click()
        return True
    except ElementClickInterceptedException:
        print(f"    [try_click] Intercepted. Trying JS click.")
        try: driver.execute_script("arguments[0].click();", element); return True
        except Exception as e_js: print(f"    [try_click] JS click failed: {e_js}"); return False
    except (TimeoutException, ElementNotInteractableException, StaleElementReferenceException) as e_click:
        print(f"    [try_click] Click failed: {type(e_click).__name__} - {str(e_click).splitlines()[0] if str(e_click) else ''}")
        return False
    except Exception as e_other_click: print(f"    [try_click] Unexpected click error: {type(e_other_click).__name__} - {e_other_click}"); return False

# --- attempt_to_close_popups (Tuned pause) ---
def attempt_to_close_popups(driver: webdriver.Chrome, thread_name: str):
    closed_any = False
    main_window = driver.current_window_handle
    initial_handles = set(driver.window_handles)

    for sel_idx, sel in enumerate(POPUP_CLOSE_SELECTORS):
        try:
            popups = driver.find_elements(By.CSS_SELECTOR, sel)
            if not popups: continue
            for popup_btn in popups:
                if popup_btn.is_displayed() and popup_btn.is_enabled():
                    print(f"      [{thread_name}] Attempting to close popup ({sel_idx+1}/{len(POPUP_CLOSE_SELECTORS)}) with selector: {sel[:30]}...")
                    try:
                        driver.execute_script("arguments[0].click();", popup_btn)
                        time.sleep(0.3 + random.uniform(0.1, 0.2)) # Reduced
                        closed_any = True
                        if not popup_btn.is_displayed(): print(f"      [{thread_name}] Popup seems closed."); break
                    except StaleElementReferenceException: print(f"      [{thread_name}] Popup closed (stale)."); closed_any = True; break
                    except Exception as e_close: print(f"      [{thread_name}] Error clicking popup close ({sel[:30]}...): {e_close}")
            if closed_any and (not popups[0].is_displayed()): break
        except Exception: pass
    
    # Close any new unexpected tabs/windows
    final_handles = set(driver.window_handles)
    new_handles = final_handles - initial_handles
    if new_handles:
        for handle in new_handles:
            if handle != main_window:
                try:
                    print(f"      [{thread_name}] Closing unexpected new window/tab.")
                    driver.switch_to.window(handle)
                    driver.close()
                    closed_any = True # Count this as a popup closure action
                except Exception as e_win_close:
                    print(f"      [{thread_name}] Error closing new window: {e_win_close}")
        driver.switch_to.window(main_window) # Ensure focus is back

    if closed_any:
        print(f"      [{thread_name}] Popup closure attempted. Pausing briefly...")
        time.sleep(0.5 + random.uniform(0.1, 0.3)) # Reduced pause

# --- HYBRID Scraper (Tuned sleeps) ---
def _scrape_category_deep_reviews_hybrid(
    company_base_url_str: str, category_name_arg: str, company_slug: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    thread_name = f"Hybrid-{category_name_arg}-{company_slug[:10]}"
    category_scrape_start_time = time.perf_counter()
    print(f"  [{thread_name}] Started for category: {category_name_arg}")
    collected_questions_for_this_category: List[Question] = []
    processed_reviews_keys_globally_for_category = set()
    category_driver = None

    try:
        category_driver = setup_selenium_driver()
        category_wait = WebDriverWait(category_driver, SELENIUM_ELEMENT_TIMEOUT_S)

        category_url_start = urljoin(company_base_url_str.rstrip('/') + "/", f"reviews/{category_name_arg}/")
        print(f"  [{thread_name}] Selenium navigating to initial Cat Page: {category_url_start}")
        category_driver.get(category_url_start)
        time.sleep(INITIAL_PAGE_LOAD_SLEEP_S) # Tuned
        attempt_to_close_popups(category_driver, thread_name)

        user_agent_hdr = category_driver.execute_script("return navigator.userAgent;")
        base_curl_headers = { 'User-Agent': user_agent_hdr, 'Accept': 'text/html...', 'Accept-Language': 'en-US,en;q=0.9' } # Abbreviated

        category_page_count = 0
        while category_page_count < MAX_CATEGORY_PAGES:
            category_page_count += 1
            current_category_page_url = category_driver.current_url
            print(f"  [{thread_name}] Selenium on Cat Page {category_page_count} (URL: {current_category_page_url[:100]}...)")

            try:
                category_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"{QUESTION_BLOCK_SELECTOR_BS}, {REVIEW_BLOCK_CSS_SELECTOR_BS}")))
            except TimeoutException:
                print(f"  [{thread_name}] Timeout waiting for content on Cat Page {category_page_count}.")
                if category_page_count == 1: print(f"  [{thread_name}] Initial page for '{category_name_arg}' seems empty.")
                break

            current_selenium_cookies = {c['name']: c['value'] for c in category_driver.get_cookies()}
            soup_current_category_page = BeautifulSoup(category_driver.page_source, DEFAULT_HTML_PARSER)
            question_blocks_on_cat_page = soup_current_category_page.find_all('div', class_=QUESTION_BLOCK_SELECTOR_BS.split('.')[-1])

            if not question_blocks_on_cat_page:
                # ... (same logic as 2.3.0 for handling no question blocks) ...
                 if category_page_count == 1 and not soup_current_category_page.find('div', class_=REVIEW_BLOCK_CSS_SELECTOR_BS.split('.')[-1]):
                      print(f"  [{thread_name}] No question/review blocks on initial Cat Page {category_page_count}.")
                 elif category_page_count > 1:
                      print(f"  [{thread_name}] No question blocks on Cat Page {category_page_count}, likely end.")
                 if category_page_count > 1 : break 

            print(f"    [{thread_name}] Found {len(question_blocks_on_cat_page)} question blocks on Cat Page {category_page_count}.")
            for q_block_idx, q_block_soup in enumerate(question_blocks_on_cat_page):
                q_elem = q_block_soup.find('h2', class_='section-subtitle')
                if not q_elem: continue
                question_text = q_elem.get_text(strip=True)
                print(f"    [{thread_name}] Q{q_block_idx+1}: '{question_text[:60]}...' (Curl-CFFI for Q-Reviews)")
                all_reviews_for_this_q_session: List[Review] = []
                current_q_reviews_html_segment_bs = q_block_soup
                current_q_reviews_source_url_for_curl = current_category_page_url

                with CurlCffiSession(impersonate=CURL_IMPERSONATE_BROWSER, trust_env=False) as curl_q_session: # Added trust_env=False for consistency
                    curl_q_session.cookies.update(current_selenium_cookies)
                    q_review_page_num_curl = 0
                    while q_review_page_num_curl < MAX_REVIEW_PAGES_PER_QUESTION:
                        q_review_page_num_curl += 1
                        reviews_from_current_segment = _parse_reviews_from_block(
                            current_q_reviews_html_segment_bs, start_date_filter, end_date_filter
                        )
                        newly_added_this_q_sub_page_count = 0
                        for r_parsed in reviews_from_current_segment:
                            r_key = (hash(question_text), hash(r_parsed.text), r_parsed.date)
                            if r_key not in processed_reviews_keys_globally_for_category:
                                all_reviews_for_this_q_session.append(r_parsed)
                                processed_reviews_keys_globally_for_category.add(r_key)
                                newly_added_this_q_sub_page_count +=1
                        
                        if newly_added_this_q_sub_page_count > 0:
                            print(f"        [{thread_name}] Added {newly_added_this_q_sub_page_count} unique Q-reviews (Q-Page {q_review_page_num_curl} via Curl).")
                        elif q_review_page_num_curl > 1 and not reviews_from_current_segment:
                            print(f"        [{thread_name}] No reviews on Q-Page {q_review_page_num_curl} for '{question_text[:30]}...'.")

                        next_q_review_page_href = None
                        # ... (Curl Q-review next page finding logic - same as 2.3.0) ...
                        pagination_scope_for_q_bs = current_q_reviews_html_segment_bs.find(['nav', 'ul', 'div'],
                            class_=lambda x: x and any(p in x.lower() for p in ['pagination', 'pager', 'page-links', 'qa-Pagination', 'cp-Pagination']),
                            recursive=True 
                        ) or current_q_reviews_html_segment_bs 

                        for sel in NEXT_PAGE_SELECTORS:
                            buttons_bs = pagination_scope_for_q_bs.select(sel)
                            for btn_tag_bs in buttons_bs:
                                href = btn_tag_bs.get('href')
                                aria_label_bs = btn_tag_bs.get("aria-label", "").lower()
                                rel_val_bs = btn_tag_bs.get("rel"); rel_str_bs = " ".join(rel_val_bs).lower() if isinstance(rel_val_bs, list) else (rel_val_bs or "").lower()
                                text_bs = btn_tag_bs.get_text(strip=True).lower()
                                combined_bs = f"{aria_label_bs} {rel_str_bs} {text_bs}"
                                is_prev_bs = "prev" in combined_bs
                                class_bs = btn_tag_bs.get('class', [])
                                is_disabled_bs = any(c in class_bs for c in ['disabled', 'inactive']) or btn_tag_bs.has_attr('disabled')
                                if is_prev_bs or is_disabled_bs: continue
                                if href and href != "#" and not href.startswith("javascript:"):
                                    next_q_review_page_href = urljoin(current_q_reviews_source_url_for_curl, href); break
                            if next_q_review_page_href: break
                        if not next_q_review_page_href: break

                        try:
                            # print(f"        [{thread_name}] Curl-CFFI fetching Q-REVIEW page: {next_q_review_page_href[:100]}...") # Log less
                            time.sleep(CURL_FETCH_DELAY_S) # Tuned
                            q_review_fetch_headers = base_curl_headers.copy()
                            q_review_fetch_headers['Referer'] = current_q_reviews_source_url_for_curl
                            response_q_review_page = curl_q_session.get(next_q_review_page_href, headers=q_review_fetch_headers, timeout=CURL_REQUEST_TIMEOUT_S)
                            response_q_review_page.raise_for_status()
                            current_q_reviews_html_segment_bs = BeautifulSoup(response_q_review_page.text, DEFAULT_HTML_PARSER)
                            current_q_reviews_source_url_for_curl = str(response_q_review_page.url)
                            if current_q_reviews_html_segment_bs.find('h2', class_='section-subtitle') and \
                               len(current_q_reviews_html_segment_bs.find_all('div', class_=QUESTION_BLOCK_SELECTOR_BS.split('.')[-1])) > 1:
                                print(f"        [{thread_name}] WARNING: Curl Q-review page looks like full category. Stopping."); break 
                        except RequestsError as e_q_rev_req:
                            status_code_msg = f" (Status: {e_q_rev_req.response.status_code})" if hasattr(e_q_rev_req, 'response') and e_q_rev_req.response else ""
                            print(f"        [{thread_name}] Curl-CFFI Error{status_code_msg} fetching Q-REVIEW page: {e_q_rev_req}")
                            break 
                        except Exception as e_gen_curl: print(f"        [{thread_name}] Generic Error Curl Q-REVIEW: {e_gen_curl}"); break
                
                if all_reviews_for_this_q_session:
                    # ... (merging logic same as 2.3.0) ...
                    all_reviews_for_this_q_session.sort(key=lambda r: r.date, reverse=True)
                    existing_q_obj = next((q for q in collected_questions_for_this_category if q.question_text == question_text), None)
                    if existing_q_obj: 
                        new_merged_count = 0
                        for r_new in all_reviews_for_this_q_session:
                            if not any(er.text == r_new.text and er.date == r_new.date for er in existing_q_obj.review_section.reviews):
                                existing_q_obj.review_section.reviews.append(r_new); new_merged_count+=1
                        if new_merged_count > 0: existing_q_obj.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
                        # print(f"    [{thread_name}] Merged {new_merged_count} new reviews into existing Q. Total now: {len(existing_q_obj.review_section.reviews)}")
                    else:
                        review_section = ReviewSection(section_name=category_name_arg, reviews=all_reviews_for_this_q_session)
                        question_obj = Question(question_text=question_text, review_section=review_section)
                        collected_questions_for_this_category.append(question_obj)
                        print(f"    [{thread_name}] Created new Q with {len(all_reviews_for_this_q_session)} reviews: '{question_text[:30]}...'")

            # --- Selenium navigates to NEXT CATEGORY PAGE ---
            # Only call popup closer if a click fails, or just once before find attempts
            # attempt_to_close_popups(category_driver, thread_name) # MOVED: Call only if click fails or once before loop
            
            next_category_page_button_sel_elem = None
            # ... (Finding next_category_page_button_sel_elem - same as 2.3.0) ...
            soup_for_cat_nav = BeautifulSoup(category_driver.page_source, DEFAULT_HTML_PARSER) 
            cat_page_nav_scope_bs = soup_for_cat_nav.find('nav', attrs={'aria-label': lambda x: x and 'pagination' in x.lower()}) or \
                                 soup_for_cat_nav.find('ul', class_=lambda x: x and 'pagination' in x.lower()) or \
                                 soup_for_cat_nav
            for sel_css in NEXT_PAGE_SELECTORS:
                potential_bs_btns = cat_page_nav_scope_bs.select(sel_css)
                for btn_s_tag in potential_bs_btns:
                    if btn_s_tag.find_parent('div', class_=QUESTION_BLOCK_SELECTOR_BS.split('.')[-1]): continue
                    aria_label_bs = btn_s_tag.get("aria-label", "").lower()
                    rel_val_bs = btn_s_tag.get("rel"); rel_bs = " ".join(rel_val_bs).lower() if isinstance(rel_val_bs, list) else (rel_val_bs or "").lower()
                    text_bs = btn_s_tag.get_text(strip=True).lower()
                    combined_bs = f"{aria_label_bs} {rel_bs} {text_bs}"; is_prev_bs = "prev" in combined_bs
                    class_bs = btn_s_tag.get('class', []); is_disabled_bs = any(c in class_bs for c in ['disabled', 'inactive']) or btn_s_tag.has_attr('disabled')
                    href_bs = btn_s_tag.get('href')
                    if not is_prev_bs and not is_disabled_bs and href_bs and href_bs != '#':
                        try:
                            selenium_potential_cat_next_btns = category_driver.find_elements(By.CSS_SELECTOR, sel_css)
                            for sel_btn_elem in selenium_potential_cat_next_btns:
                                if not sel_btn_elem.is_displayed() or not sel_btn_elem.is_enabled(): continue
                                try: sel_btn_elem.find_element(By.XPATH, f"./ancestor::div[contains(@class, '{QUESTION_BLOCK_SELECTOR_BS.split('.')[-1]}')]"); continue 
                                except NoSuchElementException: pass
                                sel_href = sel_btn_elem.get_attribute('href')
                                if sel_href and urljoin(current_category_page_url, href_bs) == sel_href:
                                    next_category_page_button_sel_elem = sel_btn_elem; break
                            if next_category_page_button_sel_elem: break
                        except (NoSuchElementException, StaleElementReferenceException): continue
                if next_category_page_button_sel_elem: break
            
            if not next_category_page_button_sel_elem:
                print(f"  [{thread_name}] No 'Next Category Page' button found after Cat Page {category_page_count}.")
                break

            print(f"  [{thread_name}] Selenium clicking 'Next Category Page'...")
            clicked_successfully = try_click(category_driver, next_category_page_button_sel_elem)
            if not clicked_successfully:
                print(f"    [{thread_name}] Initial click failed for Next Cat Page. Trying to close popups then retry.")
                attempt_to_close_popups(category_driver, thread_name) # Try popups if first click failed
                # Re-find the button after potential popup closure
                # This is simplified; a more robust re-find would be better if popups significantly alter DOM
                try:
                    next_category_page_button_sel_elem = WebDriverWait(category_driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, sel_css)) # Assuming sel_css is still the correct one
                        # Or better: find by a more unique attribute of the button if available
                    ) 
                    clicked_successfully = try_click(category_driver, next_category_page_button_sel_elem)
                except TimeoutException:
                    print(f"    [{thread_name}] Could not re-find Next Cat Page button after popup attempt.")

            if clicked_successfully:
                time.sleep(AFTER_CATEGORY_CLICK_SLEEP_S) # Tuned
                # attempt_to_close_popups(category_driver, thread_name) # Maybe not needed if initial + on-fail is enough
            else:
                print(f"  [{thread_name}] Failed to click 'Next Category Page' even after popup attempt. Stopping.")
                break
    # ... (rest of function, main orchestrator, FastAPI endpoint unchanged from 2.3.0) ...
    except Exception as e_cat_main:
        print(f"  [{thread_name}] MAJOR ERROR in category '{category_name_arg}': {e_cat_main}")
        traceback.print_exc()
    finally:
        if category_driver: category_driver.quit()
    category_scrape_duration = time.perf_counter() - category_scrape_start_time
    print(f"  [{thread_name}] Finished category '{category_name_arg}' in {category_scrape_duration:.2f}s. Total Qs: {len(collected_questions_for_this_category)}")
    return category_name_arg, collected_questions_for_this_category

# --- Main Orchestrator & FastAPI Endpoint (structure unchanged from v2.3.0) ---
def scrape_comparably_sync(
    company_base_url_str: str, company_slug: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating HYBRID (Fine-tuned) scrape for: {company_slug} (v{app.version})")
    orchestration_start_time = time.perf_counter()
    all_questions_for_company: List[Question] = []
    company_name_from_slug = company_slug.replace('-', ' ').title()
    company_details_overall: Dict[str, Any] = {
        "company_name": company_name_from_slug, 
        "comparably_url": company_base_url_str,
        "status_note": "Company name derived from slug."
    }
    print(f"  [{company_slug}] Using derived company name: '{company_name_from_slug}'")
    max_concurrent_categories = min(len(REVIEW_CATEGORIES), 4) 
    print(f"  [{company_slug}] Starting HYBRID parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")
    category_processing_start_time = time.perf_counter()
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories, thread_name_prefix="HybridTunePool") as executor:
        for cat_name_from_list in REVIEW_CATEGORIES:
            future = executor.submit(
                _scrape_category_deep_reviews_hybrid,
                company_base_url_str, cat_name_from_list, company_slug,
                start_date_filter, end_date_filter
            )
            futures_map[future] = cat_name_from_list
        for future in as_completed(futures_map):
            original_category_name_processed = futures_map[future]
            try:
                processed_cat_name, questions_from_category = future.result()
                if questions_from_category:
                    print(f"  [{company_slug}] Received {len(questions_from_category)} Qs from cat '{original_category_name_processed}'.")
                    all_questions_for_company.extend(questions_from_category)
                else:
                    print(f"  [{company_slug}] Cat '{original_category_name_processed}' returned no Qs.")
            except Exception as e_future_exc:
                print(f"  [{company_slug}] HYBRID Tuned Cat task for '{original_category_name_processed}' FAILED: {e_future_exc}")
                traceback.print_exc()
    category_processing_duration = time.perf_counter() - category_processing_start_time
    print(f"  [{company_slug}] All category threads (Hybrid Tuned) completed in {category_processing_duration:.2f}s.")
    total_duration = time.perf_counter() - orchestration_start_time
    print(f"\nFinished ALL HYBRID Tuned scrapes for {company_slug} in {total_duration:.2f}s. Total Qs: {len(all_questions_for_company)}")
    if company_details_overall.get("company_name", "").lower() in REVIEW_CATEGORIES:
        company_details_overall["company_name"] = company_name_from_slug
        company_details_overall["status_note"] = "Company name reset to slug-derived."
    return {
        "status": "success" if all_questions_for_company else "partial_success_no_reviews",
        "data": { "company_info": company_details_overall, "reviews": [q.model_dump(mode='json') for q in all_questions_for_company] }
    }

@app.post("/scrape")
async def scrape_companies(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    urls = request.urls; start_date_filter: Optional[datetime] = None; end_date_filter: Optional[datetime] = None
    if request.start_date_str:
        try: start_date_filter = datetime.strptime(request.start_date_str, "%Y-%m-%d")
        except ValueError: raise HTTPException(status_code=400, detail="Invalid start_date_str.")
    if request.end_date_str:
        try: end_date_filter = datetime.strptime(request.end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        except ValueError: raise HTTPException(status_code=400, detail="Invalid end_date_str.")
    if start_date_filter and end_date_filter and start_date_filter > end_date_filter: raise HTTPException(status_code=400, detail="Start date after end date.")
    if not urls: raise HTTPException(status_code=400, detail="No URLs provided.")

    results: Dict[str, Dict[str, Any]] = {}; valid_scrape_params = []
    print(f"API request: {len(urls)} URLs, Hybrid Optimized Tuned (v{app.version}).") 
    for url_obj in urls:
        url_str = str(url_obj)
        try:
            parsed_url = urlparse(url_str); path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
            if not (parsed_url.scheme and parsed_url.netloc and len(path_segments) >= 2 and path_segments[0] == "companies"):
                raise ValueError("URL format error")
            company_slug = path_segments[1]; company_base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/companies/{company_slug}"
            valid_scrape_params.append({'original_url': url_str, 'base_url': company_base_url, 'slug': company_slug})
        except Exception as e_slug:
            print(f"Error parsing URL '{url_str}': {e_slug}")
            results[url_str] = {"status": "error", "message": f"Invalid URL format: {url_str}. Error: {e_slug}"}
    
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
                print(f"Task for {original_url_str} (Hybrid Tuned v{app.version}) EXCEPTION: {result_or_exc}") 
                tb_str = "".join(traceback.format_exception(None, result_or_exc, result_or_exc.__traceback__))
                print(f"FULL TRACEBACK for {original_url_str} (Hybrid Tuned v{app.version}):\n{tb_str}") 
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed: {type(result_or_exc).__name__}. Check logs."}
            elif isinstance(result_or_exc, dict): results[original_url_str] = result_or_exc
            else: results[original_url_str] = {"status": "error", "message": "Unexpected internal result"}
            task_idx +=1
        else: results[original_url_str] = {"status": "error", "message": "Scraping task result missing."}
        
    print(f"Finished API request processing (Hybrid Optimized Tuned v{app.version}).") 
    return results
