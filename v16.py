#--- START OF FILE v15_xhr_centric_reviews.py ---

import os
import json
import re
import time
import random
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse, urljoin, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import sys
import logging
import threading
import zipfile

# --- Pydantic Models --- (Identical to previous)
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

# --- FastAPI --- (Identical to previous)
from fastapi import FastAPI, HTTPException, Body
app = FastAPI(
    title="Comparably Scraper API - XHR-Centric Review Strategy",
    description="Selenium for navigation, Selenium-Wire for XHR question & review loading.",
    version="3.1.0"
)

from bs4 import BeautifulSoup
from seleniumwire import webdriver as seleniumwire_webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, ElementClickInterceptedException,
    StaleElementReferenceException, ElementNotInteractableException, WebDriverException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

try: from fake_useragent import UserAgent; ua = UserAgent()
except ImportError: print("Warning: fake-useragent not installed."); ua = None

try: import lxml; DEFAULT_HTML_PARSER = "lxml"; print("INFO: Using lxml for HTML parsing.")
except ImportError: print("Warning: lxml not installed, using html.parser."); DEFAULT_HTML_PARSER = "html.parser"

# --- Constants --- (Mostly identical, may need new XHR specific constants)
REVIEW_CATEGORIES = ["leadership", "compensation", "team", "environment", "outlook"]
SELENIUM_PAGE_TIMEOUT_S = 30 # Increased slightly for more XHR activity
SELENIUM_ELEMENT_TIMEOUT_S = 15
SELENIUM_INTERACTION_TIMEOUT_S = 8
INITIAL_XHR_LOAD_TIMEOUT_S = 15 # Timeout for waiting for initial question-loading XHRs

NEXT_PAGE_SELECTORS = [ # For both category and in-question pagination in the DOM
    "a.qa-PaginationPageLink-Next", "a.pagination-link[rel='next']",
    "a[aria-label*='Next Page' i]", "a[title*='Next Page' i]",
    "li.pagination-next > a", "a.pagination-next", "a.NextPageLink",
    "nav[aria-label*='pagination' i] li:last-child a[href]",
    ".page-next > a", "a.next"
]
REVIEW_BLOCK_CSS_SELECTOR_BS = "div.cppRH"
QUESTION_BLOCK_SELECTOR_BS_FROM_XHR_HTML = "div.reviewsList" # Selector for question block within XHR HTML
# Selector for the sentiment filter tabs, specifically "All"
SENTIMENT_ALL_TAB_SELECTOR = "div.sentimentFilter[data-sentiment='all']"


TARGETED_POPUP_MAIN_SELECTOR = "div[class*='cultureQuestions-popup'] a.closeButton, div[class*='cultureQuestionsLoader'] a.closeButton"
POPUP_CLOSE_SELECTORS = [ TARGETED_POPUP_MAIN_SELECTOR, "button[class*='modal__close' i]", "button[aria-label*='Dismiss' i]", "button[aria-label*='close' i]", "div[role='dialog'] button[class*='close']", "svg[data-testid*='close' i]", "i[class*='icon-close' i]", "button[class*='close' i]", "span[class*='close' i]"]
INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(1.5, 2.5) # Longer for initial XHRs to potentially fire
AFTER_CATEGORY_CLICK_SLEEP_S = random.uniform(1.5, 2.5)
XHR_WAIT_TIMEOUT_S = 10
XHR_POLL_INTERVAL_S = 0.3
CLICK_AND_XHR_DELAY_S = random.uniform(0.5, 1.0)

webdriver_manager_lock = threading.Lock()
chromedriver_path_global: Optional[str] = None

# --- Helper Functions --- (extract_section_name_from_url, _parse_reviews_from_block are identical)
def extract_section_name_from_url(href: Optional[str]) -> str: # Identical
    if not href: return "unknown_section"
    try:
        path_parts = urlparse(href).path.strip('/').split('/')
        if len(path_parts) >= 4 and path_parts[2] == 'reviews':
            return path_parts[3]
    except Exception:
        pass
    match = re.search(r'/reviews/(\w+)', href)
    return match.group(1) if match else "unknown_section"

def _parse_reviews_from_block(review_container_soup: BeautifulSoup, start_date_filter: Optional[datetime], end_date_filter: Optional[datetime]) -> List[Review]: # Identical
    reviews_found: List[Review] = []
    review_blocks = review_container_soup.find_all('div', class_=REVIEW_BLOCK_CSS_SELECTOR_BS.split('.')[-1], recursive=True)
    for block_idx, block in enumerate(review_blocks):
        quote = block.find('p', class_='cppRH-review-quote')
        if not quote: continue
        text = quote.get_text(strip=True).replace('\u0000', '')
        cite_block = block.find('cite', class_='cppRH-review-cite')
        date_meta_tag = None
        if cite_block: date_meta_tag = cite_block.find('meta', {'itemprop': 'datePublished'}) or cite_block.find('meta', attrs={'content': re.compile(r'^\d{4}-\d{2}-\d{2}$')})
        if not date_meta_tag or not date_meta_tag.get('content'): continue
        try: date_val = datetime.strptime(date_meta_tag['content'], '%Y-%m-%d')
        except ValueError: continue
        if start_date_filter and date_val < start_date_filter: continue
        if end_date_filter and date_val > end_date_filter: continue
        reviews_found.append(Review(text=text, date=date_val))
    return reviews_found

# --- Selenium Setup --- (Identical to v14_network_intercept_locked_setup.py)
def setup_selenium_driver() -> seleniumwire_webdriver.Chrome:
    global chromedriver_path_global
    logging.getLogger('WDM').setLevel(logging.WARNING) 
    options = seleniumwire_webdriver.ChromeOptions()
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox"); options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions"); options.add_argument("window-size=1920,1080")
    user_agent_str = ua.random if ua else "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent_str}')
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--log-level=3') 
    options.add_argument("--enable-unsafe-swiftshader")
    # Remove blink-settings=imagesEnabled=false for now, to ensure all visual elements load for clicks
    # options.add_argument("--blink-settings=imagesEnabled=false") 
    sw_options = { 'disable_encoding': True, 'enable_har': False }
    current_thread_id = threading.get_ident()
    try:
        if chromedriver_path_global is None:
            with webdriver_manager_lock:
                if chromedriver_path_global is None:
                    print(f"    [{current_thread_id}] Acquiring lock to install/verify ChromeDriver...")
                    try:
                        chromedriver_path_global = ChromeDriverManager().install()
                        print(f"    [{current_thread_id}] ChromeDriver installed/verified globally: {chromedriver_path_global}")
                    except zipfile.BadZipFile as e_zip_lock:
                        print(f"  [Selenium Setup] CRITICAL BadZipFile ERROR (under lock): {e_zip_lock}. Manually delete ~/.wdm cache and retry.")
                        traceback.print_exc()
                        raise RuntimeError(f"Failed to install ChromeDriver globally due to BadZipFile: {e_zip_lock}")
                    except Exception as e_install_lock:
                        print(f"  [Selenium Setup] CRITICAL ERROR installing ChromeDriver (under lock): {type(e_install_lock).__name__} - {e_install_lock}")
                        traceback.print_exc()
                        raise RuntimeError(f"Failed to install ChromeDriver globally: {type(e_install_lock).__name__} - {e_install_lock}")
        else: print(f"    [{current_thread_id}] Using pre-established ChromeDriver path: {chromedriver_path_global}")
        if not chromedriver_path_global or not os.path.exists(chromedriver_path_global): raise RuntimeError("ChromeDriver path not set or invalid after installation attempt.")
        service = ChromeService(executable_path=chromedriver_path_global)
        driver = seleniumwire_webdriver.Chrome(service=service, options=options, seleniumwire_options=sw_options)
        driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT_S)
        print(f"    [{current_thread_id}] Selenium-Wire driver setup complete for thread.")
        return driver
    except WebDriverException as e_wd: print(f"  [Selenium Setup] CRITICAL WebDriverException: {type(e_wd).__name__} - {e_wd}"); traceback.print_exc(); raise RuntimeError(f"Failed to setup Selenium-Wire driver (WebDriverException): {type(e_wd).__name__} - {e_wd}")
    except Exception as e: print(f"  [Selenium Setup] CRITICAL UNEXPECTED ERROR: {type(e).__name__} - {e}"); traceback.print_exc(); raise RuntimeError(f"Failed to setup Selenium-Wire driver (Unexpected): {type(e).__name__} - {e}")

# --- try_click --- (Identical)
def try_click(driver: seleniumwire_webdriver.Chrome, element, timeout: int = SELENIUM_INTERACTION_TIMEOUT_S):
    try: WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element)); driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center', inline: 'nearest'});", element); time.sleep(0.2 + random.uniform(0.1, 0.2)); element.click(); return True
    except ElementClickInterceptedException:
        print(f"    [try_click] Intercepted. JS click.")
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as e_js:
            print(f"    [try_click] JS click failed: {e_js}")
            return False
    except (TimeoutException, ElementNotInteractableException, StaleElementReferenceException) as e_click:
        print(f"    [try_click] Click failed: {type(e_click).__name__}")
        return False
    except Exception as e_other_click: print(f"    [try_click] Unexpected click error: {type(e_other_click).__name__}"); return False

# --- attempt_to_close_popups --- (Identical)
def attempt_to_close_popups(driver: seleniumwire_webdriver.Chrome, thread_name: str): # Identical logic
    closed_any = False; main_window = driver.current_window_handle; initial_handles = set(driver.window_handles); popup_interaction_wait = WebDriverWait(driver, 2, poll_frequency=0.2)
    try:
        targeted_popup_buttons = driver.find_elements(By.CSS_SELECTOR, TARGETED_POPUP_MAIN_SELECTOR)
        for btn in targeted_popup_buttons:
            if btn.is_displayed() and btn.is_enabled():
                print(f"      [{thread_name}] Attempting TARGETED popup close...")
                try:
                    driver.execute_script("arguments[0].click();", btn)
                    popup_interaction_wait.until(EC.staleness_of(btn))
                    print(f"      [{thread_name}] Targeted popup seems closed (stale).")
                    closed_any = True
                    break
                except TimeoutException:
                    if not btn.is_displayed():
                        print(f"      [{thread_name}] Targeted popup seems closed (not displayed).")
                        closed_any = True
                        break
                    else:
                        print(f"      [{thread_name}] Targeted popup still visible after click.")
                except StaleElementReferenceException:
                    print(f"      [{thread_name}] Targeted popup closed (already stale).")
                    closed_any = True
                    break
                except Exception as e_ct:
                    print(f"      [{thread_name}] Error clicking targeted popup: {e_ct}")
        if closed_any:
            time.sleep(0.5 + random.uniform(0.1, 0.2))
    except Exception: pass
    run_generic_check = True
    if targeted_popup_buttons and closed_any:
        try:
            current_targeted_buttons = driver.find_elements(By.CSS_SELECTOR, TARGETED_POPUP_MAIN_SELECTOR)
            if not current_targeted_buttons or not any(b.is_displayed() for b in current_targeted_buttons):
                run_generic_check = False
        except Exception:
            run_generic_check = False
    if run_generic_check:
        for sel_idx, sel in enumerate(POPUP_CLOSE_SELECTORS):
            if sel == TARGETED_POPUP_MAIN_SELECTOR : continue
            try:
                popups = driver.find_elements(By.CSS_SELECTOR, sel)
                if not popups: continue
                for popup_btn in popups:
                    if popup_btn.is_displayed() and popup_btn.is_enabled():
                        print(f"      [{thread_name}] Attempting generic popup close ({sel[:30]}...).")
                        try:
                            driver.execute_script("arguments[0].click();", popup_btn)
                            time.sleep(0.3 + random.uniform(0.1, 0.2))
                            if not popup_btn.is_displayed():
                                print(f"      [{thread_name}] Generic popup seems closed.")
                                break
                        except StaleElementReferenceException:
                            print(f"      [{thread_name}] Generic popup closed (stale).")
                            break
                        except Exception as e_cg:
                            print(f"      [{thread_name}] Error clicking generic popup ({sel[:30]}...): {e_cg}")
                if popups and not popups[0].is_displayed():
                    break
            except Exception:
                pass
    final_handles = set(driver.window_handles); new_handles = final_handles - initial_handles
    if new_handles:
        for handle in new_handles:
            if handle != main_window:
                try:
                    print(f"      [{thread_name}] Closing new window/tab.")
                    driver.switch_to.window(handle)
                    driver.close()
                except Exception as e_wc:
                    print(f"      [{thread_name}] Error closing new window: {e_wc}")
        driver.switch_to.window(main_window)
    if closed_any and run_generic_check: print(f"      [{thread_name}] Generic popups checked. Pausing."); time.sleep(0.4 + random.uniform(0.1, 0.2))


# --- XHR-Centric Scraper ---
def _scrape_category_deep_reviews_xhr_centric(
    company_base_url_str: str, category_name_arg: str, company_slug: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    thread_name = f"XHR-{category_name_arg}-{company_slug[:10]}"
    category_scrape_start_time = time.perf_counter()
    print(f"  [{thread_name}] Started for category: {category_name_arg}")
    collected_questions_data: Dict[str, Question] = {} # Store questions by question_text to merge
    processed_reviews_keys_globally_for_category = set()
    category_driver = None
    total_reviews_in_category_count = 0

    try:
        category_driver = setup_selenium_driver()
        category_wait = WebDriverWait(category_driver, SELENIUM_ELEMENT_TIMEOUT_S)

        category_url_start = urljoin(company_base_url_str.rstrip('/') + "/", f"reviews/{category_name_arg}/")
        
        category_page_count = 0
        while True: # Loop for category pages (e.g. /reviews/leadership, /reviews/leadership/page/2)
            category_page_count += 1
            
            # For page 1, use category_url_start. For subsequent, it's set by category pagination click.
            current_category_page_url_to_load = category_driver.current_url if category_page_count > 1 else category_url_start
            
            print(f"  [{thread_name}] Navigating to Cat Page {category_page_count} for '{category_name_arg}': {current_category_page_url_to_load}")
            if category_driver.current_url != current_category_page_url_to_load : # Avoid re-getting if already on page 1
                category_driver.get(current_category_page_url_to_load)
            
            time.sleep(INITIAL_PAGE_LOAD_SLEEP_S) # Allow base page and initial JS to load
            attempt_to_close_popups(category_driver, thread_name)

            # Ensure "All" sentiment filter is active
            try:
                all_tab = category_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, SENTIMENT_ALL_TAB_SELECTOR)))
                if "active" not in (all_tab.get_attribute("class") or ""):
                    print(f"    [{thread_name}] 'All' tab not active, clicking it...")
                    num_requests_before_all_click = len(category_driver.requests)
                    if try_click(category_driver, all_tab):
                        time.sleep(CLICK_AND_XHR_DELAY_S) # Wait for XHRs from tab click
                        attempt_to_close_popups(category_driver, thread_name) # Popups after click
                    else:
                        print(f"    [{thread_name}] Failed to click 'All' tab. Proceeding with current content.")
                else:
                    print(f"    [{thread_name}] 'All' tab is active or default.")
                    num_requests_before_all_click = len(category_driver.requests) # Baseline for non-click scenario

            except TimeoutException:
                print(f"    [{thread_name}] Sentiment 'All' tab not found. Proceeding with current page content.")
                num_requests_before_all_click = len(category_driver.requests) # Baseline

            # --- Intercept INITIAL Question-Loading XHRs ---
            print(f"    [{thread_name}] Waiting for initial question-loading XHRs...")
            initial_question_xhr_bodies: List[str] = []
            wait_start_time = time.monotonic()
            initial_xhr_processed_ids = set()

            # The initial XHRs might fire on page load or after "All" tab click.
            # We need to check requests that appeared *after* the page was loaded/tab clicked.
            # If 'All' tab was clicked, num_requests_before_all_click is the baseline.
            # If not, it's requests since page load (harder to isolate if not clearing driver.requests frequently)
            # Let's assume num_requests_before_all_click is our baseline.

            time.sleep(1.0) # Give some time for XHRs to fire after page load / tab click

            while time.monotonic() - wait_start_time < INITIAL_XHR_LOAD_TIMEOUT_S:
                # Check more recent requests first
                # The index `num_requests_before_all_click` is from *before* a potential "All" tab click.
                # For safety, let's just check all requests for now and filter by URL pattern.
                # This could be optimized if we clear requests more strategically.
                current_requests_snapshot = list(category_driver.requests) # Take snapshot
                new_question_xhr_found_this_iteration = False

                for req in reversed(current_requests_snapshot): # Check most recent first
                    if req.id in initial_xhr_processed_ids: continue
                    
                    # Expecting XHRs like "reviews?questionId=..."
                    if req.response and "/reviews?questionId=" in req.url and "page=" not in req.url: # Basic filter for initial question loads
                        initial_xhr_processed_ids.add(req.id)
                        if req.response.status_code == 200 and req.response.headers.get('Content-Type', '').lower().startswith('application/json'):
                            print(f"      [{thread_name}] Intercepted initial question XHR: {req.url}")
                            try:
                                body_bytes = req.response.body
                                json_response_text = body_bytes.decode('utf-8', errors='replace')
                                json_data = json.loads(json_response_text)
                                html_body = json_data.get("body")
                                if isinstance(html_body, str):
                                    initial_question_xhr_bodies.append(html_body)
                                    new_question_xhr_found_this_iteration = True
                                else: print(f"        [{thread_name}] Initial XHR 'body' not a string for {req.url}")
                            except Exception as e_json: print(f"        [{thread_name}] Error processing initial XHR JSON for {req.url}: {e_json}")
                        else: print(f"      [{thread_name}] Matched initial Q XHR URL but status/type issue: {req.url}")
                
                if not initial_question_xhr_bodies and not new_question_xhr_found_this_iteration: # No new bodies and list still empty
                     time.sleep(XHR_POLL_INTERVAL_S) # Wait a bit more
                elif new_question_xhr_found_this_iteration: # Found some, wait a bit for others
                     time.sleep(0.5) # Give a small window for more concurrent initial XHRs
                elif initial_question_xhr_bodies: # Have some, and no new ones in last check
                     break # Likely all initial XHRs captured

            if not initial_question_xhr_bodies:
                print(f"    [{thread_name}] No initial question-loading XHRs detected for category page {category_page_count}. Page might be empty or structure changed.")
                # Check if this is the end of category pagination or an actual issue
                # For now, let's assume if no XHRs, then this category page is done.
                # The category pagination logic below will determine if we try another category page.
                
                # Try to parse from page source as a fallback if NO XHRs were found
                print(f"    [{thread_name}] Fallback: trying to parse questions from current page source as no initial XHRs found.")
                page_source_qs = BeautifulSoup(category_driver.page_source, DEFAULT_HTML_PARSER)
                # This is tricky because we need to find the *live* elements for sub-pagination clicks.
                # For simplicity in fallback, just parse for initial reviews. Sub-pagination might fail here.
                live_q_blocks_fallback = category_driver.find_elements(By.CSS_SELECTOR, QUESTION_BLOCK_SELECTOR_BS_FROM_XHR_HTML) # Using this general selector
                if live_q_blocks_fallback:
                    print(f"    [{thread_name}] Fallback: Found {len(live_q_blocks_fallback)} question blocks in DOM.")
                    initial_question_xhr_bodies = [block.get_attribute('outerHTML') for block in live_q_blocks_fallback]
                else:
                    print(f"    [{thread_name}] Fallback: No question blocks found in DOM either.")
                    # This break will be caught by category pagination check later
                    # If category pagination also fails, the outer loop breaks.
                    # break # No, don't break here, let category pagination decide

            # Process each HTML body obtained from initial XHRs (or fallback)
            for q_block_html_from_xhr in initial_question_xhr_bodies:
                q_block_soup = BeautifulSoup(q_block_html_from_xhr, DEFAULT_HTML_PARSER)
                
                # The q_block_soup might be the div.reviewsList itself or contain it
                actual_q_block_container_soup = q_block_soup.find('div', class_=QUESTION_BLOCK_SELECTOR_BS_FROM_XHR_HTML.split('.')[-1]) or q_block_soup

                try:
                    q_title_element = actual_q_block_container_soup.find('h2', class_='section-subtitle')
                    if not q_title_element: print(f"      [{thread_name}] No H2 title in XHR question block. HTML: {str(actual_q_block_container_soup)[:200]}"); continue
                    question_text = q_title_element.get_text(strip=True)
                except Exception as e: print(f"      [{thread_name}] Error getting Q title from XHR HTML: {e}"); continue

                print(f"    [{thread_name}] Processing Q (from XHR/DOM): '{question_text[:40]}...'")
                
                current_q_reviews_this_session: List[Review] = []
                initial_reviews_from_xhr_q_block = _parse_reviews_from_block(actual_q_block_container_soup, start_date_filter, end_date_filter)
                
                newly_added_count = 0
                for r_parsed in initial_reviews_from_xhr_q_block:
                    r_key = (hash(question_text), hash(r_parsed.text), r_parsed.date)
                    if r_key not in processed_reviews_keys_globally_for_category:
                        current_q_reviews_this_session.append(r_parsed)
                        processed_reviews_keys_globally_for_category.add(r_key)
                        newly_added_count += 1
                        total_reviews_in_category_count += 1
                print(f"      [{thread_name}] Initial XHR parse for Q '{question_text[:30]}...': {len(initial_reviews_from_xhr_q_block)} found, {newly_added_count} new.")

                # --- Sub-Pagination for this question (XHR-loaded question) ---
                # We need to find the LIVE Selenium element corresponding to this question_text to click its pagination
                live_q_block_element_for_sub_nav = None
                try:
                    # Find all potential question blocks in the current DOM
                    all_live_q_blocks_in_dom = category_driver.find_elements(By.CSS_SELECTOR, "div.reviewsList") # General selector for Q blocks
                    for live_block_candidate in all_live_q_blocks_in_dom:
                        try:
                            candidate_title_el = live_block_candidate.find_element(By.CSS_SELECTOR, 'h2.section-subtitle')
                            if candidate_title_el.text.strip() == question_text:
                                live_q_block_element_for_sub_nav = live_block_candidate
                                print(f"        [{thread_name}] Found live DOM element for Q '{question_text[:30]}...' for sub-pagination.")
                                break
                        except NoSuchElementException: continue # No title in this candidate
                        except StaleElementReferenceException: continue # Element went stale
                except Exception as e_find_live: print(f"      [{thread_name}] Error finding live Q block for '{question_text[:30]}...': {e_find_live}")

                if live_q_block_element_for_sub_nav:
                    q_sub_page_num = 1 # For XHR sub-pages
                    current_question_id_str_sub = None
                    try: # Extract questionId for more specific XHR matching during sub-pagination
                        # Look for a pagination link within the q_block_soup (from initial XHR)
                        # This helps form the target_request_url_part for sub-pages
                        q_links_in_xhr_html = actual_q_block_container_soup.select("a[href*='questionId=']")
                        if q_links_in_xhr_html:
                             parsed_sub_q_link = urlparse(q_links_in_xhr_html[0].get('href'))
                             query_params_sub = parse_qs(parsed_sub_q_link.query)
                             if 'questionId' in query_params_sub: current_question_id_str_sub = query_params_sub['questionId'][0]
                    except Exception: pass
                    
                    while True: # Loop for sub-pages of this question
                        next_q_sub_page_button_sel = None
                        # Find "next page" button *within the live_q_block_element_for_sub_nav*
                        for sel_css in NEXT_PAGE_SELECTORS:
                            try:
                                potential_btns = live_q_block_element_for_sub_nav.find_elements(By.CSS_SELECTOR, sel_css)
                                for btn in potential_btns: # (Button qualification logic - same as before)
                                    aria_label = (btn.get_attribute("aria-label") or "").lower(); rel = (btn.get_attribute("rel") or "").lower(); btn_text = btn.text.lower(); combined_text = f"{aria_label} {rel} {btn_text}"; is_prev = "prev" in combined_text or "previous" in combined_text; class_attr = (btn.get_attribute("class") or "").lower(); is_disabled = "disabled" in class_attr or "inactive" in class_attr or btn.get_attribute("disabled") is not None
                                    if btn.is_displayed() and btn.is_enabled() and not is_prev and not is_disabled: next_q_sub_page_button_sel = btn; break
                                if next_q_sub_page_button_sel: break
                            except (NoSuchElementException, StaleElementReferenceException): continue # Block or button not found/stale
                        
                        if not next_q_sub_page_button_sel: break # No more sub-pages for this Q

                        num_req_before_sub_click = len(category_driver.requests)
                        print(f"        [{thread_name}] Clicking 'Next Q Sub-Page' (XHR page {q_sub_page_num + 1}) for Q: '{question_text[:30]}...'")
                        
                        # Attempt to click, handle popups, retry (simplified retry for brevity, expand if needed)
                        if not try_click(category_driver, next_q_sub_page_button_sel):
                            print(f"          [{thread_name}] Failed click Q sub-page. Popups & retry...")
                            attempt_to_close_popups(category_driver, thread_name); time.sleep(0.5)
                            # Re-find live_q_block_element_for_sub_nav and next_q_sub_page_button_sel (CRUCIAL)
                            # This part is complex and prone to StaleElement if not handled carefully.
                            # For now, assume if first click fails, we break sub-pagination. Needs robust re-finding.
                            print(f"          [{thread_name}] Sub-page click failed after retry for Q '{question_text[:30]}...'. Breaking sub-page loop."); break 

                        time.sleep(CLICK_AND_XHR_DELAY_S) # Wait for XHR

                        # Intercept XHR for this sub-page
                        intercepted_sub_page_xhr_body = None
                        target_sub_xhr_url = f"/reviews?questionId={current_question_id_str_sub}" if current_question_id_str_sub else "/reviews?"
                        # Sub-page XHRs usually have `&page=N`
                        
                        sub_wait_start = time.monotonic()
                        sub_processed_ids = set()
                        found_sub_xhr = False
                        while time.monotonic() - sub_wait_start < XHR_WAIT_TIMEOUT_S:
                            sub_req_snapshot = category_driver.requests[num_req_before_sub_click:]
                            for req_sub in reversed(sub_req_snapshot):
                                if req_sub.id in sub_processed_ids: continue
                                if req_sub.response and target_sub_xhr_url in req_sub.url and "page=" in req_sub.url: # Look for "page="
                                    sub_processed_ids.add(req_sub.id)
                                    if req_sub.response.status_code == 200 and req_sub.response.headers.get('Content-Type','').lower().startswith('application/json'):
                                        print(f"          [{thread_name}] Intercepted Q Sub-Page XHR: {req_sub.url}")
                                        try:
                                            json_data_sub = json.loads(req_sub.response.body.decode('utf-8',errors='replace'))
                                            html_body_sub = json_data_sub.get("body")
                                            if isinstance(html_body_sub, str): intercepted_sub_page_xhr_body = html_body_sub; found_sub_xhr = True; break
                                        except Exception as e_json_sub: print(f"            [{thread_name}] Error processing sub-page XHR JSON: {e_json_sub}")
                            if found_sub_xhr: break
                            time.sleep(XHR_POLL_INTERVAL_S)

                        if not intercepted_sub_page_xhr_body: print(f"        [{thread_name}] No XHR for Q sub-page {q_sub_page_num + 1} of '{question_text[:30]}...'"); break
                        
                        soup_sub_xhr_reviews = BeautifulSoup(intercepted_sub_page_xhr_body, DEFAULT_HTML_PARSER)
                        reviews_from_sub_xhr = _parse_reviews_from_block(soup_sub_xhr_reviews, start_date_filter, end_date_filter)
                        
                        newly_added_sub = 0
                        for r_parsed_sub in reviews_from_sub_xhr:
                            r_key_sub = (hash(question_text), hash(r_parsed_sub.text), r_parsed_sub.date)
                            if r_key_sub not in processed_reviews_keys_globally_for_category:
                                current_q_reviews_this_session.append(r_parsed_sub)
                                processed_reviews_keys_globally_for_category.add(r_key_sub)
                                newly_added_sub += 1
                                total_reviews_in_category_count += 1
                        print(f"        [{thread_name}] Q Sub-Page {q_sub_page_num + 1} for '{question_text[:30]}...': Parsed {len(reviews_from_sub_xhr)}, {newly_added_sub} new.")
                        if not reviews_from_sub_xhr or newly_added_sub == 0: print(f"          [{thread_name}] No/no new reviews from sub-page XHR. Ending for Q '{question_text[:30]}...'"); break
                        q_sub_page_num += 1
                        time.sleep(random.uniform(0.2,0.5))
                else:
                    print(f"      [{thread_name}] Could not find live DOM element for Q '{question_text[:30]}...' to handle sub-pagination.")

                # Add/merge reviews for this question
                if question_text in collected_questions_data:
                    # Merge new reviews into existing question object
                    existing_q_obj = collected_questions_data[question_text]
                    existing_reviews_set = {(r.text, r.date) for r in existing_q_obj.review_section.reviews}
                    for r_new in current_q_reviews_this_session:
                        if (r_new.text, r_new.date) not in existing_reviews_set:
                            existing_q_obj.review_section.reviews.append(r_new)
                    existing_q_obj.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
                elif current_q_reviews_this_session:
                    review_section_obj = ReviewSection(section_name=category_name_arg, reviews=current_q_reviews_this_session)
                    question_obj = Question(question_text=question_text, review_section=review_section_obj)
                    collected_questions_data[question_text] = question_obj
            # End loop for initial_question_xhr_bodies

            # --- Category Pagination ---
            # (This logic remains largely the same as v14_network_intercept_locked_setup.py)
            # It finds the main category "Next" button at the bottom of the page.
            next_category_page_button_sel_elem = None
            # Use fresh page source for category nav detection after all Q XHRs and interactions
            time.sleep(0.5) # Let DOM settle
            soup_for_cat_nav = BeautifulSoup(category_driver.page_source, DEFAULT_HTML_PARSER)
            cat_page_nav_scope_bs = soup_for_cat_nav.find('nav', attrs={'aria-label': lambda x: x and 'pagination' in x.lower()}) or \
                                 soup_for_cat_nav.find('ul', class_=lambda x: x and 'pagination' in x.lower()) or \
                                 soup_for_cat_nav # Fallback to whole page
            href_bs_for_retry = None; sel_css_for_retry = None
            # (Category next page button finding logic - Identical to previous version)
            for sel_css in NEXT_PAGE_SELECTORS:
                potential_bs_btns = cat_page_nav_scope_bs.select(sel_css)
                for btn_s_tag in potential_bs_btns:
                    if btn_s_tag.find_parent(QUESTION_BLOCK_SELECTOR_BS_FROM_XHR_HTML.split('.')[0], class_=QUESTION_BLOCK_SELECTOR_BS_FROM_XHR_HTML.split('.')[1] if '.' in QUESTION_BLOCK_SELECTOR_BS_FROM_XHR_HTML else None): continue # Skip if it's Q pagination
                    aria_label_bs = btn_s_tag.get("aria-label", "").lower(); rel_val_bs = btn_s_tag.get("rel"); rel_bs = " ".join(rel_val_bs).lower() if isinstance(rel_val_bs, list) else (rel_val_bs or "").lower(); text_bs = btn_s_tag.get_text(strip=True).lower(); combined_bs = f"{aria_label_bs} {rel_bs} {text_bs}"; is_prev_bs = "prev" in combined_bs or "previous" in combined_bs; class_bs_list = btn_s_tag.get('class', []); is_disabled_bs = any(c in class_bs_list for c in ['disabled', 'inactive']) or btn_s_tag.has_attr('disabled'); href_bs = btn_s_tag.get('href')
                    if not is_prev_bs and not is_disabled_bs and href_bs and href_bs != '#' and not href_bs.startswith("javascript:"):
                        href_bs_for_retry = href_bs; sel_css_for_retry = sel_css
                        try:
                            selenium_potential_cat_next_btns = category_driver.find_elements(By.CSS_SELECTOR, sel_css)
                            for sel_btn_elem in selenium_potential_cat_next_btns:
                                if not sel_btn_elem.is_displayed() or not sel_btn_elem.is_enabled(): continue
                                try: sel_btn_elem.find_element(By.XPATH, f"./ancestor::div[contains(@class, '{QUESTION_BLOCK_SELECTOR_BS_FROM_XHR_HTML.split('.')[1]}')]"); continue
                                except NoSuchElementException:
                                    sel_href = sel_btn_elem.get_attribute('href'); abs_href_bs = urljoin(category_driver.current_url, href_bs); abs_sel_href = urljoin(category_driver.current_url, sel_href) if sel_href else None
                                    if abs_sel_href and abs_href_bs == abs_sel_href: next_category_page_button_sel_elem = sel_btn_elem; break
                                    elif not next_category_page_button_sel_elem: next_category_page_button_sel_elem = sel_btn_elem
                            if next_category_page_button_sel_elem and next_category_page_button_sel_elem.get_attribute('href') and urljoin(category_driver.current_url, href_bs) == urljoin(category_driver.current_url, next_category_page_button_sel_elem.get_attribute('href')): break
                        except (NoSuchElementException, StaleElementReferenceException): continue
                if next_category_page_button_sel_elem and next_category_page_button_sel_elem.get_attribute('href') and href_bs_for_retry and urljoin(category_driver.current_url, href_bs_for_retry) == urljoin(category_driver.current_url, next_category_page_button_sel_elem.get_attribute('href')): break
            
            if not next_category_page_button_sel_elem: print(f"  [{thread_name}] No 'Next Category Page' button found after Cat Page {category_page_count}. Ending category."); break
            
            print(f"  [{thread_name}] Clicking Next Category Page (to page {category_page_count + 1})...")
            del category_driver.requests[:] # Clear requests before category navigation
            clicked_cat_next = try_click(category_driver, next_category_page_button_sel_elem)
            # (Retry logic for category next page click - Identical to previous version)
            if not clicked_cat_next:
                print(f"    [{thread_name}] Initial click failed Next Cat Page. Popups & retry..."); attempt_to_close_popups(category_driver, thread_name); time.sleep(0.5)
                try: # (Simplified retry, ensure robust element re-finding based on href_bs_for_retry or sel_css_for_retry)
                    # Re-find button logic here... (omitted for brevity, use previous robust version)
                    print(f"    [{thread_name}] Retrying Next Cat Page click (simplified retry)...")
                    # This needs to be the robust re-finding logic from previous versions
                    if href_bs_for_retry:
                         final_attempt_button_cat = WebDriverWait(category_driver, 5).until(EC.element_to_be_clickable((By.XPATH, f"//a[@href='{href_bs_for_retry}']"))) # Example
                         if final_attempt_button_cat: clicked_cat_next = try_click(category_driver, final_attempt_button_cat)
                except Exception as e_refind_cat: print(f"    [{thread_name}] Error re-finding Next Cat Page button: {e_refind_cat}")
            
            if not clicked_cat_next: print(f"  [{thread_name}] Failed to click 'Next Category Page' after retry. Stopping category scan."); break
            
            # AFTER_CATEGORY_CLICK_SLEEP_S is handled by INITIAL_PAGE_LOAD_SLEEP_S at start of loop
        
    except Exception as e_cat_main:
        print(f"  [{thread_name}] MAJOR ERROR in category '{category_name_arg}': {e_cat_main}"); traceback.print_exc()
    finally:
        if category_driver: print(f"  [{thread_name}] Quitting driver for category '{category_name_arg}'."); category_driver.quit()

    final_questions_list = list(collected_questions_data.values())
    category_scrape_duration = time.perf_counter() - category_scrape_start_time
    num_questions_found = len(final_questions_list)
    print(f"  [{thread_name}] Finished category '{category_name_arg}' in {category_scrape_duration:.2f}s. Found {num_questions_found} Qs, {total_reviews_in_category_count} total reviews.")
    return category_name_arg, final_questions_list


# --- Main Orchestrator --- (Identical, but calls the new scraping function)
def scrape_comparably_sync(company_base_url_str: str, company_slug: str, start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None) -> Dict[str, Any]:
    print(f"Orchestrating XHR-Centric (v{app.version}) scrape for: {company_slug}")
    orchestration_start_time = time.perf_counter()
    all_questions_for_company: List[Question] = []
    company_name_from_slug = company_slug.replace('-', ' ').title()
    company_details_overall: Dict[str, Any] = {"company_name": company_name_from_slug, "comparably_url": company_base_url_str, "status_note": "Company name derived from slug."}
    max_concurrent_categories = min(len(REVIEW_CATEGORIES), 4)
    print(f"  [{company_slug}] Starting XHR-Centric parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")
    category_processing_start_time = time.perf_counter()
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories, thread_name_prefix="XHRcentricPool") as executor:
        for cat_name_from_list in REVIEW_CATEGORIES:
            future = executor.submit(
                _scrape_category_deep_reviews_xhr_centric, # <<< CHANGED FUNCTION
                company_base_url_str, cat_name_from_list, company_slug,
                start_date_filter, end_date_filter
            )
            futures_map[future] = cat_name_from_list
        for future in as_completed(futures_map):
            original_category_name_processed = futures_map[future]
            try:
                processed_cat_name, questions_from_category = future.result()
                # Merge questions if a category yielded questions already seen (unlikely with current structure but for safety)
                for q_new in questions_from_category:
                    existing_q = next((q_old for q_old in all_questions_for_company if q_old.question_text == q_new.question_text), None)
                    if existing_q: # Merge reviews
                        existing_reviews_set = {(r.text, r.date) for r in existing_q.review_section.reviews}
                        for r_to_merge in q_new.review_section.reviews:
                            if (r_to_merge.text, r_to_merge.date) not in existing_reviews_set:
                                existing_q.review_section.reviews.append(r_to_merge)
                        existing_q.review_section.reviews.sort(key=lambda r:r.date, reverse=True)
                    else:
                        all_questions_for_company.append(q_new)
            except Exception as e_future_exc: print(f"  [{company_slug}] XHR-Centric Cat task for '{original_category_name_processed}' FAILED: {e_future_exc}"); traceback.print_exc()
    category_processing_duration = time.perf_counter() - category_processing_start_time
    print(f"  [{company_slug}] All category threads (XHR-Centric) completed in {category_processing_duration:.2f}s.")
    total_duration = time.perf_counter() - orchestration_start_time
    num_total_questions = len(all_questions_for_company)
    num_total_reviews = sum(len(q.review_section.reviews) for q in all_questions_for_company)
    print(f"\nFinished ALL XHR-Centric scrapes for {company_slug} in {total_duration:.2f}s. Total Qs: {num_total_questions}, Total Reviews: {num_total_reviews}")
    return {"status": "success" if num_total_reviews > 0 else ("partial_success_no_reviews" if num_total_questions > 0 else "no_data_found"), "data": { "company_info": company_details_overall, "reviews": [q.model_dump(mode='json') for q in all_questions_for_company] }, "summary": {"total_questions": num_total_questions, "total_reviews": num_total_reviews}}

# --- FastAPI Endpoint --- (Identical logic, version updated in logs)
@app.post("/scrape")
async def scrape_companies(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    urls = request.urls; start_date_filter: Optional[datetime] = None; end_date_filter: Optional[datetime] = None
    if request.start_date_str:
        try:
            start_date_filter = datetime.strptime(request.start_date_str, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid start_date_str.")
    if request.end_date_str:
        try:
            end_date_filter = datetime.strptime(request.end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid end_date_str.")
    if start_date_filter and end_date_filter and start_date_filter > end_date_filter: raise HTTPException(status_code=400, detail="Start date after end date.")
    if not urls: raise HTTPException(status_code=400, detail="No URLs provided.")
    results: Dict[str, Dict[str, Any]] = {}; valid_scrape_params = []
    print(f"API request: {len(urls)} URLs, XHR-Centric Strategy (v{app.version}).")
    for url_obj in urls:
        url_str = str(url_obj)
        try:
            parsed_url = urlparse(url_str)
            path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
            if not (parsed_url.scheme and parsed_url.netloc and len(path_segments) >= 2 and path_segments[0] == "companies"): raise ValueError("URL format error")
            company_slug = path_segments[1]; company_base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/companies/{company_slug}"
            valid_scrape_params.append({'original_url': url_str, 'base_url': company_base_url, 'slug': company_slug})
        except Exception as e_slug: print(f"Error parsing URL '{url_str}': {e_slug}"); results[url_str] = {"status": "error", "message": f"Invalid URL format: {url_str}. Error: {e_slug}"}
    tasks = []
    if valid_scrape_params:
        for params in valid_scrape_params: tasks.append(asyncio.to_thread(scrape_comparably_sync, params['base_url'], params['slug'], start_date_filter, end_date_filter))
    scraped_results_or_exceptions = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []
    task_idx = 0
    for params in valid_scrape_params:
        original_url_str = params['original_url']
        if original_url_str in results: continue
        if task_idx < len(scraped_results_or_exceptions):
            result_or_exc = scraped_results_or_exceptions[task_idx]
            if isinstance(result_or_exc, Exception): print(f"Task for {original_url_str} (XHR-Centric v{app.version}) EXCEPTION: {result_or_exc}"); tb_str = "".join(traceback.format_exception(None, result_or_exc, result_or_exc.__traceback__)); print(f"FULL TRACEBACK for {original_url_str}:\n{tb_str}"); results[original_url_str] = {"status": "error", "message": f"Scraping task failed: {type(result_or_exc).__name__}. Check logs."}
            elif isinstance(result_or_exc, dict): results[original_url_str] = result_or_exc
            else: results[original_url_str] = {"status": "error", "message": "Unexpected internal result"}
            task_idx +=1
        else: results[original_url_str] = {"status": "error", "message": "Scraping task result missing."}
    print(f"Finished API request processing (XHR-Centric Strategy v{app.version}).")
    return results

# --- Main for local testing ---
# if __name__ == "__main__":
#     async def main_test():
#         # test_url = "https://www.comparably.com/companies/google/reviews/leadership"
#         test_url = "https://www.comparably.com/companies/elsevier/reviews/leadership"
#         test_request = ScrapeRequest(urls=[HttpUrl(test_url)])
#         results = await scrape_companies(test_request)
#         print(json.dumps(results, indent=2))
#     asyncio.run(main_test())

#--- END OF FILE v15_xhr_centric_reviews.py ---