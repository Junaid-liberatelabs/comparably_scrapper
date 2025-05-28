
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
    title="Comparably Scraper API - Hybrid Optimized (Pre-Click Overlay Wait)",
    description="Optimized Hybrid: Selenium (pre-click overlay wait), Curl-CFFI Q-Rev.",
    version="2.3.5" # Incremented version
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

try: import lxml; DEFAULT_HTML_PARSER = "lxml"; print("INFO: Using lxml for HTML parsing.")
except ImportError: print("Warning: lxml not installed, using html.parser."); DEFAULT_HTML_PARSER = "html.parser"

# --- Constants ---
REVIEW_CATEGORIES = ["leadership", "compensation", "team", "environment", "outlook"]
MAX_CATEGORY_PAGES = 100
MAX_REVIEW_PAGES_PER_QUESTION = 200 # Keep this reasonable to avoid excessive requests
SELENIUM_PAGE_TIMEOUT_S = 20
SELENIUM_ELEMENT_TIMEOUT_S = 12
SELENIUM_INTERACTION_TIMEOUT_S = 6
CURL_REQUEST_TIMEOUT_S = 15
CURL_IMPERSONATE_BROWSER = "chrome110"

NEXT_PAGE_SELECTORS = [
    "a.qa-PaginationPageLink-Next", "a.pagination-link[rel='next']",
    "a[aria-label*='Next Page' i]", "a[title*='Next Page' i]",
    "li.pagination-next > a", "a.pagination-next", "a.NextPageLink",
    "nav[aria-label*='pagination' i] li:last-child a[href]",
    ".page-next > a", "a.next"
]
REVIEW_BLOCK_CSS_SELECTOR_BS = "div.cppRH"
QUESTION_BLOCK_SELECTOR_BS = "div.reviewsList" # This is the container for one question and its reviews

TARGETED_POPUP_MAIN_SELECTOR = "div[class*='cultureQuestions-popup'] a.closeButton, div[class*='cultureQuestionsLoader'] a.closeButton"
POPUP_CLOSE_SELECTORS = [
    TARGETED_POPUP_MAIN_SELECTOR,
    "button[class*='modal__close' i]", "button[aria-label*='Dismiss' i]",
    "button[aria-label*='close' i]", "div[role='dialog'] button[class*='close']",
    "svg[data-testid*='close' i]", "i[class*='icon-close' i]",
    "button[class*='close' i]", "span[class*='close' i]"
]
INITIAL_PAGE_LOAD_SLEEP_S = random.uniform(0.8, 1.2)
AFTER_CATEGORY_CLICK_SLEEP_S = random.uniform(1.0, 1.5)
CURL_FETCH_DELAY_S = random.uniform(0.3, 0.6)

# --- Helper: Extract Section Name ---
def extract_section_name_from_url(href: Optional[str]) -> str:
    if not href: return "unknown_section"
    try:
        path_parts = urlparse(href).path.strip('/').split('/')
        if len(path_parts) >= 4 and path_parts[2] == 'reviews': return path_parts[3]
    except Exception: pass
    match = re.search(r'/reviews/(\w+)', href)
    return match.group(1) if match else "unknown_section"

# --- _parse_reviews_from_block (Used by Curl-CFFI part) ---
def _parse_reviews_from_block(
    review_container_soup: BeautifulSoup,
    start_date_filter: Optional[datetime],
    end_date_filter: Optional[datetime]
) -> List[Review]:
    reviews_found: List[Review] = []
    # Ensure we are looking for review blocks within the provided soup context
    review_blocks = review_container_soup.find_all('div', class_=REVIEW_BLOCK_CSS_SELECTOR_BS.split('.')[-1], recursive=True)
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

# --- Selenium Setup ---
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

# --- try_click ---
def try_click(driver: webdriver.Chrome, element, timeout: int = SELENIUM_INTERACTION_TIMEOUT_S):
    try:
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element))
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center', inline: 'nearest'});", element)
        time.sleep(0.2 + random.uniform(0.1, 0.2))
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

# --- attempt_to_close_popups (Polished targeted wait) ---
def attempt_to_close_popups(driver: webdriver.Chrome, thread_name: str):
    closed_any = False
    main_window = driver.current_window_handle
    initial_handles = set(driver.window_handles)
    popup_interaction_wait = WebDriverWait(driver, 2, poll_frequency=0.2)

    targeted_popup_buttons_found_and_visible = False
    try:
        targeted_popup_buttons = driver.find_elements(By.CSS_SELECTOR, TARGETED_POPUP_MAIN_SELECTOR)
        for btn in targeted_popup_buttons:
            if btn.is_displayed() and btn.is_enabled():
                targeted_popup_buttons_found_and_visible = True
                print(f"      [{thread_name}] Attempting to close TARGETED popup...")
                try:
                    driver.execute_script("arguments[0].click();", btn)
                    popup_interaction_wait.until(EC.staleness_of(btn))
                    print(f"      [{thread_name}] Targeted popup seems closed (became stale).")
                    closed_any = True; break
                except TimeoutException:
                    if not btn.is_displayed():
                        print(f"      [{thread_name}] Targeted popup seems closed (not displayed after timeout).")
                        closed_any = True; break
                    else: print(f"      [{thread_name}] Targeted popup still visible after click attempt and wait.")
                except StaleElementReferenceException: print(f"      [{thread_name}] Targeted popup closed (already stale)."); closed_any = True; break
                except Exception as e_close_target: print(f"      [{thread_name}] Error clicking targeted popup: {e_close_target}")
        if closed_any:
            time.sleep(0.5 + random.uniform(0.1, 0.2))
    except Exception: # Minor error finding, e.g. if page is still loading
        pass

    run_generic_check = True
    if targeted_popup_buttons_found_and_visible and closed_any:
        try:
            if not driver.find_elements(By.CSS_SELECTOR, TARGETED_POPUP_MAIN_SELECTOR) or \
               not any(b.is_displayed() for b in driver.find_elements(By.CSS_SELECTOR, TARGETED_POPUP_MAIN_SELECTOR)):
                run_generic_check = False
        except:
            run_generic_check = False

    if run_generic_check:
        for sel_idx, sel in enumerate(POPUP_CLOSE_SELECTORS):
            if sel == TARGETED_POPUP_MAIN_SELECTOR : continue
            try:
                popups = driver.find_elements(By.CSS_SELECTOR, sel)
                if not popups: continue
                for popup_btn in popups:
                    if popup_btn.is_displayed() and popup_btn.is_enabled():
                        print(f"      [{thread_name}] Attempting generic popup close ({sel_idx+1}/{len(POPUP_CLOSE_SELECTORS)}) with: {sel[:30]}...")
                        try:
                            driver.execute_script("arguments[0].click();", popup_btn)
                            time.sleep(0.3 + random.uniform(0.1, 0.2))
                            if not popup_btn.is_displayed(): print(f"      [{thread_name}] Generic popup seems closed."); break
                        except StaleElementReferenceException: print(f"      [{thread_name}] Generic popup closed (stale)."); break
                        except Exception as e_close: print(f"      [{thread_name}] Error clicking generic popup close ({sel[:30]}...): {e_close}")
                if popups and not popups[0].is_displayed(): break
            except Exception: pass

    final_handles = set(driver.window_handles)
    new_handles = final_handles - initial_handles
    if new_handles:
        for handle in new_handles:
            if handle != main_window:
                try:
                    print(f"      [{thread_name}] Closing unexpected new window/tab.")
                    driver.switch_to.window(handle); driver.close()
                except Exception as e_win_close: print(f"      [{thread_name}] Error closing new window: {e_win_close}")
        driver.switch_to.window(main_window)

    if closed_any and run_generic_check :
        print(f"      [{thread_name}] Generic popup/window closure attempted. Pausing briefly...")
        time.sleep(0.4 + random.uniform(0.1, 0.2))

# --- HYBRID Scraper ---
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
    total_reviews_in_category_count = 0 # Counter for reviews in this category

    try:
        category_driver = setup_selenium_driver()
        category_wait = WebDriverWait(category_driver, SELENIUM_ELEMENT_TIMEOUT_S)

        category_url_start = urljoin(company_base_url_str.rstrip('/') + "/", f"reviews/{category_name_arg}/")
        category_driver.get(category_url_start)
        time.sleep(INITIAL_PAGE_LOAD_SLEEP_S)
        attempt_to_close_popups(category_driver, thread_name)

        user_agent_hdr = category_driver.execute_script("return navigator.userAgent;")
        base_curl_headers = { 'User-Agent': user_agent_hdr, 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9', 'Accept-Language': 'en-US,en;q=0.9' }

        category_page_count = 0
        while category_page_count < MAX_CATEGORY_PAGES:
            category_page_count += 1
            current_category_page_url = category_driver.current_url
            print(f"  [{thread_name}] Selenium on Cat Page {category_page_count} for '{category_name_arg}' ({current_category_page_url})")

            try:
                category_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"{QUESTION_BLOCK_SELECTOR_BS}, {REVIEW_BLOCK_CSS_SELECTOR_BS}")))
            except TimeoutException:
                print(f"  [{thread_name}] Timeout waiting for content on Cat Page {category_page_count}.")
                if category_page_count == 1: print(f"  [{thread_name}] Initial page for '{category_name_arg}' seems empty or failed to load content.")
                break

            current_selenium_cookies = {c['name']: c['value'] for c in category_driver.get_cookies()}
            # It's crucial to get fresh page source AFTER popups are handled and content is expected to be stable
            time.sleep(0.5) # Small pause to ensure DOM settles after any popup closure or dynamic loading
            page_source_for_bs = category_driver.page_source
            soup_current_category_page = BeautifulSoup(page_source_for_bs, DEFAULT_HTML_PARSER)
            question_blocks_on_cat_page = soup_current_category_page.find_all('div', class_=QUESTION_BLOCK_SELECTOR_BS.split('.')[-1])

            if not question_blocks_on_cat_page:
                 if category_page_count == 1 and not soup_current_category_page.find('div', class_=REVIEW_BLOCK_CSS_SELECTOR_BS.split('.')[-1]):
                      print(f"  [{thread_name}] No question/review blocks on initial Cat Page {category_page_count}. Source length: {len(page_source_for_bs)}")
                 elif category_page_count > 1:
                      print(f"  [{thread_name}] No question blocks on Cat Page {category_page_count}, likely end of category pages.")
                 if category_page_count > 1 or not soup_current_category_page.find('div', class_=REVIEW_BLOCK_CSS_SELECTOR_BS.split('.')[-1]):
                     break # Break if no questions (and not first page with just reviews)

            for q_block_idx, q_block_soup in enumerate(question_blocks_on_cat_page):
                q_elem = q_block_soup.find('h2', class_='section-subtitle')
                if not q_elem: continue
                question_text = q_elem.get_text(strip=True)
                # print(f"    [{thread_name}] Processing Q: '{question_text[:60]}...'") # Can be verbose

                all_reviews_for_this_q_session: List[Review] = []
                current_q_reviews_html_segment_bs = q_block_soup # Initial segment is from Selenium page
                current_q_reviews_source_url_for_curl = current_category_page_url # Base URL for relative links

                with CurlCffiSession(impersonate=CURL_IMPERSONATE_BROWSER, trust_env=False) as curl_q_session:
                    curl_q_session.cookies.update(current_selenium_cookies)
                    q_review_page_num_curl = 0
                    while q_review_page_num_curl < MAX_REVIEW_PAGES_PER_QUESTION:
                        q_review_page_num_curl += 1
                        # print(f"      [{thread_name}] Q-Review Loop: Page {q_review_page_num_curl} for Q: '{question_text[:30]}...'")

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
                                total_reviews_in_category_count +=1 # Increment category total

                        print(f"      [{thread_name}] Q-Review Page {q_review_page_num_curl} (Q: '{question_text[:30]}...'): Parsed {len(reviews_from_current_segment)} items, {newly_added_this_q_sub_page_count} new. Total for this Q now: {len(all_reviews_for_this_q_session)}.")

                        if newly_added_this_q_sub_page_count == 0 and q_review_page_num_curl > 1 and reviews_from_current_segment:
                             print(f"        [{thread_name}] Note: Found {len(reviews_from_current_segment)} reviews on Q-page {q_review_page_num_curl}, but all were duplicates already seen in this category scan.")
                        elif not reviews_from_current_segment and q_review_page_num_curl > 1 : # No reviews found on a subsequent page
                             print(f"        [{thread_name}] Warning: Found 0 review items on Q-page {q_review_page_num_curl} (Q: '{question_text[:30]}...'). URL: {current_q_reviews_source_url_for_curl}. May indicate end or issue.")


                        next_q_review_page_href = None
                        # Scope pagination search to within the current question's review block/segment
                        pagination_scope_for_q_bs = current_q_reviews_html_segment_bs.find(['nav', 'ul', 'div'],
                            class_=lambda x: x and any(p in x.lower() for p in ['pagination', 'pager', 'page-links', 'qa-Pagination', 'cp-Pagination']),
                            recursive=True
                        ) or current_q_reviews_html_segment_bs

                        for sel_idx_q, sel_q in enumerate(NEXT_PAGE_SELECTORS):
                            buttons_bs = pagination_scope_for_q_bs.select(sel_q)
                            for btn_tag_bs in buttons_bs:
                                href = btn_tag_bs.get('href')
                                aria_label_bs = btn_tag_bs.get("aria-label", "").lower()
                                rel_val_bs = btn_tag_bs.get("rel"); rel_str_bs = " ".join(rel_val_bs).lower() if isinstance(rel_val_bs, list) else (rel_val_bs or "").lower()
                                text_bs = btn_tag_bs.get_text(strip=True).lower()
                                combined_bs = f"{aria_label_bs} {rel_str_bs} {text_bs}"
                                is_prev_bs = "prev" in combined_bs or "previous" in combined_bs
                                class_bs_list = btn_tag_bs.get('class', [])
                                is_disabled_bs = any(c in class_bs_list for c in ['disabled', 'inactive']) or btn_tag_bs.has_attr('disabled')

                                if is_prev_bs or is_disabled_bs: continue
                                if href and href != "#" and not href.startswith("javascript:"):
                                    next_q_review_page_href = urljoin(current_q_reviews_source_url_for_curl, href)
                                    # print(f"        [{thread_name}] Q-Review Nav: Found next link with selector {sel_idx_q+1} ('{sel_q}'): {next_q_review_page_href}")
                                    break
                            if next_q_review_page_href: break

                        if not next_q_review_page_href:
                            # print(f"        [{thread_name}] Q-Review Nav: No 'next page' link found for Q: '{question_text[:50]}...' after {q_review_page_num_curl} pages. Reviews for this Q: {len(all_reviews_for_this_q_session)}.")
                            break # No more pages for this question

                        try:
                            time.sleep(CURL_FETCH_DELAY_S)
                            q_review_fetch_headers = base_curl_headers.copy()
                            q_review_fetch_headers['Referer'] = current_q_reviews_source_url_for_curl # Important
                            # print(f"        [{thread_name}] Q-Review Nav: Fetching Q-review page {q_review_page_num_curl + 1} from: {next_q_review_page_href}")
                            response_q_review_page = curl_q_session.get(next_q_review_page_href, headers=q_review_fetch_headers, timeout=CURL_REQUEST_TIMEOUT_S, allow_redirects=True)
                            response_q_review_page.raise_for_status()
                            
                            current_q_reviews_html_segment_bs = BeautifulSoup(response_q_review_page.text, DEFAULT_HTML_PARSER)
                            current_q_reviews_source_url_for_curl = str(response_q_review_page.url) # Update to actual URL after redirects

                            # --- Sanity checks for the fetched page content ---
                            if not current_q_reviews_html_segment_bs.find('div', class_=REVIEW_BLOCK_CSS_SELECTOR_BS.split('.')[-1]):
                                print(f"        [{thread_name}] Q-Review Nav CRITICAL: Fetched page {current_q_reviews_source_url_for_curl} has NO review blocks. Stopping for this question.")
                                # For debugging, you might want to save this HTML:
                                # with open(f"debug_page_no_reviews_{company_slug}_{category_name_arg}_q{q_block_idx}_curlpage{q_review_page_num_curl}.html", "w", encoding="utf-8") as f_debug:
                                #    f_debug.write(current_q_reviews_html_segment_bs.prettify())
                                break

                            # Check if it looks like a full category page (multiple distinct question blocks)
                            if len(current_q_reviews_html_segment_bs.find_all('div', class_=QUESTION_BLOCK_SELECTOR_BS.split('.')[-1])) > 1:
                                print(f"        [{thread_name}] Q-Review Nav WARNING: Curl-fetched page {current_q_reviews_source_url_for_curl} looks like a full category page (multiple Q-blocks). Stopping Q pagination.")
                                break
                            
                            # Check if the question context changed
                            q_elem_on_new_page = current_q_reviews_html_segment_bs.find('h2', class_='section-subtitle')
                            new_page_question_text = q_elem_on_new_page.get_text(strip=True) if q_elem_on_new_page else None
                            # This check is tricky because the page might only contain reviews and no new H2 for the *same* question.
                            # Only consider it a different question if a *different* h2.section-subtitle is found.
                            # If q_elem_on_new_page is None, it might just be a continuation of reviews.
                            if q_elem_on_new_page and new_page_question_text and new_page_question_text != question_text:
                                print(f"        [{thread_name}] Q-Review Nav WARNING: Fetched page {current_q_reviews_source_url_for_curl} has different Q title ('{new_page_question_text[:30]}...') than current ('{question_text[:30]}...'). Stopping Q pagination.")
                                break

                        except RequestsError as e_q_rev_req:
                            status_code_msg = f" (Status: {e_q_rev_req.response.status_code})" if hasattr(e_q_rev_req, 'response') and e_q_rev_req.response else ""
                            print(f"        [{thread_name}] Curl-CFFI Error{status_code_msg} fetching Q-REVIEW page {next_q_review_page_href}: {e_q_rev_req}")
                            break
                        except Exception as e_gen_curl:
                            print(f"        [{thread_name}] Generic Error during Curl Q-REVIEW fetch/parse from {next_q_review_page_href}: {e_gen_curl}")
                            traceback.print_exc(file=sys.stdout) # Print traceback for generic errors
                            break
                
                if all_reviews_for_this_q_session:
                    print(f"    [{thread_name}] Finished Q: '{question_text[:60]}...'. Collected {len(all_reviews_for_this_q_session)} reviews for it in this session.")
                    all_reviews_for_this_q_session.sort(key=lambda r: r.date, reverse=True)
                    existing_q_obj = next((q for q in collected_questions_for_this_category if q.question_text == question_text), None)
                    if existing_q_obj:
                        new_merged_count = 0
                        for r_new in all_reviews_for_this_q_session:
                            if not any(er.text == r_new.text and er.date == r_new.date for er in existing_q_obj.review_section.reviews):
                                existing_q_obj.review_section.reviews.append(r_new); new_merged_count+=1
                        if new_merged_count > 0:
                            existing_q_obj.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
                            # print(f"      [{thread_name}] Merged {new_merged_count} new reviews into existing Q. Total now: {len(existing_q_obj.review_section.reviews)}")
                    else:
                        review_section = ReviewSection(section_name=category_name_arg, reviews=all_reviews_for_this_q_session)
                        question_obj = Question(question_text=question_text, review_section=review_section)
                        collected_questions_for_this_category.append(question_obj)
                # else:
                    # print(f"    [{thread_name}] No reviews collected for Q: '{question_text[:60]}...' in this session.")


            # --- Category Pagination (Selenium) ---
            next_category_page_button_sel_elem = None
            # Re-fetch page source for category navigation to be safe, though Selenium holds current state
            # soup_for_cat_nav = BeautifulSoup(category_driver.page_source, DEFAULT_HTML_PARSER)
            # Use already fetched soup_current_category_page for this to avoid another page_source call
            cat_page_nav_scope_bs = soup_current_category_page.find('nav', attrs={'aria-label': lambda x: x and 'pagination' in x.lower()}) or \
                                 soup_current_category_page.find('ul', class_=lambda x: x and 'pagination' in x.lower()) or \
                                 soup_current_category_page # Fallback to whole page if specific nav not found

            href_bs_for_retry = None
            sel_css_for_retry = None

            for sel_css in NEXT_PAGE_SELECTORS:
                potential_bs_btns = cat_page_nav_scope_bs.select(sel_css)
                for btn_s_tag in potential_bs_btns:
                    # CRUCIAL: Ensure this "next" button is NOT inside a question's review block
                    if btn_s_tag.find_parent('div', class_=QUESTION_BLOCK_SELECTOR_BS.split('.')[-1]):
                        continue # This is pagination for a question, not for the category

                    aria_label_bs = btn_s_tag.get("aria-label", "").lower()
                    rel_val_bs = btn_s_tag.get("rel"); rel_bs = " ".join(rel_val_bs).lower() if isinstance(rel_val_bs, list) else (rel_val_bs or "").lower()
                    text_bs = btn_s_tag.get_text(strip=True).lower()
                    combined_bs = f"{aria_label_bs} {rel_bs} {text_bs}"; is_prev_bs = "prev" in combined_bs or "previous" in combined_bs
                    class_bs_list = btn_s_tag.get('class', []); is_disabled_bs = any(c in class_bs_list for c in ['disabled', 'inactive']) or btn_s_tag.has_attr('disabled')
                    href_bs = btn_s_tag.get('href')

                    if not is_prev_bs and not is_disabled_bs and href_bs and href_bs != '#' and not href_bs.startswith("javascript:"):
                        href_bs_for_retry = href_bs
                        sel_css_for_retry = sel_css
                        try:
                            # Try to find the corresponding Selenium element more reliably
                            # Using XPath to ensure it's not within a question block
                            xpath_selector = f".//{btn_s_tag.name}[@href='{href_bs}' and not(ancestor::div[contains(@class, '{QUESTION_BLOCK_SELECTOR_BS.split('.')[-1]}')])]"
                            # Simpler: find by CSS then double check parentage with Selenium element if needed
                            selenium_potential_cat_next_btns = category_driver.find_elements(By.CSS_SELECTOR, sel_css)
                            for sel_btn_elem in selenium_potential_cat_next_btns:
                                if not sel_btn_elem.is_displayed() or not sel_btn_elem.is_enabled(): continue
                                try:
                                    # Check if this Selenium element is inside a question block
                                    sel_btn_elem.find_element(By.XPATH, f"./ancestor::div[contains(@class, '{QUESTION_BLOCK_SELECTOR_BS.split('.')[-1]}')]")
                                    continue # Skip if it's part of a question's pagination
                                except NoSuchElementException:
                                    # This is a candidate, verify href if possible
                                    sel_href = sel_btn_elem.get_attribute('href')
                                    if sel_href and urljoin(current_category_page_url, href_bs) == urljoin(current_category_page_url, sel_href): # Compare absolute URLs
                                        next_category_page_button_sel_elem = sel_btn_elem; break
                                    elif not next_category_page_button_sel_elem: # Fallback if hrefs don't match but it's a valid next button otherwise
                                        next_category_page_button_sel_elem = sel_btn_elem # Tentative assignment
                            if next_category_page_button_sel_elem and next_category_page_button_sel_elem.get_attribute('href') and \
                               urljoin(current_category_page_url, href_bs) == urljoin(current_category_page_url, next_category_page_button_sel_elem.get_attribute('href')): # Confirm best match
                                break
                        except (NoSuchElementException, StaleElementReferenceException): continue
                if next_category_page_button_sel_elem and next_category_page_button_sel_elem.get_attribute('href') and \
                   urljoin(current_category_page_url, href_bs_for_retry) == urljoin(current_category_page_url, next_category_page_button_sel_elem.get_attribute('href')): # Confirm best match from outer loop
                    break

            if not next_category_page_button_sel_elem:
                print(f"  [{thread_name}] No 'Next Category Page' button found after Cat Page {category_page_count}. Ending category scan.")
                break

            clicked_successfully = try_click(category_driver, next_category_page_button_sel_elem)
            if not clicked_successfully:
                print(f"    [{thread_name}] Initial click failed for Next Cat Page. Trying popups then retry.")
                attempt_to_close_popups(category_driver, thread_name)
                time.sleep(0.5) # Wait for DOM to potentially update after popup closure
                try:
                    final_attempt_button = None
                    # Re-locate the button, it might have changed or been obscured
                    if href_bs_for_retry: # Best way to re-find is by the href that BS4 found
                        expected_full_href = urljoin(current_category_page_url, href_bs_for_retry)
                        all_links_after_popup = WebDriverWait(category_driver, 5).until(
                            EC.presence_of_all_elements_located((By.XPATH, f"//a[@href='{href_bs_for_retry}' or @href='{expected_full_href}']"))
                        )
                        for link in all_links_after_popup:
                            if link.is_displayed() and link.is_enabled():
                                try: link.find_element(By.XPATH, f"./ancestor::div[contains(@class, '{QUESTION_BLOCK_SELECTOR_BS.split('.')[-1]}')]"); continue
                                except NoSuchElementException: final_attempt_button = link; break
                    
                    if final_attempt_button:
                        clicked_successfully = try_click(category_driver, final_attempt_button)
                    elif sel_css_for_retry: # Fallback: use the CSS selector that BS4 used
                        buttons_after_popup = category_driver.find_elements(By.CSS_SELECTOR, sel_css_for_retry)
                        for btn_retry in buttons_after_popup:
                             if btn_retry.is_displayed() and btn_retry.is_enabled():
                                 try: btn_retry.find_element(By.XPATH, f"./ancestor::div[contains(@class, '{QUESTION_BLOCK_SELECTOR_BS.split('.')[-1]}')]"); continue
                                 except NoSuchElementException: final_attempt_button = btn_retry; break
                        if final_attempt_button:
                             clicked_successfully = try_click(category_driver, final_attempt_button)
                except TimeoutException: print(f"    [{thread_name}] Timeout re-finding Next Cat Page button after popup attempt.")
                except Exception as e_refind: print(f"    [{thread_name}] Error during re-find of Next Cat page button: {e_refind}")

            if clicked_successfully:
                time.sleep(AFTER_CATEGORY_CLICK_SLEEP_S)
                attempt_to_close_popups(category_driver, thread_name) # Close popups that might appear AFTER page load
            else:
                print(f"  [{thread_name}] Failed to click 'Next Category Page' even after popup attempt and retry. Stopping category scan.")
                break
    except Exception as e_cat_main:
        print(f"  [{thread_name}] MAJOR ERROR in category '{category_name_arg}': {e_cat_main}"); traceback.print_exc()
    finally:
        if category_driver: category_driver.quit()

    category_scrape_duration = time.perf_counter() - category_scrape_start_time
    num_questions_found = len(collected_questions_for_this_category)
    # total_reviews_in_category_count is already updated globally
    print(f"  [{thread_name}] Finished category '{category_name_arg}' in {category_scrape_duration:.2f}s. Found {num_questions_found} Qs, {total_reviews_in_category_count} total reviews in this category.")
    return category_name_arg, collected_questions_for_this_category

# --- Main Orchestrator ---
def scrape_comparably_sync(
    company_base_url_str: str, company_slug: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating HYBRID (v{app.version}) scrape for: {company_slug}")
    orchestration_start_time = time.perf_counter()
    all_questions_for_company: List[Question] = []
    company_name_from_slug = company_slug.replace('-', ' ').title()
    company_details_overall: Dict[str, Any] = {
        "company_name": company_name_from_slug,
        "comparably_url": company_base_url_str,
        "status_note": "Company name derived from slug."
    }
    max_concurrent_categories = min(len(REVIEW_CATEGORIES), 4)
    print(f"  [{company_slug}] Starting HYBRID parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")
    category_processing_start_time = time.perf_counter()
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories, thread_name_prefix="HybridPool") as executor:
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
                    all_questions_for_company.extend(questions_from_category)
            except Exception as e_future_exc:
                print(f"  [{company_slug}] HYBRID Cat task for '{original_category_name_processed}' FAILED: {e_future_exc}")
                traceback.print_exc()
    category_processing_duration = time.perf_counter() - category_processing_start_time
    print(f"  [{company_slug}] All category threads (Hybrid) completed in {category_processing_duration:.2f}s.")
    total_duration = time.perf_counter() - orchestration_start_time
    num_total_questions = len(all_questions_for_company)
    num_total_reviews = sum(len(q.review_section.reviews) for q in all_questions_for_company)
    print(f"\nFinished ALL HYBRID scrapes for {company_slug} in {total_duration:.2f}s. Total Qs: {num_total_questions}, Total Reviews Scraped: {num_total_reviews}")

    if company_details_overall.get("company_name", "").lower() in REVIEW_CATEGORIES: # Unlikely, but a safeguard
        company_details_overall["company_name"] = company_name_from_slug
        company_details_overall["status_note"] = "Company name reset to slug-derived."
    return {
        "status": "success" if num_total_reviews > 0 else ("partial_success_no_reviews" if num_total_questions > 0 else "no_data_found"),
        "data": { "company_info": company_details_overall, "reviews": [q.model_dump(mode='json') for q in all_questions_for_company] },
        "summary": {"total_questions": num_total_questions, "total_reviews": num_total_reviews}
    }

# --- FastAPI Endpoint ---
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
    print(f"API request: {len(urls)} URLs, Hybrid Optimized (v{app.version}).")
    for url_obj in urls:
        url_str = str(url_obj)
        try:
            parsed_url = urlparse(url_str); path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
            if not (parsed_url.scheme and parsed_url.netloc and len(path_segments) >= 2 and path_segments[0] == "companies"):
                raise ValueError("URL format error: Must be like 'https://www.comparably.com/companies/company-slug'")
            company_slug = path_segments[1]; company_base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/companies/{company_slug}"
            valid_scrape_params.append({'original_url': url_str, 'base_url': company_base_url, 'slug': company_slug})
        except Exception as e_slug:
            print(f"Error parsing URL '{url_str}': {e_slug}")
            results[url_str] = {"status": "error", "message": f"Invalid URL format: {url_str}. Error: {e_slug}"}

    tasks = []
    if valid_scrape_params:
        for params in valid_scrape_params:
            # Using asyncio.to_thread to run the synchronous blocking function in a separate thread
            tasks.append(asyncio.to_thread(scrape_comparably_sync, params['base_url'], params['slug'], start_date_filter, end_date_filter))

    scraped_results_or_exceptions = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []

    task_idx = 0
    for params in valid_scrape_params:
        original_url_str = params['original_url']
        if original_url_str in results: continue # Already processed as an error
        if task_idx < len(scraped_results_or_exceptions):
            result_or_exc = scraped_results_or_exceptions[task_idx]
            if isinstance(result_or_exc, Exception):
                print(f"Task for {original_url_str} (Hybrid v{app.version}) EXCEPTION: {result_or_exc}")
                tb_str = "".join(traceback.format_exception(None, result_or_exc, result_or_exc.__traceback__))
                print(f"FULL TRACEBACK for {original_url_str} (Hybrid v{app.version}):\n{tb_str}")
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed: {type(result_or_exc).__name__}. Check server logs for details."}
            elif isinstance(result_or_exc, dict): results[original_url_str] = result_or_exc
            else: results[original_url_str] = {"status": "error", "message": "Unexpected internal result type from scraping task."}
            task_idx +=1
        else: # Should not happen if gather returns a result/exception for every task
            results[original_url_str] = {"status": "error", "message": "Scraping task result missing (internal error)."}

    print(f"Finished API request processing (Hybrid Optimized v{app.version}).")
    return results

# For local testing if needed:
# if __name__ == "__main__":
#     async def main_test():
#         test_url = "https://www.comparably.com/companies/google" # Replace with a valid test URL
#         test_request = ScrapeRequest(urls=[HttpUrl(test_url)])
#         # test_request = ScrapeRequest(urls=[HttpUrl(test_url)], start_date_str="2023-01-01", end_date_str="2023-12-31")
#         results = await scrape_companies(test_request)
#         print(json.dumps(results, indent=2))

#     asyncio.run(main_test())
