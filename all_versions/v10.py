
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
    title="Comparably Scraper API - Selenium Pure Navigation (Refined)",
    description="Pure Selenium for all page navigation with improved AJAX handling and interception fixes.",
    version="2.2.1" # Updated version
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
SELENIUM_PAGE_TIMEOUT_S = 25
SELENIUM_ELEMENT_TIMEOUT_S = 15
SELENIUM_INTERACTION_TIMEOUT_S = 10 # For clicks and waiting for results of clicks
SELENIUM_AJAX_POLL_INTERVAL_S = 0.5 # How often to check for AJAX updates

NEXT_PAGE_SELECTORS = [
    "a.qa-PaginationPageLink-Next", "a.pagination-link[rel='next']",
    "a[aria-label*='Next Page' i]", "a[title*='Next Page' i]",
    "li.pagination-next > a", "a.pagination-next", "a.NextPageLink",
    "nav[aria-label*='pagination' i] li:last-child a[href]",
    ".page-next > a", "a.next"
]
REVIEW_BLOCK_CSS_SELECTOR = "div.cppRH"
QUESTION_BLOCK_SELECTOR = "div.reviewsList"
# Generic selectors for closing popups/modals
POPUP_CLOSE_SELECTORS = [
    "button[aria-label*='close' i]", "button[class*='close' i]",
    "div[class*='modal'] button[data-dismiss*='modal']",
    "div[class*='popup'] span[class*='close' i]",
    "i[class*='close' i]", "span[class*='close' i]",
]


# --- Helper: Extract Section Name (Unchanged) ---
def extract_section_name_from_url(href: Optional[str]) -> str:
    if not href: return "unknown_section"
    try:
        path_parts = urlparse(href).path.strip('/').split('/')
        if len(path_parts) >= 4 and path_parts[2] == 'reviews': return path_parts[3]
    except Exception: pass
    match = re.search(r'/reviews/(\w+)', href)
    return match.group(1) if match else "unknown_section"

# --- _parse_reviews_from_block (Unchanged) ---
def _parse_reviews_from_block(
    review_container_soup: BeautifulSoup,
    start_date_filter: Optional[datetime],
    end_date_filter: Optional[datetime]
) -> List[Review]:
    reviews_found: List[Review] = []
    review_blocks = review_container_soup.find_all('div', class_='cppRH')
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

# --- Selenium Setup (Unchanged) ---
def setup_selenium_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-extensions")
    options.add_argument("window-size=1920,1080")
    user_agent_str = ua.random if ua else "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent_str}')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    # Suppress DevTools listening message more effectively if possible
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument('--log-level=3') # Try to reduce console noise
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

def try_click(driver: webdriver.Chrome, element, timeout: int = SELENIUM_INTERACTION_TIMEOUT_S):
    """Attempts to click an element, handling common issues."""
    try:
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(element))
        # Scroll into view more robustly
        driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center', inline: 'nearest'});", element)
        time.sleep(0.3 + random.uniform(0.1, 0.3)) # Short pause for scroll
        element.click()
        return True
    except ElementClickInterceptedException:
        print(f"    [try_click] Intercepted. Trying JS click.")
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as e_js:
            print(f"    [try_click] JS click failed: {e_js}")
            return False
    except (TimeoutException, ElementNotInteractableException, StaleElementReferenceException) as e_click:
        print(f"    [try_click] Click failed: {type(e_click).__name__} - {e_click}")
        return False
    except Exception as e_other_click:
        print(f"    [try_click] Unexpected click error: {type(e_other_click).__name__} - {e_other_click}")
        return False


def attempt_to_close_popups(driver: webdriver.Chrome, thread_name: str):
    closed_any = False
    for sel in POPUP_CLOSE_SELECTORS:
        try:
            popups = driver.find_elements(By.CSS_SELECTOR, sel)
            for popup_btn in popups:
                if popup_btn.is_displayed() and popup_btn.is_enabled():
                    print(f"      [{thread_name}] Attempting to close potential popup with selector: {sel}")
                    try:
                        driver.execute_script("arguments[0].click();", popup_btn) # JS click for popups
                        time.sleep(0.5 + random.uniform(0.1, 0.3)) # Give it a moment to disappear
                        closed_any = True
                        print(f"      [{thread_name}] Clicked a close button.")
                        # Optional: check if it actually closed, if possible
                        if not popup_btn.is_displayed(): # Check if it disappeared
                           break 
                    except Exception as e_close:
                        print(f"      [{thread_name}] Error clicking popup close button ({sel}): {e_close}")
            if closed_any and not popup_btn.is_displayed(): # if we closed something and it's gone, maybe enough
                break
        except Exception: # Broad catch, as finding popups is best-effort
            pass
    if closed_any:
        print(f"      [{thread_name}] Attempted to close popups. Pausing briefly...")
        time.sleep(1.0 + random.uniform(0.2, 0.5)) # Pause after attempting to close


# --- Scraper: Pure Selenium for All Navigation (Refined) ---
def _scrape_category_reviews_selenium_pure(
    company_base_url_str: str,
    category_name_arg: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    thread_name = f"SelPure-{category_name_arg}-{company_slug[:10]}"
    category_scrape_start_time = time.perf_counter()
    print(f"  [{thread_name}] Started for category: {category_name_arg}")

    collected_questions_for_this_category: List[Question] = []
    processed_reviews_keys_globally_for_category = set()

    category_driver = None
    try:
        category_driver = setup_selenium_driver()
        category_wait = WebDriverWait(category_driver, SELENIUM_ELEMENT_TIMEOUT_S)
        interaction_wait = WebDriverWait(category_driver, SELENIUM_INTERACTION_TIMEOUT_S)

        category_url_start = urljoin(company_base_url_str.rstrip('/') + "/", f"reviews/{category_name_arg}/")
        print(f"  [{thread_name}] Selenium navigating to initial Cat Page: {category_url_start}")
        category_driver.get(category_url_start)
        time.sleep(random.uniform(1.2, 1.8)) # Initial settle, allow popups to appear
        attempt_to_close_popups(category_driver, thread_name)


        category_page_count = 0
        while category_page_count < MAX_CATEGORY_PAGES:
            category_page_count += 1
            current_category_page_url = category_driver.current_url
            print(f"  [{thread_name}] Selenium on Cat Page {category_page_count} (URL: {current_category_page_url})")

            try:
                category_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, QUESTION_BLOCK_SELECTOR)))
            except TimeoutException:
                print(f"  [{thread_name}] Timeout waiting for question content on Cat Page {category_page_count}.")
                if category_page_count == 1: print(f"  [{thread_name}] Initial category page for '{category_name_arg}' appears empty.")
                break

            # Get Selenium WebElements for each question block ONCE per category page
            try:
                live_q_block_elements = category_driver.find_elements(By.CSS_SELECTOR, QUESTION_BLOCK_SELECTOR)
            except Exception as e_find_q_blocks:
                print(f"  [{thread_name}] Error finding live question blocks: {e_find_q_blocks}. Skipping page.")
                break
            
            if not live_q_block_elements:
                msg = "No question blocks found on initial category page." if category_page_count == 1 else "No question blocks found, likely end of category pages."
                print(f"  [{thread_name}] {msg}")
                break
            print(f"    [{thread_name}] Found {len(live_q_block_elements)} live question blocks on Cat Page {category_page_count}.")

            # Iterate through the LIVE Selenium question block elements
            for q_block_idx, current_live_q_block_element in enumerate(live_q_block_elements):
                try:
                    # Get the HTML of this specific live question block for BeautifulSoup parsing
                    q_block_html = current_live_q_block_element.get_attribute('outerHTML')
                    q_block_initial_bs_soup = BeautifulSoup(q_block_html, DEFAULT_HTML_PARSER)

                    q_elem_bs = q_block_initial_bs_soup.find('h2', class_='section-subtitle')
                    if not q_elem_bs:
                        print(f"    [{thread_name}] Q_block_idx {q_block_idx} has no title, skipping.")
                        continue
                    question_text = q_elem_bs.get_text(strip=True)
                    print(f"    [{thread_name}] Processing Q_idx {q_block_idx}: '{question_text[:60]}...'")

                    all_reviews_for_this_q_on_this_cat_page: List[Review] = [] # Reviews for this Q specifically from this cat page visit
                    
                    current_q_block_bs_for_parsing = q_block_initial_bs_soup # Initial state for parsing

                    q_review_page_num_for_this_q = 0
                    while q_review_page_num_for_this_q < MAX_REVIEW_PAGES_PER_QUESTION:
                        q_review_page_num_for_this_q += 1
                        
                        # Count reviews BEFORE potential click, only within the current Q-block's BS representation
                        reviews_in_current_q_block_segment_bs = current_q_block_bs_for_parsing.find_all('div', class_=REVIEW_BLOCK_CSS_SELECTOR.split('.')[-1])
                        num_reviews_before_click = len(reviews_in_current_q_block_segment_bs)

                        reviews_from_current_segment = _parse_reviews_from_block(
                            current_q_block_bs_for_parsing, start_date_filter, end_date_filter
                        )
                        
                        newly_added_this_q_sub_page_count = 0
                        for r_parsed in reviews_from_current_segment:
                            r_key = (hash(question_text), hash(r_parsed.text), r_parsed.date)
                            if r_key not in processed_reviews_keys_globally_for_category:
                                all_reviews_for_this_q_on_this_cat_page.append(r_parsed)
                                processed_reviews_keys_globally_for_category.add(r_key)
                                newly_added_this_q_sub_page_count +=1
                        
                        if newly_added_this_q_sub_page_count > 0:
                            print(f"        [{thread_name}] Q_idx {q_block_idx} - Added {newly_added_this_q_sub_page_count} unique reviews (Q-SubPage {q_review_page_num_for_this_q}). Total for Q so far on this cat page: {len(all_reviews_for_this_q_on_this_cat_page)}")
                        elif q_review_page_num_for_this_q > 1:
                            print(f"        [{thread_name}] Q_idx {q_block_idx} - No new reviews parsed on Q-SubPage {q_review_page_num_for_this_q}.")

                        # Find "Next Page" for Q-Reviews using Selenium, scoped to the CURRENT live question block
                        q_next_page_selenium_element = None
                        try:
                            # We need to use current_live_q_block_element (Selenium WebElement) for finding child elements
                            for sel in NEXT_PAGE_SELECTORS:
                                potential_buttons = current_live_q_block_element.find_elements(By.CSS_SELECTOR, sel)
                                for btn_elem in potential_buttons:
                                    if not btn_elem.is_displayed() or not btn_elem.is_enabled(): continue
                                    href_val = btn_elem.get_attribute('href')
                                    if not href_val or href_val == '#': continue

                                    aria_label = (btn_elem.get_attribute("aria-label") or "").lower()
                                    rel_attr = (btn_elem.get_attribute("rel") or "").lower()
                                    text_content = btn_elem.text.lower()
                                    combined_test_str = f"{aria_label} {rel_attr} {text_content}"
                                    is_prev = "prev" in combined_test_str
                                    
                                    if not is_prev:
                                        q_next_page_selenium_element = btn_elem
                                        break
                                if q_next_page_selenium_element: break
                        except StaleElementReferenceException:
                            print(f"        [{thread_name}] Q_idx {q_block_idx} - Stale element finding Q-review next. Re-finding q_block.")
                            # Re-fetch the live question block elements for the whole page
                            live_q_block_elements_refresh = category_driver.find_elements(By.CSS_SELECTOR, QUESTION_BLOCK_SELECTOR)
                            if q_block_idx < len(live_q_block_elements_refresh):
                                current_live_q_block_element = live_q_block_elements_refresh[q_block_idx] # Update reference
                                # Retry finding the button once
                                for sel in NEXT_PAGE_SELECTORS: # Duplicated logic, could be function
                                    potential_buttons = current_live_q_block_element.find_elements(By.CSS_SELECTOR, sel)
                                    for btn_elem in potential_buttons:
                                        if not btn_elem.is_displayed() or not btn_elem.is_enabled(): continue
                                        href_val = btn_elem.get_attribute('href')
                                        if not href_val or href_val == '#': continue
                                        aria_label = (btn_elem.get_attribute("aria-label") or "").lower()
                                        rel_attr = (btn_elem.get_attribute("rel") or "").lower()
                                        text_content = btn_elem.text.lower()
                                        combined_test_str = f"{aria_label} {rel_attr} {text_content}"
                                        is_prev = "prev" in combined_test_str
                                        if not is_prev: q_next_page_selenium_element = btn_elem; break
                                    if q_next_page_selenium_element: break
                            else:
                                print(f"        [{thread_name}] Q_idx {q_block_idx} - Failed to re-find q_block after stale. Stopping Q-pagination.")
                                break # Break from Q-review pagination loop

                        except NoSuchElementException: pass # No button found, normal for last page

                        if not q_next_page_selenium_element:
                            # Only log if it's not the first page, or if first page had no reviews
                            if q_review_page_num_for_this_q > 1 or not reviews_from_current_segment:
                                print(f"        [{thread_name}] Q_idx {q_block_idx} - No further Q-review pages found for Q: '{question_text[:30]}...'")
                            break # No more pages for this specific question

                        # Click the Q-review next page button
                        print(f"        [{thread_name}] Q_idx {q_block_idx} - Attempting to click Q-review 'Next' (for Q-SubPage {q_review_page_num_for_this_q + 1})")
                        if not try_click(category_driver, q_next_page_selenium_element):
                            print(f"        [{thread_name}] Q_idx {q_block_idx} - Failed to click Q-review 'Next'. Stopping Q-pagination.")
                            break
                        
                        # Wait for AJAX to load by checking for review count change within THIS Q-block
                        try:
                            interaction_wait.until(
                                lambda driver: len(
                                    BeautifulSoup(
                                        driver.find_elements(By.CSS_SELECTOR, QUESTION_BLOCK_SELECTOR)[q_block_idx].get_attribute('outerHTML'),
                                        DEFAULT_HTML_PARSER
                                    ).find_all('div', class_=REVIEW_BLOCK_CSS_SELECTOR.split('.')[-1])
                                ) > num_reviews_before_click or \
                                # Or if the button we just clicked is gone/disabled (might be last page)
                                (not q_next_page_selenium_element.is_displayed() or not q_next_page_selenium_element.is_enabled())
                            )
                            print(f"        [{thread_name}] Q_idx {q_block_idx} - AJAX update detected or Q-next button changed state.")
                        except StaleElementReferenceException:
                             print(f"        [{thread_name}] Q_idx {q_block_idx} - Q-next button became stale during AJAX wait. Assuming update happened.")
                        except TimeoutException:
                            print(f"        [{thread_name}] Q_idx {q_block_idx} - Timeout waiting for AJAX update (review count change) for Q-review. Might not have loaded more.")
                            # Continue to re-parse anyway, it might have loaded but not enough to exceed count, or button disabled
                        except IndexError: # If q_block_idx is out of bounds after DOM change
                            print(f"        [{thread_name}] Q_idx {q_block_idx} - Q-block index error during AJAX wait. DOM changed significantly. Stopping Q-pagination.")
                            break

                        time.sleep(0.3 + random.uniform(0.1, 0.3)) # Small pause after AJAX wait

                        # After click and wait, get fresh HTML of THIS question's block for the next iteration's parsing
                        try:
                            live_q_block_elements_after_click = category_driver.find_elements(By.CSS_SELECTOR, QUESTION_BLOCK_SELECTOR)
                            if q_block_idx < len(live_q_block_elements_after_click):
                                current_live_q_block_element = live_q_block_elements_after_click[q_block_idx] # Update reference to live element
                                current_q_block_bs_for_parsing = BeautifulSoup(current_live_q_block_element.get_attribute('outerHTML'), DEFAULT_HTML_PARSER)
                            else:
                                print(f"        [{thread_name}] Q_idx {q_block_idx} - CRITICAL: Could not re-find Q-block (idx {q_block_idx}) in DOM after Q-review click. Stopping Q-pagination for this question.")
                                break
                        except Exception as e_refresh_q_block:
                            print(f"        [{thread_name}] Q_idx {q_block_idx} - Error refreshing Q-block for parsing after click: {e_refresh_q_block}. Stopping Q-pagination.")
                            break
                    # End of Q-review sub-page loop
                
                except StaleElementReferenceException:
                    print(f"    [{thread_name}] Q_idx {q_block_idx} - Main question block became stale. Skipping this Q on this page.")
                    continue # Move to the next question block on this category page
                except Exception as e_q_block_processing:
                    print(f"    [{thread_name}] Q_idx {q_block_idx} - Error processing question block for '{question_text[:60]}...': {e_q_block_processing}")
                    # traceback.print_exc(limit=2)
                    continue


                # After iterating all Q-review pages for this question from this category page
                if all_reviews_for_this_q_on_this_cat_page:
                    # Merge with master list for the category
                    existing_q_obj = next((q for q in collected_questions_for_this_category if q.question_text == question_text), None)
                    if existing_q_obj:
                        newly_merged_count = 0
                        for r_new in all_reviews_for_this_q_on_this_cat_page: # these are already globally unique
                            if not any(er.text == r_new.text and er.date == r_new.date for er in existing_q_obj.review_section.reviews):
                                existing_q_obj.review_section.reviews.append(r_new)
                                newly_merged_count +=1
                        if newly_merged_count > 0:
                            existing_q_obj.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
                        print(f"    [{thread_name}] Q_idx {q_block_idx} - Merged {newly_merged_count} new reviews into existing Q object. Total now: {len(existing_q_obj.review_section.reviews)}")
                    else:
                        review_section = ReviewSection(section_name=category_name_arg, reviews=all_reviews_for_this_q_on_this_cat_page)
                        question_obj = Question(question_text=question_text, review_section=review_section)
                        collected_questions_for_this_category.append(question_obj)
                        print(f"    [{thread_name}] Q_idx {q_block_idx} - Collected {len(all_reviews_for_this_q_on_this_cat_page)} reviews for NEW Q: '{question_text[:30]}'")
            # End of loop for question blocks on this category page

            # --- Category Page Pagination (Selenium) ---
            attempt_to_close_popups(category_driver, thread_name) # Try closing popups before next page click
            
            next_category_page_button_sel_elem = None
            soup_for_cat_nav = BeautifulSoup(category_driver.page_source, DEFAULT_HTML_PARSER)
            cat_page_nav_scope_bs = soup_for_cat_nav.find('nav', attrs={'aria-label': lambda x: x and 'pagination' in x.lower()}) or \
                                 soup_for_cat_nav.find('ul', class_=lambda x: x and 'pagination' in x.lower()) or \
                                 soup_for_cat_nav
            
            # Try to find the category next button (logic from previous version, seems mostly okay)
            # ... (this part of finding next_category_page_button_sel_elem is largely the same as your last working version)
            for sel in NEXT_PAGE_SELECTORS:
                potential_bs_btns = cat_page_nav_scope_bs.select(sel)
                for btn_s_tag in potential_bs_btns: 
                    if btn_s_tag.find_parent('div', class_=QUESTION_BLOCK_SELECTOR.split('.')[-1]): continue
                    
                    aria_label_bs = btn_s_tag.get("aria-label", "").lower()
                    rel_bs_val = btn_s_tag.get("rel"); rel_bs = " ".join(rel_bs_val).lower() if isinstance(rel_bs_val, list) else (rel_bs_val or "").lower()
                    text_bs = btn_s_tag.get_text(strip=True).lower()
                    combined_bs = f"{aria_label_bs} {rel_bs} {text_bs}"
                    is_prev_bs = "prev" in combined_bs
                    class_bs = btn_s_tag.get('class', [])
                    is_disabled_bs = any(c in class_bs for c in ['disabled', 'inactive']) or btn_s_tag.has_attr('disabled')
                    href_bs = btn_s_tag.get('href')

                    if not is_prev_bs and not is_disabled_bs and href_bs and href_bs != '#':
                        try:
                            selenium_potential_cat_next_btns = category_driver.find_elements(By.CSS_SELECTOR, sel)
                            for sel_btn_elem in selenium_potential_cat_next_btns:
                                if not sel_btn_elem.is_displayed() or not sel_btn_elem.is_enabled(): continue
                                try:
                                    sel_btn_elem.find_element(By.XPATH, f"./ancestor::div[contains(@class, '{QUESTION_BLOCK_SELECTOR.split('.')[-1]}')]")
                                    continue 
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

            print(f"  [{thread_name}] Attempting to click 'Next Category Page'...")
            if try_click(category_driver, next_category_page_button_sel_elem):
                time.sleep(random.uniform(1.8, 2.8)) # Wait for next category page to load
                attempt_to_close_popups(category_driver, thread_name) # Close popups on new page
            else:
                print(f"  [{thread_name}] Failed to click 'Next Category Page'. Stopping category pagination.")
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

# --- Main Orchestrator (Unchanged) ---
def scrape_comparably_sync(
    company_base_url_str: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating PURE SELENIUM scrape for: {company_slug} (v{app.version})")
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
    print(f"  [{company_slug}] Starting PURE SELENIUM parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")
    
    category_processing_start_time = time.perf_counter()
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories, thread_name_prefix="SelPurePool") as executor:
        for cat_name_from_list in REVIEW_CATEGORIES:
            future = executor.submit(
                _scrape_category_reviews_selenium_pure, 
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
                if questions_from_category:
                    print(f"  [{company_slug}] Received {len(questions_from_category)} Qs from cat '{original_category_name_processed}'. Merging...")
                    all_questions_for_company.extend(questions_from_category)
                else:
                    print(f"  [{company_slug}] Cat '{original_category_name_processed}' returned no Qs.")
            except Exception as e_future_exc:
                print(f"  [{company_slug}] PURE SELENIUM Category task for '{original_category_name_processed}' FAILED in executor: {e_future_exc}")
                traceback.print_exc()
    
    category_processing_duration = time.perf_counter() - category_processing_start_time
    print(f"  [{company_slug}] All category threads (Pure Selenium) completed processing in {category_processing_duration:.2f}s.")

    total_duration = time.perf_counter() - orchestration_start_time
    print(f"\nFinished ALL PURE SELENIUM scrapes for {company_slug} in {total_duration:.2f}s. Total Qs collected: {len(all_questions_for_company)}")
    
    if company_details_overall.get("company_name", "").lower() in REVIEW_CATEGORIES:
        company_details_overall["company_name"] = company_name_from_slug
        company_details_overall["status_note"] = "Company name reset to slug-derived as it was a category name."

    return {
        "status": "success" if all_questions_for_company else "partial_success_no_reviews",
        "data": {
            "company_info": company_details_overall,
            "reviews": [q.model_dump(mode='json') for q in all_questions_for_company]
        }
    }

# --- FastAPI Endpoint (Unchanged from previous Selenium-Pure) ---
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
    print(f"API request: {len(urls)} URLs, Pure Selenium Navigation (v{app.version}).")
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
                print(f"Task for {original_url_str} (Pure Selenium Refined) EXCEPTION: {result_or_exc}")
                tb_str = "".join(traceback.format_exception(None, result_or_exc, result_or_exc.__traceback__))
                print(f"FULL TRACEBACK for {original_url_str} (Pure Selenium Refined):\n{tb_str}")
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed. Type: {type(result_or_exc).__name__}. Check logs."}
            elif isinstance(result_or_exc, dict): results[original_url_str] = result_or_exc
            else: results[original_url_str] = {"status": "error", "message": "Unexpected internal result type"}
            task_idx +=1
        else: results[original_url_str] = {"status": "error", "message": "Scraping task result missing."}
        
    print(f"Finished API request processing (Pure Selenium v{app.version}).")
    return results

