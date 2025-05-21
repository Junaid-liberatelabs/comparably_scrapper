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
    section_name: str # This will be the category name (e.g., leadership)
    reviews: List[Review]

class Question(BaseModel):
    question_text: str
    review_section: ReviewSection # Contains all reviews for this question from all its pages

class ScrapeRequest(BaseModel):
    urls: List[HttpUrl]
    start_date_str: Optional[str] = Field(None, description="Optional start date (YYYY-MM-DD) for reviews.")
    end_date_str: Optional[str] = Field(None, description="Optional end date (YYYY-MM-DD) for reviews.")

# --- FastAPI ---
from fastapi import FastAPI, HTTPException, Body
app = FastAPI(
    title="Comparably Scraper API - Hybrid Deep Reviews & Date Filter",
    description="API to scrape Comparably reviews, handling category pagination (Hybrid) and per-question review pagination (HTTPX), with date filtering.",
    version="1.8.0" # Version bump for deep review pagination
)
 
# --- Selenium and BeautifulSoup ---
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- HTTPX ---
import httpx

# --- User Agent ---
try:
    from fake_useragent import UserAgent
    ua = UserAgent()
except ImportError:
    print("Warning: fake-useragent not installed. Using a generic User-Agent.")
    ua = None

# --- Constants ---
REVIEW_CATEGORIES = ["leadership", "compensation", "team", "environment", "outlook"]
MAX_CATEGORY_PAGES = 15 # Max pages for a category (e.g., Leadership P1, P2...)
MAX_REVIEW_PAGES_PER_QUESTION = 20 # Max pages of reviews for a single question
SELENIUM_PAGE_TIMEOUT_S = 30
SELENIUM_ELEMENT_TIMEOUT_S = 18
HTTPX_TIMEOUT_S = 30

# Selectors for "Next Page" button (generic, used for both category and Q-review pagination)
NEXT_PAGE_SELECTORS = [
    "a.pagination-link[rel='next']", # Common Bootstrap-like
    "a[aria-label*='Next Page' i]", # Case-insensitive aria-label
    "a[title*='Next Page' i]",      # Case-insensitive title
    "li.pagination-next > a",
    "a.pagination-next",
    "nav[aria-label*='pagination' i] li:last-child a[href]", # Common nav structure
    ".qa-PaginationPageLink-Next", # Comparably specific class from observation
    ".page-next > a" # Another common pattern
]
REVIEW_BLOCK_CSS_SELECTOR = "div.cppRH" # Main review item container


# --- Helper: Extract Section Name (Unchanged) ---
def extract_section_name_from_url(href: Optional[str]) -> str:
    if not href: return "unknown_section"
    try:
        path_parts = urlparse(href).path.strip('/').split('/')
        if len(path_parts) >= 4 and path_parts[2] == 'reviews': return path_parts[3]
    except Exception: pass
    match = re.search(r'/reviews/(\w+)', href)
    return match.group(1) if match else "unknown_section"

# --- NEW HELPER: Parses reviews from a given HTML block ---
def _parse_reviews_from_block(
    review_container_soup: BeautifulSoup,
    start_date_filter: Optional[datetime],
    end_date_filter: Optional[datetime]
) -> List[Review]:
    reviews_found: List[Review] = []
    # Directly find review items, assuming review_container_soup is the correct scope
    review_blocks = review_container_soup.find_all('div', class_='cppRH') 
    for block_idx, block in enumerate(review_blocks):
        quote = block.find('p', class_='cppRH-review-quote')
        if not quote:
            # print(f"        _parse_reviews: Skipping block {block_idx} (no quote)")
            continue
        text = quote.get_text(strip=True).replace('\u0000', '')
        
        cite_block = block.find('cite', class_='cppRH-review-cite')
        date_meta_tag = None
        if cite_block:
            date_meta_tag = cite_block.find('meta', {'itemprop': 'datePublished'}) or \
                         cite_block.find('meta', attrs={'content': re.compile(r'^\d{4}-\d{2}-\d{2}$')})
        
        if not date_meta_tag or not date_meta_tag.get('content'):
            # print(f"        _parse_reviews: Skipping review (no date_meta_tag): '{text[:30]}...'")
            continue
        
        try:
            date_val = datetime.strptime(date_meta_tag['content'], '%Y-%m-%d')
        except ValueError:
            # print(f"        _parse_reviews: Skipping review (date parse error): '{text[:30]}...'")
            continue

        if start_date_filter and date_val < start_date_filter:
            # print(f"        _parse_reviews: Skipping review (before start_date): {date_val.date()} '{text[:30]}...'")
            continue
        if end_date_filter and date_val > end_date_filter:
            # print(f"        _parse_reviews: Skipping review (after end_date): {date_val.date()} '{text[:30]}...'")
            continue
        
        reviews_found.append(Review(text=text, date=date_val))
    return reviews_found


# --- Function to Extract Basic Company Info (Unchanged) ---
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

# --- Selenium Setup (Unchanged) ---
def setup_selenium_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
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

# --- MODIFIED: Hybrid Scraper for a single category, with deep review pagination ---
def _scrape_specific_category_all_pages_hybrid(
    company_base_url_str: str,
    category_name: str, # e.g. "leadership"
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    thread_name = f"HybridDeep-{category_name}-{company_slug[:10]}"
    print(f"  [{thread_name}] Started.")
    
    # Stores Question objects for this category
    collected_questions_for_category: List[Question] = []
    # Global for this category run: (hash(question_text), hash(review.text), review.date)
    processed_reviews_keys_in_this_category_run = set() 

    # --- Helper to process one category page's soup for questions and their paginated reviews ---
    def _process_one_category_page_soup(
        cat_page_soup: BeautifulSoup,
        cat_page_url: str, # URL that provided this soup, for resolving relative links
        http_client_for_q_reviews: httpx.Client, # Client to fetch Q-review sub-pages
        common_headers: Dict[str, str] # Headers for sub-page requests
    ):
        nonlocal collected_questions_for_category, processed_reviews_keys_in_this_category_run
        
        question_blocks_on_cat_page = cat_page_soup.find_all('div', class_='reviewsList')
        if not question_blocks_on_cat_page:
            print(f"    [{thread_name}] No question blocks (div.reviewsList) found on category page: {cat_page_url}")
            return

        for q_block_idx, q_block_soup_initial in enumerate(question_blocks_on_cat_page):
            q_elem = q_block_soup_initial.find('h2', class_='section-subtitle')
            if not q_elem: continue
            question_text = q_elem.get_text(strip=True)
            print(f"    [{thread_name}] Q{q_block_idx+1}: '{question_text[:60]}...'")
            
            all_reviews_for_this_single_q: List[Review] = []
            current_q_reviews_content_soup = q_block_soup_initial # Start with the content from category page
            current_q_reviews_source_url = cat_page_url
            
            q_review_page_num = 0
            while q_review_page_num < MAX_REVIEW_PAGES_PER_QUESTION:
                q_review_page_num += 1
                # print(f"      [{thread_name}] Q-Review Page {q_review_page_num} for '{question_text[:30]}...' (Source: {current_q_reviews_source_url})")

                reviews_from_current_content = _parse_reviews_from_block(
                    current_q_reviews_content_soup, start_date_filter, end_date_filter
                )
                
                newly_added_to_this_q_count = 0
                for r_parsed in reviews_from_current_content:
                    r_key = (hash(question_text), hash(r_parsed.text), r_parsed.date)
                    if r_key not in processed_reviews_keys_in_this_category_run:
                        all_reviews_for_this_single_q.append(r_parsed)
                        processed_reviews_keys_in_this_category_run.add(r_key)
                        newly_added_to_this_q_count +=1
                
                if newly_added_to_this_q_count > 0:
                    print(f"        [{thread_name}] Added {newly_added_to_this_q_count} unique reviews for this Q (Q-Page {q_review_page_num}).")
                
                # Find "next page" link for this question's reviews *within current_q_reviews_content_soup*
                next_q_review_page_href = None
                # Search within the current block for its own pagination
                # Common pagination structures: <nav aria-label="pagination"> or <ul class="pagination">
                pagination_container = current_q_reviews_content_soup.find(['nav', 'ul'], 
                                                                    class_=lambda x: x and any(p in x.lower() for p in ['pagination', 'pager']),
                                                                    attrs={'aria-label': lambda x: x and 'pagination' in x.lower()}
                                                                    ) or current_q_reviews_content_soup # Fallback to whole block
                
                for sel in NEXT_PAGE_SELECTORS:
                    buttons = pagination_container.select(sel)
                    for btn_tag in buttons:
                        href = btn_tag.get('href')
                        # Check attributes to ensure it's "next" not "prev"
                        aria_label = (btn_tag.get("aria-label", "") or "").lower()
                        rel_attr = (btn_tag.get("rel", "") or "").lower()
                        title_attr = (btn_tag.get("title", "") or "").lower()
                        text_content = btn_tag.get_text(strip=True).lower()

                        is_prev = any(indicator in val for indicator in ["prev", "older", "back"] for val in [aria_label, rel_attr, title_attr, text_content])
                        is_disabled = any(cls in (btn_tag.get('class', [])) for cls in ['disabled', 'inactive']) or btn_tag.has_attr('disabled')
                        
                        if is_prev or is_disabled: continue
                        
                        if href and href != "#" and not href.startswith("javascript:"):
                            next_q_review_page_href = urljoin(current_q_reviews_source_url, href)
                            # print(f"        [{thread_name}] Found next Q-review page link: {next_q_review_page_href} (selector: '{sel}')")
                            break
                    if next_q_review_page_href: break
                
                if not next_q_review_page_href:
                    # print(f"        [{thread_name}] No more Q-review pages for '{question_text[:30]}...'.")
                    break # Exit while loop for this question's review pages

                # Fetch the next page of reviews for THIS question
                try:
                    time.sleep(random.uniform(0.6, 1.3)) # Delay for Q-review pages
                    q_review_headers = common_headers.copy()
                    q_review_headers['Referer'] = current_q_reviews_source_url
                    
                    # print(f"        [{thread_name}] HTTPX fetching Q-review page: {next_q_review_page_href}")
                    response_q_review_page = http_client_for_q_reviews.get(next_q_review_page_href, headers=q_review_headers)
                    response_q_review_page.raise_for_status()
                    
                    current_q_reviews_content_soup = BeautifulSoup(response_q_review_page.text, 'html.parser')
                    current_q_reviews_source_url = str(response_q_review_page.url)
                except Exception as e_q_rev_fetch:
                    print(f"        [{thread_name}] Error fetching Q-review page {next_q_review_page_href}: {e_q_rev_fetch}")
                    break # Stop trying for this question
            # End of while loop for a single question's review pages (Q-pagination)
            
            if all_reviews_for_this_single_q:
                all_reviews_for_this_single_q.sort(key=lambda r: r.date, reverse=True)
                # Add or merge this Question object
                existing_question_obj = next((q for q in collected_questions_for_category if q.question_text == question_text), None)
                if existing_question_obj: # Should be rare if questions are distinct per category page
                    print(f"    [{thread_name}] Warning: Question '{question_text[:30]}' re-encountered. Merging reviews.")
                    # `all_reviews_for_this_single_q` already contains unique items for this specific Q's processing pass
                    # The `processed_reviews_keys_in_this_category_run` ensures global uniqueness.
                    # This extend might add duplicates if not careful, but keys set should prevent functional dupes in final list.
                    existing_question_obj.review_section.reviews.extend(r for r in all_reviews_for_this_single_q if not any(er.text == r.text and er.date == r.date for er in existing_question_obj.review_section.reviews))
                    existing_question_obj.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
                else:
                    try:
                        review_section = ReviewSection(section_name=category_name, reviews=all_reviews_for_this_single_q)
                        question_obj = Question(question_text=question_text, review_section=review_section)
                        collected_questions_for_category.append(question_obj)
                    except ValidationError as e_val:
                        print(f"    [{thread_name}] Pydantic error for Q '{question_text[:30]}...': {e_val}")
                # print(f"    [{thread_name}] Finalized Q: '{question_text[:30]}' with {len(all_reviews_for_this_single_q)} unique reviews after its pagination.")
        # End of for loop over q_blocks on one category page
    # End of _process_one_category_page_soup

    # --- Phase 1: Selenium for first CATEGORY page ---
    driver = None
    next_category_page_url_from_selenium: Optional[str] = None
    selenium_cookies_for_hybrid: Optional[Dict[str, str]] = None
    category_url_start = urljoin(company_base_url_str.rstrip('/') + "/", f"reviews/{category_name}/")
    
    user_agent_hdr = ua.random if ua else "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    base_httpx_headers = {
        'User-Agent': user_agent_hdr,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        print(f"  [{thread_name}] Selenium: Initializing for Cat Page 1: {category_url_start}")
        driver = setup_selenium_driver()
        driver.get(category_url_start)
        time.sleep(random.uniform(1.5, 2.5)) # Wait for JS

        if "Error" in driver.title or "Not Found" in driver.title or "Access Denied" in driver.page_source:
            print(f"  [{thread_name}] Selenium: Error page (Title: {driver.title}). Skipping category.")
            return category_name, []

        WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.reviewsList, div.cppRH, body")) # Wait for some content
        )
        
        soup_cat_page1 = BeautifulSoup(driver.page_source, 'html.parser')
        current_cat_page_url_p1 = driver.current_url
        s_cookies = driver.get_cookies()
        selenium_cookies_for_hybrid = {c['name']: c['value'] for c in s_cookies}

        print(f"  [{thread_name}] Selenium: Processing Cat Page 1 ({current_cat_page_url_p1}) questions & their review sub-pages...")
        # Use an httpx client with Selenium's cookies to process Q-reviews on Cat Page 1
        with httpx.Client(cookies=selenium_cookies_for_hybrid, follow_redirects=True, timeout=HTTPX_TIMEOUT_S) as q_review_client_for_cat_p1:
            _process_one_category_page_soup(
                soup_cat_page1, 
                current_cat_page_url_p1,
                q_review_client_for_cat_p1,
                base_httpx_headers.copy()
            )

        # Extract NEXT CATEGORY page_url from soup_cat_page1 (Selenium-viewed page)
        for sel in NEXT_PAGE_SELECTORS:
            buttons = soup_cat_page1.select(sel) # Use select on soup
            for btn_tag in buttons:
                href = btn_tag.get('href')
                aria_label = (btn_tag.get("aria-label", "") or "").lower()
                rel_attr = (btn_tag.get("rel", "") or "").lower()
                title_attr = (btn_tag.get("title", "") or "").lower()
                text_content = btn_tag.get_text(strip=True).lower()
                is_prev = any(indicator in val for indicator in ["prev", "older", "back"] for val in [aria_label, rel_attr, title_attr, text_content])
                is_disabled = any(cls in (btn_tag.get('class', [])) for cls in ['disabled', 'inactive']) or btn_tag.has_attr('disabled')
                if is_prev or is_disabled: continue

                if href and href != '#' and not href.startswith("javascript:"):
                    next_category_page_url_from_selenium = urljoin(current_cat_page_url_p1, href)
                    print(f"  [{thread_name}] Selenium: Found Next Cat Page link: {next_category_page_url_from_selenium}")
                    break
            if next_category_page_url_from_selenium: break
        
    except Exception as e_sel_phase:
        print(f"  [{thread_name}] CRITICAL ERROR during Selenium phase: {e_sel_phase}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print(f"  [{thread_name}] Selenium WebDriver instance for Cat Page 1 quit.")

    # --- HTTPX Phase for subsequent CATEGORY pages ---
    if not next_category_page_url_from_selenium:
        print(f"  [{thread_name}] Finished after Selenium phase (no next category page). Total unique Qs: {len(collected_questions_for_category)}")
        return category_name, collected_questions_for_category

    current_httpx_cat_page_url = next_category_page_url_from_selenium
    category_page_count = 1 # Selenium handled page 1
    
    with httpx.Client(cookies=selenium_cookies_for_hybrid, follow_redirects=True, timeout=HTTPX_TIMEOUT_S) as main_category_httpx_client:
        while current_httpx_cat_page_url and category_page_count < MAX_CATEGORY_PAGES:
            category_page_count += 1
            print(f"  [{thread_name}] HTTPX: Requesting Cat Page {category_page_count} (URL: {current_httpx_cat_page_url})")
            
            httpx_cat_headers = base_httpx_headers.copy()
            # Set Referer to the URL of the page that linked to this one
            if category_page_count == 2 and current_cat_page_url_p1 : # First httpx cat page request
                 httpx_cat_headers['Referer'] = current_cat_page_url_p1
            elif 'previous_cat_page_url_for_referer' in locals() and previous_cat_page_url_for_referer:
                 httpx_cat_headers['Referer'] = previous_cat_page_url_for_referer


            try:
                time.sleep(random.uniform(0.8, 1.9)) # Politeness delay for category pages
                response_cat_page = main_category_httpx_client.get(current_httpx_cat_page_url, headers=httpx_cat_headers)
                previous_cat_page_url_for_referer = str(response_cat_page.url) # For next iteration
                response_cat_page.raise_for_status()

                soup_httpx_cat_pageN = BeautifulSoup(response_cat_page.text, 'html.parser')
                
                if "Error" in (soup_httpx_cat_pageN.title.string if soup_httpx_cat_pageN.title else "") or "Access Denied" in response_cat_page.text:
                    print(f"  [{thread_name}] HTTPX: Error page detected on Cat Page {category_page_count}. Stopping.")
                    break
                if not soup_httpx_cat_pageN.find(['div.reviewsList', 'div.cppRH']): # Check for actual review content
                    print(f"  [{thread_name}] HTTPX: No review/question blocks found on Cat Page {category_page_count}. Likely end.")
                    break

                print(f"  [{thread_name}] HTTPX: Processing Cat Page {category_page_count} ({previous_cat_page_url_for_referer}) questions & their review sub-pages...")
                _process_one_category_page_soup(
                    soup_httpx_cat_pageN,
                    previous_cat_page_url_for_referer, 
                    main_category_httpx_client, # This same client fetches Q-review sub-pages
                    base_httpx_headers.copy() # Pass base headers, referer will be handled by _process_one_category_page_soup for its sub-requests
                )

                # Extract NEXT CATEGORY page_url from soup_httpx_cat_pageN
                next_cat_page_url_from_httpx = None
                for sel in NEXT_PAGE_SELECTORS:
                    buttons = soup_httpx_cat_pageN.select(sel)
                    for btn_tag in buttons:
                        href = btn_tag.get('href')
                        aria_label = (btn_tag.get("aria-label", "") or "").lower()
                        rel_attr = (btn_tag.get("rel", "") or "").lower()
                        title_attr = (btn_tag.get("title", "") or "").lower()
                        text_content = btn_tag.get_text(strip=True).lower()
                        is_prev = any(indicator in val for indicator in ["prev", "older", "back"] for val in [aria_label, rel_attr, title_attr, text_content])
                        is_disabled = any(cls in (btn_tag.get('class', [])) for cls in ['disabled', 'inactive']) or btn_tag.has_attr('disabled')
                        if is_prev or is_disabled: continue
                        
                        if href and href != '#' and not href.startswith("javascript:"):
                            next_cat_page_url_from_httpx = urljoin(previous_cat_page_url_for_referer, href)
                            # print(f"  [{thread_name}] HTTPX: Found Next Cat Page link: {next_cat_page_url_from_httpx}")
                            break
                    if next_cat_page_url_from_httpx: break
                
                current_httpx_cat_page_url = next_cat_page_url_from_httpx
                if not current_httpx_cat_page_url:
                    print(f"  [{thread_name}] HTTPX: No Next Cat Page link found on Cat Page {category_page_count}.")
                    break

            except Exception as e_httpx_cat_page:
                print(f"  [{thread_name}] HTTPX: Error on Cat Page {category_page_count} (URL: {current_httpx_cat_page_url}): {e_httpx_cat_page}")
                traceback.print_exc()
                break
    
    print(f"  [{thread_name}] Finished. Total unique Qs for category: {len(collected_questions_for_category)}")
    return category_name, collected_questions_for_category


# --- Main Orchestrator (Unchanged from v1.7.0 - calls the modified hybrid scraper) ---
def scrape_comparably_sync(
    company_base_url_str: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating HYBRID DEEP REVIEWS parallel category scrape for: {company_slug}")
    start_time_total = time.time()
    all_questions_for_company: List[Question] = [] # Final list of all questions from all categories
    company_details_overall: Dict[str, Any] = {}
    initial_info_driver = None

    # Step 1: Fetch Initial Company Info (Selenium)
    try:
        print(f"  [{company_slug}] Fetching initial company info with Selenium...")
        initial_info_driver = setup_selenium_driver()
        info_fetch_url = urljoin(company_base_url_str.rstrip('/') + "/", "reviews/")
        initial_info_driver.get(info_fetch_url)
        time.sleep(1.5)
        if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title or "Access Denied" in initial_info_driver.page_source:
            print(f"  [{company_slug}] /reviews/ page for info failed, trying base URL: {company_base_url_str}")
            initial_info_driver.get(company_base_url_str)
            time.sleep(1.5)
            if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title or "Access Denied" in initial_info_driver.page_source:
                raise Exception(f"Could not load a valid page for company info (Title: {initial_info_driver.title})")
        
        info_html = initial_info_driver.page_source
        info_soup = BeautifulSoup(info_html, 'html.parser')
        company_details_overall = extract_company_info(info_soup, company_base_url_str)
        print(f"  [{company_slug}] Initial company info fetched: Name='{company_details_overall.get('company_name')}'")
    except Exception as e_info:
        print(f"  [{company_slug}] Error fetching initial company info: {e_info}")
        company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": f"Initial info fetch error: {str(e_info)}"}
    finally:
        if initial_info_driver:
            try: initial_info_driver.quit()
            except Exception as e_close: print(f"  [{company_slug}] Error closing Selenium info browser: {e_close}")

    # Step 2: Scrape categories in parallel using the hybrid scraper
    max_concurrent_categories = min(len(REVIEW_CATEGORIES), 3)
    print(f"  [{company_slug}] Starting HYBRID DEEP REVIEWS parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories, thread_name_prefix="HybridDeepPool") as executor:
        for cat_name_iter in REVIEW_CATEGORIES: # Use a different variable name
            future = executor.submit(
                _scrape_specific_category_all_pages_hybrid,
                company_base_url_str, cat_name_iter, company_slug,
                start_date_filter, end_date_filter
            )
            futures_map[future] = cat_name_iter
        
        for future in as_completed(futures_map):
            original_category_name_from_future = futures_map[future]
            try:
                _, questions_from_category = future.result() 
                if questions_from_category:
                    print(f"  [{company_slug}] Received {len(questions_from_category)} Qs from cat '{original_category_name_from_future}'. Merging...")
                    all_questions_for_company.extend(questions_from_category)
                else:
                    print(f"  [{company_slug}] Cat '{original_category_name_from_future}' returned no Qs.")
            except Exception as e_future_exc:
                print(f"  [{company_slug}] HYBRID DEEP REVIEWS Category task for '{original_category_name_from_future}' FAILED in executor: {e_future_exc}")
                traceback.print_exc()

    total_duration = time.time() - start_time_total
    print(f"\nFinished ALL HYBRID DEEP REVIEWS scrapes for {company_slug} in {total_duration:.2f}s. Total Qs collected: {len(all_questions_for_company)}")
    
    if not company_details_overall.get("company_name") or company_details_overall.get("company_name", "").lower() == company_slug.lower() or company_details_overall.get("company_name", "") == "unknown_company":
        current_name = company_details_overall.get("company_name", "unknown_company")
        fallback_name = company_slug.replace('-', ' ').title()
        if current_name.lower() in REVIEW_CATEGORIES or current_name == "unknown_company":
             company_details_overall["company_name"] = fallback_name
             if "status_note" not in company_details_overall: company_details_overall["status_note"] = "Name set to fallback slug-based name."
             print(f"  [{company_slug}] Company name was '{current_name}', updated to fallback '{fallback_name}'.")

    return {
        "status": "success" if all_questions_for_company or (company_details_overall.get("company_name") != company_slug.replace('-', ' ').title() and company_details_overall.get("company_name") != "unknown_company") else "partial_success_no_reviews",
        "data": {
            "company_info": company_details_overall,
            "reviews": [q.model_dump(mode='json') for q in all_questions_for_company]
        }
    }

# --- FastAPI Endpoint (Unchanged from v1.7.0) ---
@app.post("/scrape")
async def scrape_companies(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    urls = request.urls
    start_date_filter: Optional[datetime] = None
    end_date_filter: Optional[datetime] = None

    if request.start_date_str:
        try: start_date_filter = datetime.strptime(request.start_date_str, "%Y-%m-%d")
        except ValueError: raise HTTPException(status_code=400, detail="Invalid start_date_str. Use YYYY-MM-DD.")
    if request.end_date_str:
        try: end_date_filter = datetime.strptime(request.end_date_str, "%Y-%m-%d"); end_date_filter = end_date_filter.replace(hour=23, minute=59, second=59)
        except ValueError: raise HTTPException(status_code=400, detail="Invalid end_date_str. Use YYYY-MM-DD.")
    if start_date_filter and end_date_filter and start_date_filter > end_date_filter:
        raise HTTPException(status_code=400, detail="start_date_str cannot be after end_date_str.")
    if not urls:
        raise HTTPException(status_code=400, detail="No URLs provided.")

    results: Dict[str, Dict[str, Any]] = {}
    valid_scrape_params = [] # Stores {'original_url': ..., 'base_url': ..., 'slug': ...}
    
    date_filter_msg = f" (Start: {request.start_date_str or 'N/A'}, End: {request.end_date_str or 'N/A'})"
    print(f"API request: {len(urls)} URLs, Hybrid DeepReviews & Date Filter{date_filter_msg} (v{app.version}).")

    for url_obj in urls:
        url_str = str(url_obj)
        try:
            parsed_url = urlparse(url_str)
            path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
            if not (parsed_url.scheme and parsed_url.netloc and len(path_segments) >= 2 and path_segments[0] == "companies"):
                raise ValueError("URL format error or incomplete URL")
            company_slug = path_segments[1]
            company_base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/companies/{company_slug}"
            valid_scrape_params.append({'original_url': url_str, 'base_url': company_base_url, 'slug': company_slug})
        except Exception as e_slug:
            print(f"Error parsing slug/URL '{url_str}': {e_slug}")
            results[url_str] = {"status": "error", "message": f"Invalid Comparably company URL format: {url_str}. Error: {e_slug}"}
    
    tasks = []
    if valid_scrape_params:
        for params in valid_scrape_params:
            tasks.append(asyncio.to_thread(
                scrape_comparably_sync, 
                params['base_url'], 
                params['slug'], 
                start_date_filter, 
                end_date_filter
            ))
    
    if tasks:
        scraped_results_or_exceptions = await asyncio.gather(*tasks, return_exceptions=True)
    else:
        scraped_results_or_exceptions = []

    task_idx = 0
    for params in valid_scrape_params:
        original_url_str = params['original_url']
        if original_url_str in results: continue 

        if task_idx < len(scraped_results_or_exceptions):
            result_or_exc = scraped_results_or_exceptions[task_idx]
            if isinstance(result_or_exc, Exception):
                print(f"Task for {original_url_str} EXCEPTION (type: {type(result_or_exc).__name__}): {result_or_exc}")
                tb_str = "".join(traceback.format_exception(None, result_or_exc, result_or_exc.__traceback__))
                print(f"FULL TRACEBACK for {original_url_str} (HybridDeep):\n{tb_str}")
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed. Type: {type(result_or_exc).__name__}. Check logs."}
            elif isinstance(result_or_exc, dict):
                results[original_url_str] = result_or_exc
            else:
                results[original_url_str] = {"status": "error", "message": "Unexpected internal result type from scraping task"}
            task_idx +=1
        else:
             results[original_url_str] = {"status": "error", "message": "Scraping task result missing (logic error)."}

    print(f"Finished API request processing (HybridDeep v{app.version}).")
    return results

# --- Health Check Endpoint ---
@app.get("/health")
async def health_check(): return {"status": "ok"}

# --- Main guard for uvicorn ---
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("v3_hybrid_deep_reviews:app", host="0.0.0.0", port=8000, reload=False) # reload=False for stable threading

