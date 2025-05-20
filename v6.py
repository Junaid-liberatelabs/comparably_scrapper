

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
    title="Comparably Scraper API - Hybrid Selenium/HTTPX & Date Filter",
    description="API to scrape company reviews from Comparably using Selenium for the first page and HTTPX for subsequent pages, with date filtering.",
    version="1.7.0" # Version bump for Hybrid Selenium/HTTPX
)

# --- Selenium and BeautifulSoup ---
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException # ElementClickInterceptedException not directly used by hybrid for next page
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- HTTPX for faster subsequent page fetches ---
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
MAX_PAGES_PER_CATEGORY = 15
SELENIUM_PAGE_TIMEOUT_S = 30  # Seconds for page loads
SELENIUM_ELEMENT_TIMEOUT_S = 18 # Seconds for element waits
HTTPX_TIMEOUT_S = 30 # Seconds for httpx requests

# Selectors for "Next Page" button, usable by both Selenium and BeautifulSoup
NEXT_PAGE_SELECTORS = [
    "a.pagination-link[rel='next']",
    "a[aria-label='Next Page']",
    "a[title='Next Page']",
    "li.pagination-next > a",
    "a.pagination-next",
    "nav[aria-label*='pagination'] li:last-child a[href]"
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

# --- Integrated Parsing Logic with Date Filter (Unchanged) ---
def parse_review_page_html(
    soup: BeautifulSoup,
    company_slug: str,
    current_category_for_context: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> List[Question]:
    questions: List[Question] = []
    review_list_divs = soup.find_all('div', class_='reviewsList')
    if not review_list_divs: return []
    for review_list_div in review_list_divs:
        q_elem = review_list_div.find('h2', class_='section-subtitle')
        if not q_elem: continue
        question_text = q_elem.get_text(strip=True)
        section_name_to_use = current_category_for_context
        reviews_for_this_question: List[Review] = []
        review_blocks = review_list_div.find_all('div', class_='cppRH')
        for block in review_blocks:
            quote = block.find('p', class_='cppRH-review-quote')
            if not quote: continue
            text = quote.get_text(strip=True).replace('\u0000', '')
            cite_block = block.find('cite', class_='cppRH-review-cite')
            date_meta = None
            if cite_block:
                date_meta = cite_block.find('meta', {'itemprop': 'datePublished'}) or \
                            cite_block.find('meta', attrs={'content': re.compile(r'^\d{4}-\d{2}-\d{2}$')})
            if not date_meta or not date_meta.get('content'): continue
            try: date_val = datetime.strptime(date_meta['content'], '%Y-%m-%d')
            except ValueError: continue
            if start_date_filter and date_val < start_date_filter: continue
            if end_date_filter and date_val > end_date_filter: continue
            reviews_for_this_question.append(Review(text=text, date=date_val))
        if not reviews_for_this_question: continue
        reviews_for_this_question.sort(key=lambda r: r.date, reverse=True)
        try:
            section = ReviewSection(section_name=section_name_to_use, reviews=reviews_for_this_question)
            questions.append(Question(question_text=question_text, review_section=section))
        except ValidationError as e: print(f"Pydantic validation error (Date Filtered) for '{question_text}' in cat '{current_category_for_context}': {e}")
    return questions

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
            name_candidate = h1_text # Default to full h1 text
            if " Reviews" in h1_text: name_candidate = h1_text.split(" Reviews")[0].strip()
            # Added check to ensure candidate is not a category name and has reasonable length
            if name_candidate and name_candidate.lower() not in REVIEW_CATEGORIES and len(name_candidate) > 3:
                details['company_name'] = name_candidate
        # Fallback or refinement using title tag
        if details['company_name'] == default_name or details['company_name'].lower() in REVIEW_CATEGORIES:
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                name_from_title = title_text.split(" Reviews")[0].split(" | Comparably")[0].strip()
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
        # print(f"  [Selenium Setup] Initializing WebDriver with ChromeDriverManager...") # Less verbose
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT_S)
        # print(f"  [Selenium Setup] WebDriver initialized.") # Less verbose
        return driver
    except Exception as e:
        print(f"  [Selenium Setup] CRITICAL ERROR during Selenium setup: {e}")
        traceback.print_exc()
        raise RuntimeError(f"Failed to setup Selenium WebDriver: {e}")


# --- MODIFIED: Hybrid Scraper for a single category ---
def _scrape_specific_category_all_pages_hybrid(
    company_base_url_str: str,
    category_name: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    thread_name = f"HybridScraper-{category_name}-{company_slug[:10]}"
    print(f"  [{thread_name}] Started.")
    category_questions: List[Question] = []
    processed_reviews_in_this_category_run = set()

    next_page_url_from_selenium: Optional[str] = None
    selenium_cookies_dict: Optional[Dict[str, str]] = None
    
    # --- Phase 1: Selenium for the first page ---
    driver = None
    try:
        print(f"  [{thread_name}] Initializing Selenium for first page...")
        driver = setup_selenium_driver()
        category_wait = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S)
        
        category_url_start = urljoin(company_base_url_str.rstrip('/') + "/", f"reviews/{category_name}/")
        print(f"  [{thread_name}] Selenium navigating to initial page: {category_url_start}")
        driver.get(category_url_start)
        time.sleep(random.uniform(0.8, 1.5)) # Wait for potential JS rendering

        if "Error" in driver.title or "Not Found" in driver.title or "Access Denied" in driver.page_source:
            print(f"  [{thread_name}] Selenium: Error page (Title: {driver.title}). Skipping category.")
            return category_name, []

        print(f"  [{thread_name}] Selenium: Scraping page 1 (URL: {driver.current_url})")
        try:
            category_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, REVIEW_BLOCK_CSS_SELECTOR)))
        except TimeoutException:
            print(f"  [{thread_name}] Selenium: Timeout waiting for review content on page 1. Category may be empty or inaccessible.")
            return category_name, []

        html_content = driver.page_source
        soup_page1 = BeautifulSoup(html_content, 'html.parser')
        questions_page1 = parse_review_page_html(soup_page1, company_slug, category_name, start_date_filter, end_date_filter)
        
        # Merge logic for questions (common for both Selenium and HTTPX phases)
        def merge_questions(new_questions: List[Question]):
            nonlocal category_questions, processed_reviews_in_this_category_run
            reviews_added_count = 0
            for q_new in new_questions:
                existing_q = next((q for q in category_questions if q.question_text == q_new.question_text), None)
                if not existing_q:
                    unique_reviews_for_new_q = []
                    for r_new in q_new.review_section.reviews:
                        r_key = (hash(q_new.question_text), hash(r_new.text), r_new.date)
                        if r_key not in processed_reviews_in_this_category_run:
                            unique_reviews_for_new_q.append(r_new)
                            processed_reviews_in_this_category_run.add(r_key)
                            reviews_added_count += 1
                    if unique_reviews_for_new_q:
                        q_new.review_section.reviews = unique_reviews_for_new_q
                        category_questions.append(q_new)
                else:
                    for r_new in q_new.review_section.reviews:
                        r_key = (hash(existing_q.question_text), hash(r_new.text), r_new.date)
                        if r_key not in processed_reviews_in_this_category_run:
                            existing_q.review_section.reviews.append(r_new)
                            processed_reviews_in_this_category_run.add(r_key)
                            reviews_added_count += 1
                    existing_q.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
            return reviews_added_count

        added_count_p1 = merge_questions(questions_page1)
        if added_count_p1 > 0: print(f"  [{thread_name}] Selenium: Added {added_count_p1} unique reviews from page 1.")
        elif not questions_page1: print(f"  [{thread_name}] Selenium: No reviews parsed from page 1.")


        # Extract next_page_url and cookies using Selenium
        for sel in NEXT_PAGE_SELECTORS:
            try:
                buttons = driver.find_elements(By.CSS_SELECTOR, sel)
                for btn in buttons:
                    if btn.is_displayed():
                        href = btn.get_attribute('href')
                        aria_label = (btn.get_attribute("aria-label") or "").lower()
                        rel_attr = (btn.get_attribute("rel") or "").lower()
                        title_attr = (btn.get_attribute("title") or "").lower()
                        # Ensure it's not a "previous" link
                        if "prev" in aria_label or "prev" in rel_attr or "prev" in title_attr:
                            continue
                        if href and href != '#' and not href.startswith('javascript:'):
                            next_page_url_from_selenium = urljoin(driver.current_url, href)
                            print(f"  [{thread_name}] Selenium: Found 'Next Page' href: {next_page_url_from_selenium} with selector '{sel}'")
                            break
                if next_page_url_from_selenium: break
            except Exception: continue
        
        if next_page_url_from_selenium:
            s_cookies = driver.get_cookies()
            selenium_cookies_dict = {c['name']: c['value'] for c in s_cookies}
            # print(f"  [{thread_name}] Selenium: Extracted {len(selenium_cookies_dict)} cookies.")
        else:
            print(f"  [{thread_name}] Selenium: No 'Next Page' href found on page 1. Category might have only one page.")

    except Exception as e_sel:
        print(f"  [{thread_name}] CRITICAL ERROR during Selenium phase: {e_sel}")
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()
            print(f"  [{thread_name}] Selenium WebDriver instance quit.")

    # --- Phase 2: httpx for subsequent pages ---
    if not next_page_url_from_selenium:
        print(f"  [{thread_name}] Finished after Selenium phase (no next page found). Total Qs: {len(category_questions)}")
        return category_name, category_questions

    page_count = 1 # Selenium handled page 1
    current_url_for_httpx = next_page_url_from_selenium
    
    user_agent_to_use = ua.random if ua else "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    httpx_headers = {
        'User-Agent': user_agent_to_use,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        # Referer will be set to the previous page's URL dynamically
    }

    with httpx.Client(cookies=selenium_cookies_dict, follow_redirects=True, timeout=HTTPX_TIMEOUT_S) as http_client:
        while current_url_for_httpx and page_count < MAX_PAGES_PER_CATEGORY:
            page_count += 1
            print(f"  [{thread_name}] HTTPX: Requesting page {page_count} (URL: {current_url_for_httpx})")
            
            # Set Referer to the URL of the page that linked to this one
            # For the first httpx request, referer can be the category_url_start or driver.current_url from Selenium phase
            if page_count == 2: # First httpx request
                 httpx_headers['Referer'] = category_url_start # or driver.current_url if available
            # For subsequent httpx requests, Referer is implicitly handled by httpx if it was the previous response's URL,
            # or we could set it explicitly to the previous current_url_for_httpx.
            # httpx.Client with follow_redirects=True handles referrers on redirects.
            # For direct GETs, it's good practice to set it if known.

            try:
                time.sleep(random.uniform(0.7, 1.8)) # Politeness delay
                response = http_client.get(current_url_for_httpx, headers=httpx_headers)
                
                # Update referer for the *next* potential request
                httpx_headers['Referer'] = str(response.url)

                response.raise_for_status()

                html_content_httpx = response.text
                soup_httpx = BeautifulSoup(html_content_httpx, 'html.parser')
                
                if "Error" in (soup_httpx.title.string if soup_httpx.title else "") or "Access Denied" in html_content_httpx:
                    print(f"  [{thread_name}] HTTPX: Error page detected on page {page_count}. Stopping.")
                    break
                if not soup_httpx.find(REVIEW_BLOCK_CSS_SELECTOR.split('.')[0], class_=REVIEW_BLOCK_CSS_SELECTOR.split('.')[1] if '.' in REVIEW_BLOCK_CSS_SELECTOR else None):
                    print(f"  [{thread_name}] HTTPX: No review blocks found on page {page_count}. Likely end of content.")
                    break

                questions_this_page_httpx = parse_review_page_html(soup_httpx, company_slug, category_name, start_date_filter, end_date_filter)
                added_count_httpx = merge_questions(questions_this_page_httpx)

                if added_count_httpx > 0: print(f"  [{thread_name}] HTTPX: Added {added_count_httpx} unique reviews from page {page_count}.")
                elif not questions_this_page_httpx: print(f"  [{thread_name}] HTTPX: No reviews parsed from page {page_count}.")


                # Extract next_page_url using BeautifulSoup from current httpx page
                next_page_url_bs = None
                for sel_bs in NEXT_PAGE_SELECTORS:
                    buttons_bs = soup_httpx.select(sel_bs)
                    for btn_bs in buttons_bs:
                        href_bs = btn_bs.get('href')
                        aria_label_bs = (btn_bs.get("aria-label", "") or "").lower()
                        rel_attr_bs = (btn_bs.get("rel", "") or "").lower()
                        title_bs = (btn_bs.get("title","") or "").lower()
                        
                        if "prev" in aria_label_bs or "prev" in rel_attr_bs or "prev" in title_bs:
                            continue
                        
                        if href_bs and href_bs != '#' and not href_bs.startswith('javascript:'):
                            next_page_url_bs = urljoin(str(response.url), href_bs) # Base URL is the current response URL
                            print(f"  [{thread_name}] HTTPX: Found 'Next Page' href: {next_page_url_bs} with selector '{sel_bs}'")
                            break
                    if next_page_url_bs: break
                
                current_url_for_httpx = next_page_url_bs
                if not current_url_for_httpx:
                    print(f"  [{thread_name}] HTTPX: No 'Next Page' href found on page {page_count}. End of category pagination.")
                    break

            except httpx.RequestError as e_req:
                print(f"  [{thread_name}] HTTPX: Request error for page {page_count} (URL: {current_url_for_httpx}): {e_req}")
                break
            except httpx.HTTPStatusError as e_status:
                print(f"  [{thread_name}] HTTPX: HTTP status error {e_status.response.status_code} for page {page_count} (URL: {current_url_for_httpx}).")
                break
            except Exception as e_page:
                print(f"  [{thread_name}] HTTPX: Error processing page {page_count} (URL: {current_url_for_httpx}): {e_page}")
                traceback.print_exc()
                break
    
    print(f"  [{thread_name}] Finished. Total Qs: {len(category_questions)} after {page_count} page(s).")
    return category_name, category_questions


# --- MODIFIED: Main Orchestrator (calls hybrid scraper) ---
def scrape_comparably_sync(
    company_base_url_str: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating HYBRID parallel category scrape for: {company_slug}")
    start_time_total = time.time()
    all_questions_for_company: List[Question] = []
    company_details_overall: Dict[str, Any] = {}
    initial_info_driver = None

    # Step 1: Fetch Initial Company Info (Selenium - unchanged for now, as it's once per company)
    try:
        print(f"  [{company_slug}] Fetching initial company info with Selenium...")
        initial_info_driver = setup_selenium_driver()
        # Try /reviews/ first, then base URL for company info
        info_fetch_url = urljoin(company_base_url_str.rstrip('/') + "/", "reviews/")
        initial_info_driver.get(info_fetch_url)
        time.sleep(1.5)
        if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title or "Access Denied" in initial_info_driver.page_source:
            print(f"  [{company_slug}] /reviews/ page for info failed (Title: {initial_info_driver.title}), trying base URL: {company_base_url_str}")
            initial_info_driver.get(company_base_url_str)
            time.sleep(1.5)
            if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title or "Access Denied" in initial_info_driver.page_source:
                raise Exception(f"Could not load a valid page for company info (Title: {initial_info_driver.title})")
        
        info_html = initial_info_driver.page_source
        info_soup = BeautifulSoup(info_html, 'html.parser')
        company_details_overall = extract_company_info(info_soup, company_base_url_str)
        print(f"  [{company_slug}] Initial company info fetched: Name='{company_details_overall.get('company_name')}'")
    except Exception as e_info:
        print(f"  [{company_slug}] Error fetching initial company info with Selenium: {e_info}")
        # traceback.print_exc() # Can be verbose
        company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": f"Initial info fetch error: {str(e_info)}"}
    finally:
        if initial_info_driver:
            try: initial_info_driver.quit()
            except Exception as e_close: print(f"  [{company_slug}] Error closing Selenium info browser: {e_close}")

    # Step 2: Scrape categories in parallel using the hybrid scraper
    max_concurrent_categories = min(len(REVIEW_CATEGORIES), 3) # Keep concurrency manageable
    print(f"  [{company_slug}] Starting HYBRID parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories, thread_name_prefix="HybridScraperPool") as executor:
        for category_name in REVIEW_CATEGORIES:
            future = executor.submit(
                _scrape_specific_category_all_pages_hybrid, # Use HYBRID version
                company_base_url_str, category_name, company_slug,
                start_date_filter, end_date_filter
            )
            futures_map[future] = category_name
        
        for future in as_completed(futures_map):
            original_category_name = futures_map[future]
            try:
                _, questions_from_category = future.result() # Result is (cat_name, questions_list)
                if questions_from_category:
                    print(f"  [{company_slug}] Received {len(questions_from_category)} Qs from cat '{original_category_name}'. Merging...")
                    all_questions_for_company.extend(questions_from_category) # Directly extend
                else:
                    print(f"  [{company_slug}] Cat '{original_category_name}' returned no new Qs.")
            except Exception as e_future_exc:
                print(f"  [{company_slug}] HYBRID Category scraping task for '{original_category_name}' FAILED in executor: {e_future_exc}")
                traceback.print_exc()

    total_duration = time.time() - start_time_total
    print(f"\nFinished ALL HYBRID parallel category scrapes for {company_slug} in {total_duration:.2f}s. Total Qs collected: {len(all_questions_for_company)}")
    
    # Ensure company_name is reasonable
    if not company_details_overall.get("company_name") or company_details_overall.get("company_name", "").lower() == company_slug.lower() or company_details_overall.get("company_name", "") == "unknown_company":
        current_name = company_details_overall.get("company_name", "unknown_company")
        fallback_name = company_slug.replace('-', ' ').title()
        if current_name.lower() in REVIEW_CATEGORIES or current_name == "unknown_company": # If current name is bad
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


# --- FastAPI Endpoint (largely unchanged, ensure it calls new orchestrator) ---
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
    tasks = []
    
    date_filter_msg = f" (Start: {request.start_date_str or 'N/A'}, End: {request.end_date_str or 'N/A'})"
    print(f"API request: {len(urls)} URLs, Hybrid Selenium/HTTPX & Date Filter{date_filter_msg} (v{app.version}).")

    # Create a list of (url_str, company_slug) tuples for task submission, handling bad URLs early
    valid_scrape_params = []
    for url_obj in urls:
        url_str = str(url_obj)
        try:
            parsed_url = urlparse(url_str)
            path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
            if not (parsed_url.scheme and parsed_url.netloc and len(path_segments) >= 2 and path_segments[0] == "companies"):
                raise ValueError("URL path error or incomplete URL")
            company_slug = path_segments[1]
            # Construct a canonical base URL for the company
            company_base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/companies/{company_slug}"
            valid_scrape_params.append({'original_url': url_str, 'base_url': company_base_url, 'slug': company_slug})
        except Exception as e_slug:
            print(f"Error parsing slug/URL '{url_str}': {e_slug}")
            results[url_str] = {"status": "error", "message": f"Invalid Comparably company URL format: {url_str}. Error: {e_slug}"}
            
    # Submit valid tasks
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
    else: # No valid tasks to run
        scraped_results_or_exceptions = []

    # Process results
    task_idx = 0
    for params in valid_scrape_params: # Iterate through what was submitted
        original_url_str = params['original_url']
        if original_url_str in results: continue # Already has an error from parsing

        if task_idx < len(scraped_results_or_exceptions):
            result_or_exc = scraped_results_or_exceptions[task_idx]
            if isinstance(result_or_exc, Exception):
                print(f"Task for {original_url_str} EXCEPTION (type: {type(result_or_exc).__name__}): {result_or_exc}")
                tb_str = "".join(traceback.format_exception(None, result_or_exc, result_or_exc.__traceback__))
                print(f"FULL TRACEBACK for {original_url_str} (Hybrid):\n{tb_str}")
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed. Type: {type(result_or_exc).__name__}. Check logs."}
            elif isinstance(result_or_exc, dict):
                results[original_url_str] = result_or_exc
            else:
                results[original_url_str] = {"status": "error", "message": "Unexpected internal result type from scraping task"}
            task_idx +=1
        else: # Should not happen if logic is correct
             results[original_url_str] = {"status": "error", "message": "Scraping task result missing."}


    print(f"Finished API request processing (Hybrid v{app.version}).")
    return results

# --- Health Check Endpoint ---
@app.get("/health")
async def health_check(): return {"status": "ok"}

# --- Main guard for uvicorn ---
# if __name__ == "__main__":
#     import uvicorn
#     # Note: Reload should be False in production or when debugging threading issues.
#     uvicorn.run("v3_hybrid:app", host="0.0.0.0", port=8000, reload=True)

