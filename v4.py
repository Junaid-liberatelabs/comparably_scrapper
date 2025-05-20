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

# --- Pydantic Models (Unchanged) ---
from pydantic import BaseModel, Field, HttpUrl, ValidationError
class Review(BaseModel): text: str; date: datetime
class ReviewSection(BaseModel): section_name: str; reviews: List[Review]
class Question(BaseModel): question_text: str; review_section: ReviewSection
class ScrapeRequest(BaseModel):
    urls: List[HttpUrl]
    start_date_str: Optional[str] = Field(None, description="YYYY-MM-DD")
    end_date_str: Optional[str] = Field(None, description="YYYY-MM-DD")

# --- FastAPI (Unchanged) ---
from fastapi import FastAPI, HTTPException, Body
app = FastAPI(title="Comparably Scraper API - Hybrid Selenium/cURL-cffi", version="1.7.0")

# --- Selenium, BeautifulSoup, cURL-cffi ---
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from curl_cffi.requests import AsyncSession as CurlAsyncSession # For async HTTP requests
from curl_cffi.requests import Session as CurlSyncSession # For sync HTTP requests

# --- User Agent (Unchanged) ---
try:
    from fake_useragent import UserAgent
    ua = UserAgent()
except ImportError: ua = None

# --- Constants (Unchanged) ---
REVIEW_CATEGORIES = ["leadership", "compensation", "team", "environment", "outlook"]
MAX_PAGES_PER_CATEGORY = 15 # Still a good safety net
SELENIUM_PAGE_TIMEOUT_S = 30
SELENIUM_ELEMENT_TIMEOUT_S = 18
CURL_REQUEST_TIMEOUT_S = 15 # Timeout for curl-cffi requests

# --- Helper Functions (Unchanged) ---
def extract_section_name_from_url(href: Optional[str]) -> str: # ... (same)
    if not href: return "unknown_section"
    try: path_parts = urlparse(href).path.strip('/').split('/');
    except Exception: pass
    if len(path_parts) >= 4 and path_parts[2] == 'reviews': return path_parts[3]
    match = re.search(r'/reviews/(\w+)', href); return match.group(1) if match else "unknown_section"

def parse_review_page_html(# ... (same)
    soup: BeautifulSoup, company_slug: str, current_category_for_context: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> List[Question]: # ... (same logic)
    questions: List[Question] = []; review_list_divs = soup.find_all('div', class_='reviewsList')
    if not review_list_divs: return []
    for review_list_div in review_list_divs:
        q_elem = review_list_div.find('h2', class_='section-subtitle');
        if not q_elem: continue
        question_text = q_elem.get_text(strip=True); section_name_to_use = current_category_for_context
        reviews_for_this_question: List[Review] = []
        review_blocks = review_list_div.find_all('div', class_='cppRH')
        for block in review_blocks:
            quote = block.find('p', class_='cppRH-review-quote');
            if not quote: continue
            text = quote.get_text(strip=True).replace('\u0000', '')
            cite_block = block.find('cite', class_='cppRH-review-cite'); date_meta = None
            if cite_block: date_meta = cite_block.find('meta', {'itemprop': 'datePublished'}) or cite_block.find('meta', attrs={'content': re.compile(r'^\d{4}-\d{2}-\d{2}$')})
            if not date_meta or not date_meta.get('content'): continue
            try: date_val = datetime.strptime(date_meta['content'], '%Y-%m-%d')
            except ValueError: continue
            if start_date_filter and date_val < start_date_filter: continue
            if end_date_filter and date_val > end_date_filter: continue
            reviews_for_this_question.append(Review(text=text, date=date_val))
        if not reviews_for_this_question: continue
        reviews_for_this_question.sort(key=lambda r: r.date, reverse=True)
        try: section = ReviewSection(section_name=section_name_to_use, reviews=reviews_for_this_question); questions.append(Question(question_text=question_text, review_section=section))
        except ValidationError as e: print(f"Pydantic validation error for '{question_text}' in cat '{current_category_for_context}': {e}")
    return questions

def extract_company_info(soup: BeautifulSoup, company_base_url_str: str) -> Dict: # ... (same)
    details = {};
    try: parsed_base_url = urlparse(str(company_base_url_str)); path_parts = parsed_base_url.path.strip('/').split('/'); company_slug_from_base = path_parts[1] if len(path_parts) > 1 else "unknown_company"; default_name = company_slug_from_base.replace('-', ' ').title(); details['company_name'] = default_name; details['comparably_url'] = str(company_base_url_str); name_tag_h1 = soup.find('h1')
    except Exception as e: print(f"Error extracting company details for {company_base_url_str}: {e}")
    if name_tag_h1: h1_text = name_tag_h1.get_text(strip=True);
    if " Reviews" in h1_text: name_candidate = h1_text.split(" Reviews")[0].strip();
    if name_candidate.lower() not in REVIEW_CATEGORIES and len(name_candidate) > 3: details['company_name'] = name_candidate
    if details.get('company_name', default_name) == default_name or details.get('company_name', "").lower() in REVIEW_CATEGORIES : title_tag = soup.find('title');
    if title_tag: title_text = title_tag.get_text(strip=True); name_from_title = title_text.split(" Reviews")[0].split(" | Comparably")[0].strip();
    if name_from_title and name_from_title != details.get('company_name') and len(name_from_title) > 3: details['company_name'] = name_from_title
    return details

# --- Selenium Setup (Unchanged) ---
def setup_selenium_driver() -> webdriver.Chrome: # ... (same)
    options = webdriver.ChromeOptions(); options.add_argument("--headless"); options.add_argument("--no-sandbox"); options.add_argument("--disable-dev-shm-usage"); options.add_argument("--disable-gpu"); options.add_argument("window-size=1920,1080"); user_agent_str = ua.random if ua else "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"; options.add_argument(f'user-agent={user_agent_str}'); options.add_experimental_option("excludeSwitches", ["enable-automation"]); options.add_experimental_option('useAutomationExtension', False);
    try: print(f"  [Selenium Setup] Initializing WebDriver..."); service = Service(ChromeDriverManager().install()); driver = webdriver.Chrome(service=service, options=options); driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"); driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT_S); print(f"  [Selenium Setup] WebDriver initialized."); return driver
    except Exception as e: print(f"  [Selenium Setup] CRITICAL ERROR: {e}"); traceback.print_exc(); raise RuntimeError(f"Failed to setup Selenium WebDriver: {e}")


# --- MODIFIED HELPER: Scrapes a single category with HYBRID Selenium/cURL-cffi ---
def _scrape_specific_category_all_pages_hybrid(
    company_base_url_str: str, category_name: str, company_slug: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    thread_name = f"HybridScraper-{category_name}-{company_slug[:10]}"
    print(f"  [{thread_name}] Started.")
    category_questions: List[Question] = []
    processed_reviews_in_this_category_run = set()
    driver = None # For initial page and fallback

    # --- Headers for curl-cffi to mimic a browser ---
    curl_headers = {
        "User-Agent": ua.random if ua else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin", # Assuming navigation from within the site
    }

    try:
        # Step 1: Use Selenium for the FIRST page of the category
        # This helps get initial cookies, discover pagination patterns, and parse page 1
        print(f"  [{thread_name}] Using Selenium for initial page of category '{category_name}'...")
        driver = setup_selenium_driver()
        category_wait = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S)
        category_short_wait = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S // 2)

        category_url_start = f"{company_base_url_str.rstrip('/')}/reviews/{category_name}/"
        driver.get(category_url_start)
        time.sleep(0.8)
        if "Error" in driver.title or "Not Found" in driver.title:
            print(f"  [{thread_name}] Error page via Selenium (Title: {driver.title}). Skipping category.")
            return category_name, []

        review_block_sel_css = "div.cppRH"
        try:
            category_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, review_block_sel_css)))
        except TimeoutException:
            print(f"  [{thread_name}] Selenium: Timeout waiting for review content on page 1. Category might be empty.")
            return category_name, [] # Empty category

        html_content_page1 = driver.page_source
        soup_page1 = BeautifulSoup(html_content_page1, 'html.parser')
        questions_page1 = parse_review_page_html(soup_page1, company_slug, category_name, start_date_filter, end_date_filter)
        
        # Process questions from page 1 (same merging logic)
        reviews_added_page1_count = 0
        if questions_page1:
            for q_page in questions_page1: # ... (same merging logic as before for category_questions and processed_reviews_in_this_category_run)
                existing_q_obj = next((q for q in category_questions if q.question_text == q_page.question_text), None)
                if not existing_q_obj:
                    unique_reviews = [];
                    for r_new in q_page.review_section.reviews: r_key = (hash(q_page.question_text), hash(r_new.text), r_new.date);
                    if r_key not in processed_reviews_in_this_category_run: unique_reviews.append(r_new); processed_reviews_in_this_category_run.add(r_key); reviews_added_page1_count +=1
                    if unique_reviews: q_page.review_section.reviews = unique_reviews; category_questions.append(q_page)
                else:
                    for r_new in q_page.review_section.reviews: r_key = (hash(existing_q_obj.question_text), hash(r_new.text), r_new.date);
                    if r_key not in processed_reviews_in_this_category_run: existing_q_obj.review_section.reviews.append(r_new); processed_reviews_in_this_category_run.add(r_key); reviews_added_page1_count += 1
                    existing_q_obj.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
            if reviews_added_page1_count > 0: print(f"  [{thread_name}] Selenium: Added {reviews_added_page1_count} unique reviews from page 1.")
        
        if not reviews_added_page1_count and not questions_page1: # No reviews at all on page 1
            print(f"  [{thread_name}] Selenium: No reviews found on page 1. Assuming category empty or fully filtered out.")
            return category_name, category_questions

        # Step 2: Discover total pages or if direct URL pattern exists for subsequent pages
        total_pages_in_category = 1
        pagination_links = driver.find_elements(By.CSS_SELECTOR, "ul.pagination-list a.pagination-link")
        if pagination_links:
            page_numbers = [int(link.text) for link in pagination_links if link.text.isdigit()]
            if page_numbers:
                total_pages_in_category = max(page_numbers)
                print(f"  [{thread_name}] Selenium: Discovered ~{total_pages_in_category} pages for category '{category_name}'.")
            else: # Fallback if only next/prev found, try to find last page link
                last_page_link = driver.find_elements(By.CSS_SELECTOR, "ul.pagination-list > li:nth-last-child(2) > a.pagination-link") # Second to last is often last numbered page
                if last_page_link and last_page_link[0].text.isdigit():
                    total_pages_in_category = int(last_page_link[0].text)
                    print(f"  [{thread_name}] Selenium: Discovered ~{total_pages_in_category} pages (fallback) for '{category_name}'.")


        # Ensure Selenium driver is quit before starting curl-cffi loop to free resources
        if driver: driver.quit(); driver = None; print(f"  [{thread_name}] Selenium driver (page 1) quit.")

        # Step 3: Use curl-cffi for pages 2 to N if total_pages > 1
        if total_pages_in_category > 1:
            print(f"  [{thread_name}] Using curl-cffi for pages 2 to {min(total_pages_in_category, MAX_PAGES_PER_CATEGORY)}...")
            with CurlSyncSession() as curl_session: # Use a session for potential cookie persistence (though not explicitly handled here)
                for page_num in range(2, min(total_pages_in_category, MAX_PAGES_PER_CATEGORY) + 1):
                    page_url = f"{category_url_start}?page={page_num}" # Assumes ?page=N pattern
                    print(f"    [{thread_name}] curl-cffi: Fetching page {page_num} URL: {page_url}")
                    try:
                        response = curl_session.get(page_url, headers=curl_headers, impersonate="chrome110", timeout=CURL_REQUEST_TIMEOUT_S)
                        response.raise_for_status() # Check for HTTP errors
                        html_content_curl = response.text
                        soup_curl = BeautifulSoup(html_content_curl, 'html.parser')
                        questions_curl_page = parse_review_page_html(soup_curl, company_slug, category_name, start_date_filter, end_date_filter)
                        
                        reviews_added_curl_count = 0
                        if questions_curl_page: # ... (same merging logic as for page 1)
                            for q_page in questions_curl_page: # ... (merge logic)
                                existing_q_obj = next((q for q in category_questions if q.question_text == q_page.question_text), None)
                                if not existing_q_obj:
                                    unique_reviews = [];
                                    for r_new in q_page.review_section.reviews: r_key = (hash(q_page.question_text), hash(r_new.text), r_new.date);
                                    if r_key not in processed_reviews_in_this_category_run: unique_reviews.append(r_new); processed_reviews_in_this_category_run.add(r_key); reviews_added_curl_count +=1
                                    if unique_reviews: q_page.review_section.reviews = unique_reviews; category_questions.append(q_page)
                                else:
                                    for r_new in q_page.review_section.reviews: r_key = (hash(existing_q_obj.question_text), hash(r_new.text), r_new.date);
                                    if r_key not in processed_reviews_in_this_category_run: existing_q_obj.review_section.reviews.append(r_new); processed_reviews_in_this_category_run.add(r_key); reviews_added_curl_count += 1
                                    existing_q_obj.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
                            if reviews_added_curl_count > 0: print(f"    [{thread_name}] curl-cffi: Added {reviews_added_curl_count} unique reviews from page {page_num}.")
                        
                        if not reviews_added_curl_count and not questions_curl_page:
                            print(f"    [{thread_name}] curl-cffi: No reviews found/matched on page {page_num}. Assuming end for this category.")
                            break # Stop if a curl-cffi page returns nothing

                        time.sleep(random.uniform(0.2, 0.7)) # Small delay between curl requests
                    except Exception as e_curl:
                        print(f"    [{thread_name}] curl-cffi: Error fetching page {page_num} ({page_url}): {e_curl}. Stopping for this category.")
                        # Fallback to Selenium click might be an option here if critical, but adds complexity
                        break
        
        print(f"  [{thread_name}] Finished. Found {len(category_questions)} question sections for it.")
        return category_name, category_questions

    except Exception as e_cat_overall:
        print(f"  [{thread_name}] CRITICAL ERROR during HYBRID category processing: {e_cat_overall}")
        traceback.print_exc()
        return category_name, category_questions # Return what was collected
    finally:
        if driver: # Ensure driver is quit if an error happened before its planned quit
            driver.quit()
            print(f"  [{thread_name}] Final Selenium WebDriver quit (if fallback/error).")


# --- Main Orchestrator (MODIFIED to call hybrid function) ---
def scrape_comparably_sync(
    company_base_url_str: str, company_slug: str,
    start_date_filter: Optional[datetime] = None, end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating HYBRID Selenium/cURL-cffi scrape for: {company_slug}")
    start_time_total = time.time(); all_questions_for_company: List[Question] = []
    company_details_overall: Dict[str, Any] = {}
    initial_info_driver = None

    # Step 1: Fetch Initial Company Info (still with Selenium for robustness)
    try:
        print(f"  [{company_slug}] Fetching initial company info with Selenium..."); initial_info_driver = setup_selenium_driver()
        info_fetch_url = f"{company_base_url_str.rstrip('/')}/reviews/"; initial_info_driver.get(info_fetch_url)
        time.sleep(1.5);
        if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title:
            print(f"  [{company_slug}] /reviews/ page error, trying base: {company_base_url_str}"); initial_info_driver.get(company_base_url_str)
            time.sleep(1.5);
            if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title: raise Exception("Could not load valid info page.")
        info_html = initial_info_driver.page_source; info_soup = BeautifulSoup(info_html, 'html.parser'); company_details_overall = extract_company_info(info_soup, company_base_url_str)
        print(f"  [{company_slug}] Initial info fetched: {company_details_overall.get('company_name')}")
    except Exception as e_info_sel:
        print(f"  [{company_slug}] Error initial info fetch: {e_info_sel}"); traceback.print_exc()
        company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": f"Initial info fetch error: {str(e_info_sel)}"}
    finally:
        if initial_info_driver:
            try: initial_info_driver.quit()
            except Exception as e_close_info: print(f"  [{company_slug}] Error closing info browser: {e_close_info}")

    # Step 2: Scrape categories in parallel using the HYBRID method
    max_concurrent_categories = min(len(REVIEW_CATEGORIES), 3) # Each thread starts a brief Selenium then potentially many curl calls
    print(f"  [{company_slug}] Starting HYBRID parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories) as executor:
        for category_name in REVIEW_CATEGORIES:
            future = executor.submit(_scrape_specific_category_all_pages_hybrid, company_base_url_str, category_name, company_slug, start_date_filter, end_date_filter)
            futures_map[future] = category_name
        for future in as_completed(futures_map):
            original_category_name = futures_map[future]
            try:
                _, questions_from_category = future.result()
                if questions_from_category: print(f"  [{company_slug}] Received {len(questions_from_category)} Qs from cat '{original_category_name}'. Merging..."); all_questions_for_company.extend(questions_from_category)
                else: print(f"  [{company_slug}] Cat '{original_category_name}' returned no Qs.")
            except Exception as e_future: print(f"  [{company_slug}] HYBRID Category scraping task for '{original_category_name}' FAILED: {e_future}"); traceback.print_exc()
    
    total_duration = time.time() - start_time_total
    print(f"\nFinished ALL HYBRID scrapes for {company_slug} in {total_duration:.2f}s. Total Qs: {len(all_questions_for_company)}")
    if not company_details_overall.get("company_name") or company_details_overall.get("company_name", "").lower() == company_slug.lower() or company_details_overall.get("company_name", "") == "unknown_company":
        company_details_overall["company_name"] = company_slug.replace('-', ' ').title()
        if "status_note" not in company_details_overall: company_details_overall["status_note"] = "Name set to fallback."
    return {"status": "success" if all_questions_for_company or company_details_overall.get("company_name") != company_slug.replace('-', ' ').title() else "partial_success_no_reviews", "data": {"company_info": company_details_overall, "reviews": [q.model_dump(mode='json') for q in all_questions_for_company]}}


# --- FastAPI Endpoint (Unchanged from v1.6.0 - logic for calling orchestrator is same) ---
@app.post("/scrape")
async def scrape_companies(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    # ... (same as previous version: date parsing, task creation for scrape_comparably_sync, result aggregation) ...
    urls = request.urls; start_date_filter: Optional[datetime] = None; end_date_filter: Optional[datetime] = None
    if request.start_date_str:
        try: start_date_filter = datetime.strptime(request.start_date_str, "%Y-%m-%d")
        except ValueError: raise HTTPException(status_code=400, detail="Invalid start_date_str. Use YYYY-MM-DD.")
    if request.end_date_str:
        try: end_date_filter = datetime.strptime(request.end_date_str, "%Y-%m-%d"); end_date_filter = end_date_filter.replace(hour=23, minute=59, second=59)
        except ValueError: raise HTTPException(status_code=400, detail="Invalid end_date_str. Use YYYY-MM-DD.")
    if start_date_filter and end_date_filter and start_date_filter > end_date_filter: raise HTTPException(status_code=400, detail="start_date_str after end_date_str.")
    if not urls: raise HTTPException(status_code=400, detail="No URLs provided.")
    results: Dict[str, Dict[str, Any]] = {}; tasks = []
    date_filter_msg = f" (Start: {request.start_date_str or 'N/A'}, End: {request.end_date_str or 'N/A'})"
    print(f"API request: {len(urls)} URLs, Hybrid Selenium/cURL-cffi & Date Filter{date_filter_msg} (v1.7.0).")
    for url_obj in urls:
        url_str = str(url_obj)
        try:
            parsed_url = urlparse(url_str); path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
            if len(path_segments) >= 2 and path_segments[0] == "companies": company_slug = path_segments[1]
            else: raise ValueError("URL path error")
        except Exception as e_slug: print(f"Error parsing slug from URL '{url_str}': {e_slug}"); results[url_str] = {"status": "error", "message": f"Invalid Comparably company URL format: {url_str}"}; continue
        tasks.append(asyncio.to_thread(scrape_comparably_sync, url_str, company_slug, start_date_filter, end_date_filter))
    scraped_results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, url_obj in enumerate(urls):
        url_str = str(url_obj)
        if url_str in results: continue
        result_or_exc = scraped_results[i]
        if isinstance(result_or_exc, Exception):
            print(f"Task for {url_str} EXCEPTION (type: {type(result_or_exc).__name__}): {result_or_exc}")
            exc_type, exc_value, exc_traceback_obj = sys.exc_info()
            tb_str = "No traceback available."
            if result_or_exc.__traceback__: tb_str = "".join(traceback.format_exception(None, result_or_exc, result_or_exc.__traceback__))
            elif exc_traceback_obj : tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback_obj))
            print(f"FULL TRACEBACK for {url_str} (Hybrid):\n{tb_str}")
            results[url_str] = {"status": "error", "message": f"Scraping task failed. Type: {type(result_or_exc).__name__}, Repr: {repr(result_or_exc)}. Check logs."}
        elif isinstance(result_or_exc, dict): results[url_str] = result_or_exc
        else: results[url_str] = {"status": "error", "message": "Unexpected internal result type"}
    print(f"Finished API request processing (Hybrid v1.7.0).")
    return results

# --- Health Check Endpoint (Unchanged) ---
@app.get("/health")
async def health_check(): return {"status": "ok"}

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)