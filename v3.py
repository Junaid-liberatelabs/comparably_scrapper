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
    title="Comparably Scraper API with Selenium & Date Filter",
    description="API to scrape company reviews from Comparably using Selenium, with date filtering.",
    version="1.6.0" # Version bump for Selenium revert
)

# --- Selenium and BeautifulSoup ---
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager # For Selenium

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
    # ... (This function was already fine and doesn't depend on Selenium/Playwright directly) ...
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
    # ... (This function was already fine) ...
    details = {}
    try:
        parsed_base_url = urlparse(str(company_base_url_str)); path_parts = parsed_base_url.path.strip('/').split('/')
        company_slug_from_base = path_parts[1] if len(path_parts) > 1 else "unknown_company"
        default_name = company_slug_from_base.replace('-', ' ').title(); details['company_name'] = default_name
        details['comparably_url'] = str(company_base_url_str)
        name_tag_h1 = soup.find('h1')
        if name_tag_h1:
            h1_text = name_tag_h1.get_text(strip=True)
            if " Reviews" in h1_text: name_candidate = h1_text.split(" Reviews")[0].strip()
            if name_candidate.lower() not in REVIEW_CATEGORIES and len(name_candidate) > 3: details['company_name'] = name_candidate
        if details['company_name'] == default_name or details['company_name'].lower() in REVIEW_CATEGORIES :
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text(strip=True); name_from_title = title_text.split(" Reviews")[0].split(" | Comparably")[0].strip()
                if name_from_title and name_from_title != details['company_name'] and len(name_from_title) > 3: details['company_name'] = name_from_title
    except Exception as e: print(f"Error extracting company details for {company_base_url_str}: {e}")
    return details


# --- MODIFIED: Selenium Setup ---
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
        print(f"  [Selenium Setup] Initializing WebDriver with ChromeDriverManager...")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.set_page_load_timeout(SELENIUM_PAGE_TIMEOUT_S)
        print(f"  [Selenium Setup] WebDriver initialized.")
        return driver
    except Exception as e:
        print(f"  [Selenium Setup] CRITICAL ERROR during Selenium setup: {e}")
        traceback.print_exc()
        raise RuntimeError(f"Failed to setup Selenium WebDriver: {e}")


# --- MODIFIED HELPER: Scrapes a single category with Selenium ---
def _scrape_specific_category_all_pages_selenium(
    company_base_url_str: str,
    category_name: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    thread_name = f"SeleniumCatScraper-{category_name}-{company_slug[:10]}"
    print(f"  [{thread_name}] Started.")
    category_questions: List[Question] = []
    processed_reviews_in_this_category_run = set()
    driver = None # Initialize driver to None

    try:
        driver = setup_selenium_driver() # Use Selenium setup
        category_wait = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S)
        category_short_wait = WebDriverWait(driver, SELENIUM_ELEMENT_TIMEOUT_S // 2)

        category_url_start = f"{company_base_url_str.rstrip('/')}/reviews/{category_name}/"
        print(f"  [{thread_name}] Navigating to initial page: {category_url_start}")
        driver.get(category_url_start)

        time.sleep(0.8)
        if "Error" in driver.title or "Not Found" in driver.title:
            print(f"  [{thread_name}] Error page detected (Title: {driver.title}). Skipping category.")
            return category_name, []

        page_count = 0
        while page_count < MAX_PAGES_PER_CATEGORY:
            page_count += 1
            current_page_url_in_cat = driver.current_url
            print(f"  [{thread_name}] Scraping its page {page_count} (URL: {current_page_url_in_cat})")

            review_block_sel_css = "div.cppRH" # CSS selector
            try:
                category_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, review_block_sel_css)))
            except TimeoutException:
                print(f"  [{thread_name}] Timeout waiting for review content on its page {page_count}.")
                if page_count == 1: print(f"  [{thread_name}] Appears empty or inaccessible.")
                break

            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            questions_this_page = parse_review_page_html(soup, company_slug, category_name, start_date_filter, end_date_filter)
            # ... (Merging logic for questions_this_page and category_questions - same as Playwright version's logic) ...
            reviews_added_this_page_count = 0
            if questions_this_page:
                for q_page in questions_this_page:
                    existing_q_obj = next((q for q in category_questions if q.question_text == q_page.question_text), None)
                    if not existing_q_obj:
                        unique_reviews = []
                        for r_new in q_page.review_section.reviews:
                            r_key = (hash(q_page.question_text), hash(r_new.text), r_new.date)
                            if r_key not in processed_reviews_in_this_category_run: unique_reviews.append(r_new); processed_reviews_in_this_category_run.add(r_key); reviews_added_this_page_count +=1
                        if unique_reviews: q_page.review_section.reviews = unique_reviews; category_questions.append(q_page)
                    else:
                        for r_new in q_page.review_section.reviews:
                            r_key = (hash(existing_q_obj.question_text), hash(r_new.text), r_new.date)
                            if r_key not in processed_reviews_in_this_category_run: existing_q_obj.review_section.reviews.append(r_new); processed_reviews_in_this_category_run.add(r_key); reviews_added_this_page_count += 1
                        existing_q_obj.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
                if reviews_added_this_page_count > 0: print(f"  [{thread_name}] Added {reviews_added_this_page_count} unique reviews from its page {page_count}.")
            else:
                print(f"  [{thread_name}] No review questions parsed/matched from its page {page_count}.")
                try: driver.find_element(By.CSS_SELECTOR, review_block_sel_css) # Check if containers are still there
                except NoSuchElementException: # No containers at all
                    if page_count > 1 : print(f"  [{thread_name}] No review containers found, likely end."); break
                    if page_count == 1: print(f"  [{thread_name}] Seems empty (first page)."); break
            
            # Selenium Pagination Click Logic
            next_page_button_element = None
            try:
                selectors_for_next = [ "a.pagination-link[rel='next']", "a[aria-label='Next Page']", "a[title='Next Page']", "li.pagination-next > a", "a.pagination-next", "nav[aria-label*='pagination'] li:last-child a[href]" ]
                for sel in selectors_for_next:
                    try:
                        candidate_buttons = driver.find_elements(By.CSS_SELECTOR, sel)
                        for btn_candidate in candidate_buttons:
                            if btn_candidate.is_displayed():
                                aria_label = btn_candidate.get_attribute("aria-label") or ""
                                rel_attr = btn_candidate.get_attribute("rel") or ""
                                if "prev" not in aria_label.lower() and "prev" not in rel_attr.lower():
                                    # Use category_short_wait for element_to_be_clickable
                                    next_page_button_element = category_short_wait.until(EC.element_to_be_clickable(btn_candidate))
                                    if next_page_button_element:
                                        print(f"  [{thread_name}] Found 'Next Page' button with selector: '{sel}'")
                                        break
                        if next_page_button_element: break
                    except (NoSuchElementException, TimeoutException): continue # Try next selector
                
                if not next_page_button_element: # or not next_page_button_element.is_displayed(): # is_displayed already checked
                    print(f"  [{thread_name}] No clickable 'Next Page' button found after its page {page_count}. End of category.")
                    break

                print(f"  [{thread_name}] Attempting to click 'Next Page'...")
                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_page_button_element)
                time.sleep(0.5)
                try:
                    next_page_button_element.click()
                except ElementClickInterceptedException:
                    print(f"  [{thread_name}] Click intercepted, trying JS click for 'Next Page'...")
                    driver.execute_script("arguments[0].click();", next_page_button_element)
                
                time.sleep(random.uniform(2.0, 3.5)) # Wait for page transition
            except (NoSuchElementException, TimeoutException) as e_timeout_pag:
                print(f"  [{thread_name}] Timeout/Error in Selenium pagination (Page {page_count}): {e_timeout_pag}. End of category.")
                break
            except Exception as e_pag:
                print(f"  [{thread_name}] Selenium Pagination error (Page {page_count}): {type(e_pag).__name__} - {e_pag}. End of category.")
                traceback.print_exc()
                break
        # End of while loop for pages
        print(f"  [{thread_name}] Finished. Found {len(category_questions)} question sections for it.")
        return category_name, category_questions
    except Exception as e_cat_page_overall:
        print(f"  [{thread_name}] CRITICAL ERROR during Selenium category page processing: {e_cat_page_overall}")
        traceback.print_exc()
        return category_name, category_questions # Return what was collected so far
    finally:
        if driver:
            driver.quit()
            print(f"  [{thread_name}] Selenium WebDriver quit.")

# --- MODIFIED: Main Orchestrator for a single company with Selenium ---
def scrape_comparably_sync(
    company_base_url_str: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating SELENIUM parallel category scrape for: {company_slug}")
    start_time_total = time.time()
    all_questions_for_company: List[Question] = []
    company_details_overall: Dict[str, Any] = {}
    initial_info_driver = None # Initialize

    # Step 1: Fetch Initial Company Info (once, using Selenium)
    try:
        print(f"  [{company_slug}] Fetching initial company info with Selenium...")
        initial_info_driver = setup_selenium_driver()
        info_fetch_url = f"{company_base_url_str.rstrip('/')}/reviews/"
        initial_info_driver.get(info_fetch_url)
        time.sleep(1.5)
        if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title:
            print(f"  [{company_slug}] /reviews/ page for info failed, trying base URL: {company_base_url_str}")
            initial_info_driver.get(company_base_url_str)
            time.sleep(1.5)
            if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title:
                raise Exception("Could not load a valid page for company info.")
        info_html = initial_info_driver.page_source
        info_soup = BeautifulSoup(info_html, 'html.parser')
        company_details_overall = extract_company_info(info_soup, company_base_url_str)
        print(f"  [{company_slug}] Initial company info fetched: {company_details_overall.get('company_name')}")
    except Exception as e_info_sel:
        print(f"  [{company_slug}] Error fetching initial company info with Selenium: {e_info_sel}")
        traceback.print_exc()
        company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": f"Initial info fetch error (Selenium): {str(e_info_sel)}"}
    finally:
        if initial_info_driver:
            try: initial_info_driver.quit()
            except Exception as e_close_info: print(f"  [{company_slug}] Error closing Selenium info browser: {e_close_info}")

    # Step 2: Scrape categories in parallel
    max_concurrent_categories = min(len(REVIEW_CATEGORIES), 3)
    print(f"  [{company_slug}] Starting SELENIUM parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories) as executor:
        for category_name in REVIEW_CATEGORIES:
            future = executor.submit(
                _scrape_specific_category_all_pages_selenium, # Use Selenium version
                company_base_url_str, category_name, company_slug,
                start_date_filter, end_date_filter
            )
            futures_map[future] = category_name
        for future in as_completed(futures_map):
            original_category_name = futures_map[future]
            try:
                _, questions_from_category = future.result()
                if questions_from_category:
                    print(f"  [{company_slug}] Received {len(questions_from_category)} Qs from cat '{original_category_name}'. Merging...")
                    all_questions_for_company.extend(questions_from_category)
                else:
                    print(f"  [{company_slug}] Cat '{original_category_name}' returned no Qs (or none matched date filter).")
            except Exception as e_future:
                print(f"  [{company_slug}] SELENIUM Category scraping task for '{original_category_name}' FAILED: {e_future}")
                traceback.print_exc()

    total_duration = time.time() - start_time_total
    print(f"\nFinished ALL SELENIUM parallel category scrapes for {company_slug} in {total_duration:.2f}s. Total Qs: {len(all_questions_for_company)}")
    if not company_details_overall.get("company_name") or company_details_overall.get("company_name", "").lower() == company_slug.lower() or company_details_overall.get("company_name", "") == "unknown_company":
        company_details_overall["company_name"] = company_slug.replace('-', ' ').title()
        if "status_note" not in company_details_overall: company_details_overall["status_note"] = "Name set to fallback."
    return {"status": "success" if all_questions_for_company or company_details_overall.get("company_name") != company_slug.replace('-', ' ').title() else "partial_success_no_reviews", "data": {"company_info": company_details_overall, "reviews": [q.model_dump(mode='json') for q in all_questions_for_company]}}


# --- FastAPI Endpoint (Unchanged from v1.5.3 - logic for calling orchestrator is same) ---
@app.post("/scrape")
async def scrape_companies(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
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
    print(f"API request: {len(urls)} URLs, Selenium & Date Filter{date_filter_msg} (v1.6.0).")
    for url_obj in urls:
        url_str = str(url_obj)
        try:
            parsed_url = urlparse(url_str); path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
            if len(path_segments) >= 2 and path_segments[0] == "companies": company_slug = path_segments[1]
            else: raise ValueError("URL path error")
        except Exception as e_slug: print(f"Error parsing slug from URL '{url_str}': {e_slug}"); results[url_str] = {"status": "error", "message": f"Invalid Comparably company URL format: {url_str}"}; continue
        tasks.append(asyncio.to_thread(scrape_comparably_sync, url_str, company_slug, start_date_filter, end_date_filter)) # scrape_comparably_sync now uses Selenium
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
            print(f"FULL TRACEBACK for {url_str} (Selenium):\n{tb_str}")
            results[url_str] = {"status": "error", "message": f"Scraping task failed. Type: {type(result_or_exc).__name__}, Repr: {repr(result_or_exc)}. Check logs."}
        elif isinstance(result_or_exc, dict): results[url_str] = result_or_exc
        else: results[url_str] = {"status": "error", "message": "Unexpected internal result type"}
    print(f"Finished API request processing (Selenium v1.6.0).")
    return results

# --- Health Check Endpoint ---
@app.get("/health")
async def health_check(): return {"status": "ok"}

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)