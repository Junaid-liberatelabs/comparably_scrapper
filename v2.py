import os
import json
import re
import time
import random
import asyncio
from typing import List, Dict, Optional, Any, Tuple # Added Tuple
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed # For parallel category scraping

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

# --- FastAPI ---
from fastapi import FastAPI, HTTPException, Body
app = FastAPI(
    title="Comparably Scraper API",
    description="API to scrape company reviews (multi-category, multi-page, parallel categories) from Comparably.",
    version="1.4.0" # Version bump
)

# --- Selenium and BeautifulSoup ---
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
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

# --- Constants ---
REVIEW_CATEGORIES = ["leadership", "compensation", "team", "environment", "outlook"]
MAX_PAGES_PER_CATEGORY = 15 # Safety limit for pagination

# --- Helper: Extract Section Name ---
def extract_section_name_from_url(href: Optional[str]) -> str:
    if not href: return "unknown_section"
    try:
        path_parts = urlparse(href).path.strip('/').split('/')
        if len(path_parts) >= 4 and path_parts[2] == 'reviews': return path_parts[3]
    except Exception: pass
    match = re.search(r'/reviews/(\w+)', href)
    return match.group(1) if match else "unknown_section"

# --- Integrated Parsing Logic ---
def parse_review_page_html(soup: BeautifulSoup, company_slug: str, current_category_for_context: str) -> List[Question]:
    questions: List[Question] = []
    review_list_divs = soup.find_all('div', class_='reviewsList')
    if not review_list_divs: return []

    for review_list_div in review_list_divs:
        q_elem = review_list_div.find('h2', class_='section-subtitle')
        if not q_elem: continue
        question_text = q_elem.get_text(strip=True)

        # Use the current_category_for_context as the primary section_name
        # The link in HTML might be for a sub-section or could be missing
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
            reviews_for_this_question.append(Review(text=text, date=date_val))

        if not reviews_for_this_question: continue
        reviews_for_this_question.sort(key=lambda r: r.date, reverse=True)
        try:
            section = ReviewSection(section_name=section_name_to_use, reviews=reviews_for_this_question)
            questions.append(Question(question_text=question_text, review_section=section))
        except ValidationError as e:
            print(f"Pydantic validation error creating Question for '{question_text}' in category '{current_category_for_context}': {e}")
    return questions

# --- Function to Extract Basic Company Info ---
def extract_company_info(soup: BeautifulSoup, company_base_url_str: str) -> Dict:
    details = {}
    try:
        parsed_base_url = urlparse(str(company_base_url_str))
        path_parts = parsed_base_url.path.strip('/').split('/')
        company_slug_from_base = path_parts[1] if len(path_parts) > 1 else "unknown_company"
        default_name = company_slug_from_base.replace('-', ' ').title()
        details['company_name'] = default_name
        details['comparably_url'] = str(company_base_url_str)
        name_tag_h1 = soup.find('h1')
        if name_tag_h1:
            h1_text = name_tag_h1.get_text(strip=True)
            if " Reviews" in h1_text:
                name_candidate = h1_text.split(" Reviews")[0].strip()
                if name_candidate.lower() not in REVIEW_CATEGORIES and len(name_candidate) > 3:
                    details['company_name'] = name_candidate
        if details['company_name'] == default_name or details['company_name'].lower() in REVIEW_CATEGORIES:
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                name_from_title = title_text.split(" Reviews")[0].split(" | Comparably")[0].strip()
                if name_from_title and name_from_title != details['company_name'] and len(name_from_title) > 3:
                    details['company_name'] = name_from_title
    except Exception as e:
        print(f"Error extracting company details (URL: {company_base_url_str}): {e}")
    return details

# --- Selenium Setup ---
def setup_driver():
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
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', { 'source': ''' ... ''' }) # Keep anti-detection
        return driver
    except Exception as e:
        print(f"Error setting up WebDriver: {e}")
        raise RuntimeError(f"Failed to setup Selenium WebDriver: {e}")

# --- NEW HELPER: Scrapes a single category and all its pages ---
def _scrape_specific_category_all_pages(
    company_base_url_str: str,
    category_name: str,
    company_slug: str
) -> Tuple[str, List[Question]]:
    """
    Scrapes all pages for a single review category. Creates its own WebDriver.
    Returns (category_name, list_of_questions_for_this_category).
    """
    thread_name = f"CatScraper-{category_name}-{company_slug[:10]}" # For logging
    print(f"  [{thread_name}] Started.")
    category_driver = None
    category_questions: List[Question] = []
    # This tracker is for reviews *within this specific category task*
    processed_reviews_in_this_category_run = set() # (question_text_hash, review_text_hash, date)

    try:
        category_driver = setup_driver()
        category_wait = WebDriverWait(category_driver, 18) # Wait for elements in this category
        category_short_wait = WebDriverWait(category_driver, 8) # Shorter wait for buttons

        category_url_start = f"{company_base_url_str.rstrip('/')}/reviews/{category_name}/"
        print(f"  [{thread_name}] Navigating to initial page: {category_url_start}")
        category_driver.get(category_url_start)

        time.sleep(0.8)
        if "Error" in category_driver.title or "Not Found" in category_driver.title:
            print(f"  [{thread_name}] Error page detected. Skipping category.")
            return category_name, []

        page_count = 0
        while page_count < MAX_PAGES_PER_CATEGORY:
            page_count += 1
            current_page_url_in_cat = category_driver.current_url
            print(f"  [{thread_name}] Scraping its page {page_count} (URL: {current_page_url_in_cat})")

            review_block_sel = "div.cppRH"
            try:
                category_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, review_block_sel)))
            except TimeoutException:
                print(f"  [{thread_name}] Timeout waiting for review content on its page {page_count}.")
                if page_count == 1: print(f"  [{thread_name}] Appears empty or inaccessible.")
                break

            soup = BeautifulSoup(category_driver.page_source, 'html.parser')
            # Pass current_category_name for context to parser
            questions_this_page = parse_review_page_html(soup, company_slug, category_name)

            reviews_added_this_page_count = 0
            if questions_this_page:
                for q_page in questions_this_page:
                    existing_q_obj_for_category = next((q for q in category_questions if q.question_text == q_page.question_text), None)
                    if not existing_q_obj_for_category:
                        unique_reviews = []
                        for r_new in q_page.review_section.reviews:
                            r_key = (hash(q_page.question_text), hash(r_new.text), r_new.date)
                            if r_key not in processed_reviews_in_this_category_run:
                                unique_reviews.append(r_new)
                                processed_reviews_in_this_category_run.add(r_key)
                                reviews_added_this_page_count +=1
                        if unique_reviews:
                            q_page.review_section.reviews = unique_reviews
                            category_questions.append(q_page)
                    else:
                        for r_new in q_page.review_section.reviews:
                            r_key = (hash(existing_q_obj_for_category.question_text), hash(r_new.text), r_new.date)
                            if r_key not in processed_reviews_in_this_category_run:
                                existing_q_obj_for_category.review_section.reviews.append(r_new)
                                processed_reviews_in_this_category_run.add(r_key)
                                reviews_added_this_page_count += 1
                        existing_q_obj_for_category.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
                if reviews_added_this_page_count > 0:
                     print(f"  [{thread_name}] Added {reviews_added_this_page_count} unique reviews from its page {page_count}.")
            else:
                print(f"  [{thread_name}] No review questions parsed from its page {page_count}.")
                if page_count == 1: print(f"  [{thread_name}] Seems empty (first page).")
                break

            # Pagination Click Logic (using category_driver)
            next_page_button_element = None
            try:
                selectors_for_next = [ "a.pagination-link[rel='next']", "a[aria-label='Next Page']", "a[title='Next Page']", "li.pagination-next > a", "a.pagination-next", "nav[aria-label*='pagination'] li:last-child a[href]" ]
                for sel in selectors_for_next:
                    try:
                        candidate_buttons = category_driver.find_elements(By.CSS_SELECTOR, sel)
                        for btn in candidate_buttons:
                            if btn.is_displayed() and ("prev" not in (btn.get_attribute("aria-label") or "").lower() and "prev" not in (btn.get_attribute("rel") or "").lower()):
                                next_page_button_element = category_short_wait.until(EC.element_to_be_clickable(btn))
                                if next_page_button_element: break
                        if next_page_button_element: break
                    except (NoSuchElementException, TimeoutException): continue
                if not next_page_button_element:
                    print(f"  [{thread_name}] No clickable 'Next Page' button found after its page {page_count}. End of category.")
                    break
                print(f"  [{thread_name}] Attempting to click 'Next Page'...")
                category_driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_page_button_element)
                time.sleep(0.5)
                try: next_page_button_element.click()
                except ElementClickInterceptedException:
                    print(f"  [{thread_name}] Click intercepted, trying JS click for 'Next Page'...")
                    category_driver.execute_script("arguments[0].click();", next_page_button_element)
                time.sleep(random.uniform(2.0, 3.5))
            except (NoSuchElementException, TimeoutException) as e_pag:
                print(f"  [{thread_name}] Pagination error after its page {page_count} (Error: {type(e_pag).__name__}). End of category.")
                break
        # End of while loop for pages in this category
        print(f"  [{thread_name}] Finished. Found {len(category_questions)} question sections for it.")
        return category_name, category_questions
    except Exception as e_cat:
        print(f"  [{thread_name}] ERROR: {e_cat}")
        import traceback
        traceback.print_exc()
        return category_name, [] # Return empty on error to not break aggregation
    finally:
        if category_driver:
            category_driver.quit()
            print(f"  [{thread_name}] WebDriver quit.")

# --- MODIFIED: Main Orchestrator for a single company ---
def scrape_comparably_sync(company_base_url_str: str, company_slug: str) -> Dict[str, Any]:
    print(f"Orchestrating parallel category scrape for: {company_slug}")
    initial_info_driver = None
    start_time_total = time.time()

    all_questions_for_company: List[Question] = []
    company_details_overall: Dict[str, Any] = {}

    # Step 1: Fetch Initial Company Info (once, before parallel category scraping)
    try:
        print(f"  [{company_slug}] Fetching initial company info...")
        initial_info_driver = setup_driver()
        # Use a reliable page for info, e.g., the base reviews URL or first category
        info_fetch_url = f"{company_base_url_str.rstrip('/')}/reviews/" # General reviews page
        initial_info_driver.get(info_fetch_url)
        time.sleep(1.5) # Allow page to load for info extraction
        if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title:
            # Try the base company URL if /reviews/ fails
            print(f"  [{company_slug}] /reviews/ page for info failed, trying base URL: {company_base_url_str}")
            initial_info_driver.get(company_base_url_str)
            time.sleep(1.5)
            if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title:
                raise Exception("Could not load a valid page for company info.")
        info_soup = BeautifulSoup(initial_info_driver.page_source, 'html.parser')
        company_details_overall = extract_company_info(info_soup, company_base_url_str)
        print(f"  [{company_slug}] Initial company info fetched: {company_details_overall.get('company_name')}")
    except Exception as e_info:
        print(f"  [{company_slug}] Error fetching initial company info: {e_info}. Proceeding without detailed info.")
        company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": f"Initial info fetch error: {e_info}"}
    finally:
        if initial_info_driver:
            initial_info_driver.quit()

    # Step 2: Scrape categories in parallel
    # Limit concurrent browser instances for categories for a single company
    max_concurrent_categories = min(len(REVIEW_CATEGORIES), 3) # Adjust based on server resources
    print(f"  [{company_slug}] Starting parallel scrape for {len(REVIEW_CATEGORIES)} categories with max {max_concurrent_categories} concurrent browsers...")

    futures_map = {} # To map future to category_name for easier result handling
    with ThreadPoolExecutor(max_workers=max_concurrent_categories) as executor:
        for category_name in REVIEW_CATEGORIES:
            future = executor.submit(
                _scrape_specific_category_all_pages,
                company_base_url_str,
                category_name,
                company_slug
            )
            futures_map[future] = category_name

        for future in as_completed(futures_map):
            original_category_name = futures_map[future]
            try:
                _, questions_from_category = future.result() # Unpack (cat_name, questions)
                if questions_from_category:
                    print(f"  [{company_slug}] Received {len(questions_from_category)} question sections from category '{original_category_name}'. Merging...")
                    # Simple append for now, assuming _scrape_specific_category_all_pages handles its internal uniqueness
                    # And parse_review_page_html sets section_name correctly via context
                    all_questions_for_company.extend(questions_from_category)
                else:
                    print(f"  [{company_slug}] Category '{original_category_name}' returned no questions.")
            except Exception as e_future:
                print(f"  [{company_slug}] Category scraping task for '{original_category_name}' generated an exception: {e_future}")

    # Optional: Post-merge de-duplication of questions if needed, though unlikely if tracker is good
    # final_unique_questions = []
    # seen_q_texts = set()
    # for q_obj in all_questions_for_company:
    #    if q_obj.question_text not in seen_q_texts:
    #        final_unique_questions.append(q_obj)
    #        seen_q_texts.add(q_obj.question_text)
    # all_questions_for_company = final_unique_questions

    total_duration = time.time() - start_time_total
    print(f"\nFinished ALL parallel category scrapes for {company_slug} in {total_duration:.2f}s. Total questions collected: {len(all_questions_for_company)}")

    # Ensure company_name is somewhat sensible if initial fetch failed badly
    if not company_details_overall.get("company_name") or company_details_overall.get("company_name", "").lower() == company_slug.lower() or company_details_overall.get("company_name", "") == "unknown_company":
        company_details_overall["company_name"] = company_slug.replace('-', ' ').title() # Fallback name
        if "status_note" not in company_details_overall:
            company_details_overall["status_note"] = "Company name set to fallback."

    return {
        "status": "success",
        "data": {
            "company_info": company_details_overall,
            "reviews": [q.model_dump(mode='json') for q in all_questions_for_company]
        }
    }

# --- FastAPI Endpoint (Unchanged) ---
@app.post("/scrape")
async def scrape_companies(
    request: ScrapeRequest = Body(...)
) -> Dict[str, Dict[str, Any]]:
    urls = request.urls
    if not urls:
        raise HTTPException(status_code=400, detail="No URLs provided.")
    results: Dict[str, Dict[str, Any]] = {}
    tasks = []
    print(f"Received API request to scrape {len(urls)} URLs (Parallel Categories v1.4).")
    for url_obj in urls:
        url_str = str(url_obj)
        try:
            parsed_url = urlparse(url_str)
            path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg]
            if len(path_segments) >= 2 and path_segments[0] == "companies":
                company_slug = path_segments[1]
            else:
                raise ValueError("URL path does not conform to /companies/company-slug")
        except Exception as e_slug:
            print(f"Error parsing company slug from URL '{url_str}': {e_slug}")
            results[url_str] = {"status": "error", "message": f"Invalid Comparably company URL format: {url_str}"}
            continue
        tasks.append(asyncio.to_thread(scrape_comparably_sync, url_str, company_slug))
    scraped_results = await asyncio.gather(*tasks, return_exceptions=True)
    for i, url_obj in enumerate(urls):
        url_str = str(url_obj)
        if url_str in results: continue
        result_or_exc = scraped_results[i]
        if isinstance(result_or_exc, Exception):
            results[url_str] = {"status": "error", "message": f"Scraping task failed: {result_or_exc}"}
        elif isinstance(result_or_exc, dict):
             results[url_str] = result_or_exc
        else:
             results[url_str] = {"status": "error", "message": "Unexpected internal result type"}
    print("Finished processing API request (Parallel Categories v1.4).")
    return results
