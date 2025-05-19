import os
import json
import re
import time
import random
import asyncio
from typing import List, Dict, Optional, Any
from datetime import datetime
from urllib.parse import urlparse, urljoin

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
    description="API to scrape company reviews (multi-category, multi-page) from Comparably using Selenium.",
    version="1.3.0" # Version bump
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
def parse_review_page_html(soup: BeautifulSoup, company_slug: str) -> List[Question]:
    questions: List[Question] = []
    review_list_divs = soup.find_all('div', class_='reviewsList')
    if not review_list_divs:
        # print(f"Debug: No 'div.reviewsList' found in HTML for {company_slug} on current page.")
        return []

    for review_list_div in review_list_divs:
        q_elem = review_list_div.find('h2', class_='section-subtitle')
        if not q_elem: continue
        question_text = q_elem.get_text(strip=True)

        section_name_from_page = "unknown_section" # Default if link not found
        link_p = review_list_div.find('p', class_='section-text')
        if link_p and (a_tag := link_p.find('a', href=True)):
            section_name_from_page = extract_section_name_from_url(a_tag['href'])
        # If parsing HTML directly, the section name should reflect the current category context
        # This might be redundant if we pass category_name, but good for independent parsing

        reviews_for_this_question: List[Review] = []
        review_blocks = review_list_div.find_all('div', class_='cppRH') # Individual review containers

        for block in review_blocks:
            quote = block.find('p', class_='cppRH-review-quote')
            if not quote: continue
            text = quote.get_text(strip=True).replace('\u0000', '')

            cite_block = block.find('cite', class_='cppRH-review-cite')
            date_meta = None
            if cite_block:
                date_meta = cite_block.find('meta', {'itemprop': 'datePublished'}) or \
                            cite_block.find('meta', attrs={'content': re.compile(r'^\d{4}-\d{2}-\d{2}$')})

            if not date_meta or not date_meta.get('content'):
                # print(f"Debug: No date found for review: {text[:30]}...")
                continue
            try:
                date_val = datetime.strptime(date_meta['content'], '%Y-%m-%d')
            except ValueError:
                # print(f"Debug: Invalid date format: {date_meta['content']}")
                continue
            reviews_for_this_question.append(Review(text=text, date=date_val))

        if not reviews_for_this_question: continue

        reviews_for_this_question.sort(key=lambda r: r.date, reverse=True)
        try:
            # The section_name here is tied to the link in the HTML, might not always match the overall category being processed
            # If we rely on overall category context, use that instead. For now, this uses what's parsed.
            section = ReviewSection(section_name=section_name_from_page, reviews=reviews_for_this_question)
            questions.append(Question(question_text=question_text, review_section=section))
        except ValidationError as e:
            print(f"Pydantic validation error creating Question for '{question_text}': {e}")
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
        details['comparably_url'] = str(company_base_url_str) # Original base URL

        # Attempt to get name from H1 on the current page (could be a category reviews page)
        # The H1 might be like "Datadog Leadership Reviews"
        name_tag_h1 = soup.find('h1') # More generic H1
        if name_tag_h1:
            h1_text = name_tag_h1.get_text(strip=True)
            # Try to extract a cleaner company name if H1 includes " Reviews" or category
            # This is heuristic
            if " Reviews" in h1_text:
                name_candidate = h1_text.split(" Reviews")[0].strip()
                # Avoid setting if it becomes just the category name
                if name_candidate.lower() not in REVIEW_CATEGORIES and len(name_candidate) > 3:
                    details['company_name'] = name_candidate

        # Fallback to page title if H1 wasn't specific enough
        if details['company_name'] == default_name or details['company_name'].lower() in REVIEW_CATEGORIES :
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                # Example: "Datadog Leadership Reviews | Comparably"
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
    options.add_argument("--disable-dev-shm-usage") # Crucial for Docker
    options.add_argument("--disable-gpu") # Recommended for headless
    options.add_argument("window-size=1920,1080") # Can help with element visibility
    user_agent_str = ua.random if ua else "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent_str}')

    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.navigator.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            '''
        })
        return driver
    except Exception as e:
        print(f"Error setting up WebDriver: {e}")
        raise RuntimeError(f"Failed to setup Selenium WebDriver: {e}")


# --- Main Scraping Function ---
def scrape_comparably_sync(company_base_url_str: str, company_slug: str) -> Dict[str, Any]:
    print(f"Starting multi-category, multi-page scrape for: {company_slug}")
    driver = None
    start_time_total = time.time()

    all_questions_for_company: List[Question] = []
    company_details_overall: Dict[str, Any] = {}
    processed_reviews_tracker = set() # (question_text_hash, review_text_hash, review_date)

    try:
        driver = setup_driver()
        wait = WebDriverWait(driver, 18) # General wait for elements
        short_wait = WebDriverWait(driver, 7) # For quick checks like button presence

        for category_idx, category_name in enumerate(REVIEW_CATEGORIES):
            # Construct base URL for the category, ensuring trailing slash for urljoin to work as expected if used
            # For direct navigation, we build the full URL.
            category_url_base = f"{company_base_url_str.rstrip('/')}/reviews/{category_name}/"
            print(f"\n  Processing category: {category_name} ({category_idx+1}/{len(REVIEW_CATEGORIES)})")
            print(f"    Navigating to initial category page: {category_url_base}")
            driver.get(category_url_base)

            time.sleep(0.75)
            if "Error" in driver.title or "Not Found" in driver.title or "404" in driver.title:
                print(f"    Error page detected for category '{category_name}'. Skipping category.")
                continue

            page_count_in_category = 0
            while page_count_in_category < MAX_PAGES_PER_CATEGORY:
                page_count_in_category += 1
                current_page_scrape_url = driver.current_url # Log the URL we are actually scraping
                print(f"    Scraping page {page_count_in_category} for '{category_name}' (URL: {current_page_scrape_url})")

                review_block_selector = "div.cppRH"
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, review_block_selector)))
                except TimeoutException:
                    print(f"    Timeout waiting for review content on page {page_count_in_category} of '{category_name}'.")
                    if page_count_in_category == 1:
                        print(f"    Category '{category_name}' appears to be empty or no reviews on its first page.")
                    break # Break from pagination loop for this category

                current_soup = BeautifulSoup(driver.page_source, 'html.parser')

                if not company_details_overall: # Fetch once
                    company_details_overall = extract_company_info(current_soup, company_base_url_str)

                questions_on_this_page = parse_review_page_html(current_soup, company_slug)
                reviews_added_this_page_count = 0

                if questions_on_this_page:
                    for q_parsed_from_page in questions_on_this_page:
                        # Override section_name with the current category context for consistency
                        q_parsed_from_page.review_section.section_name = category_name

                        existing_q_object = next((q for q in all_questions_for_company if q.question_text == q_parsed_from_page.question_text), None)

                        if not existing_q_object: # New question structure for this company
                            unique_reviews_for_new_q = []
                            for review in q_parsed_from_page.review_section.reviews:
                                review_key = (hash(q_parsed_from_page.question_text), hash(review.text), review.date)
                                if review_key not in processed_reviews_tracker:
                                    unique_reviews_for_new_q.append(review)
                                    processed_reviews_tracker.add(review_key)
                                    reviews_added_this_page_count += 1
                            if unique_reviews_for_new_q:
                                q_parsed_from_page.review_section.reviews = unique_reviews_for_new_q # Replace with only unique
                                all_questions_for_company.append(q_parsed_from_page)
                        else: # Question structure already exists, append new unique reviews
                            for review in q_parsed_from_page.review_section.reviews:
                                review_key = (hash(existing_q_object.question_text), hash(review.text), review.date)
                                if review_key not in processed_reviews_tracker:
                                    existing_q_object.review_section.reviews.append(review)
                                    processed_reviews_tracker.add(review_key)
                                    reviews_added_this_page_count += 1
                            existing_q_object.review_section.reviews.sort(key=lambda r: r.date, reverse=True)

                    if reviews_added_this_page_count > 0:
                        print(f"    Added {reviews_added_this_page_count} unique reviews from page {page_count_in_category} of '{category_name}'.")
                else:
                    print(f"    No review questions parsed from page {page_count_in_category} of '{category_name}'.")
                    if page_count_in_category == 1:
                        print(f"    Category '{category_name}' seems empty (first page).")
                    break # End pagination for this category

                # --- Click-Based Pagination Logic ---
                next_page_button_element = None
                try:
                    # Selector for the '>' button, often an <a> tag inside <li> or with specific class/aria-label
                    # Example: <li class="pagination-next"><a href="...?page=2">Next</a></li>
                    # Or the direct button like in your image.
                    # The tooltip "Next Page" is a strong hint.
                    # This specific selector targets an <a> tag whose direct child is a <span> with class "ico-arrow-right"
                    # or an <a> tag with aria-label="Next Page". Adapt if needed.
                    selectors_for_next = [
                        "a.pagination-link[rel='next']", # Common standard
                        "a[aria-label='Next Page']",
                        "a[title='Next Page']",
                        "li.pagination-next > a", # Next button within an li
                        "a.pagination-next", # A common class name for next button
                        "nav[aria-label*='pagination'] li:last-child a[href]" # Last link in pagination nav
                    ]
                    for sel in selectors_for_next:
                        try:
                            # Check if element is present and then if it's clickable
                            candidate_buttons = driver.find_elements(By.CSS_SELECTOR, sel)
                            for btn in candidate_buttons:
                                # Filter out "Previous" if selector is too general and ensure it's displayed
                                if btn.is_displayed() and ("prev" not in (btn.get_attribute("aria-label") or "").lower() and \
                                   "prev" not in (btn.get_attribute("rel") or "").lower()):
                                    next_page_button_element = short_wait.until(EC.element_to_be_clickable(btn))
                                    if next_page_button_element:
                                        print(f"    Found 'Next Page' button with selector: '{sel}' and text/aria: '{next_page_button_element.text or next_page_button_element.get_attribute('aria-label')}'")
                                        break # Found a suitable button
                            if next_page_button_element: break # Found it, exit selector loop
                        except (NoSuchElementException, TimeoutException):
                            continue # Try next selector

                    if not next_page_button_element:
                        print(f"    No clickable 'Next Page' button found for '{category_name}' after page {page_count_in_category}. End of category.")
                        break

                    print(f"    Attempting to click 'Next Page' button (Current URL: {driver.current_url})...")
                    # Scroll into view gently
                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", next_page_button_element)
                    time.sleep(0.5) # Brief pause for scrolling

                    try:
                        next_page_button_element.click()
                    except ElementClickInterceptedException:
                        print("    Click intercepted, trying JavaScript click for 'Next Page'...")
                        driver.execute_script("arguments[0].click();", next_page_button_element)

                    # Wait for page to transition. A robust way is to wait for old content to go stale
                    # or new content to load fully. For simplicity, a timed wait.
                    time.sleep(random.uniform(2.0, 4.0)) # Increased wait for content to load

                except (NoSuchElementException, TimeoutException) as e_pagination:
                    print(f"    No 'Next Page' button or error in pagination logic for '{category_name}' after page {page_count_in_category}. (Error: {type(e_pagination).__name__}). End of category.")
                    break
            # Small delay between categories
            time.sleep(random.uniform(1.5, 3.0))

        total_duration = time.time() - start_time_total
        print(f"\nFinished all categories for {company_slug} in {total_duration:.2f}s. Total unique questions structured: {len(all_questions_for_company)}")

        if not company_details_overall and all_questions_for_company: # Failsafe
             company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": "Company details fetch might have been incomplete"}

        return {
            "status": "success",
            "data": {
                "company_info": company_details_overall,
                "reviews": [q.model_dump(mode='json') for q in all_questions_for_company]
            }
        }

    except Exception as e:
        total_duration = time.time() - start_time_total
        print(f"Critical error during multi-category scrape for {company_slug} after {total_duration:.2f}s: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": f"An internal error occurred: {str(e)}"}
    finally:
        if driver:
            driver.quit()

# --- FastAPI Endpoint ---
@app.post("/scrape")
async def scrape_companies(
    request: ScrapeRequest = Body(...)
) -> Dict[str, Dict[str, Any]]:
    urls = request.urls
    if not urls:
        raise HTTPException(status_code=400, detail="No URLs provided.")

    results: Dict[str, Dict[str, Any]] = {}
    tasks = []
    print(f"Received request to scrape {len(urls)} URLs (multi-category, click pagination v1.3).")

    for url_obj in urls:
        url_str = str(url_obj)
        try:
            # Ensure we are getting the company slug correctly from the base URL
            parsed_url = urlparse(url_str)
            path_segments = [seg for seg in parsed_url.path.strip('/').split('/') if seg] # Get non-empty segments
            if len(path_segments) >= 2 and path_segments[0] == "companies":
                company_slug = path_segments[1]
            else:
                raise ValueError("URL path does not conform to /companies/company-slug structure")
        except Exception as e_slug:
            print(f"Error parsing company slug from URL '{url_str}': {e_slug}")
            results[url_str] = {"status": "error", "message": f"Invalid Comparably company URL format: {url_str}"}
            continue

        tasks.append(
            asyncio.to_thread(scrape_comparably_sync, url_str, company_slug)
        )

    scraped_results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, url_obj in enumerate(urls):
        url_str = str(url_obj)
        if url_str in results: continue # Skip URLs that failed slug parsing

        result_or_exc = scraped_results[i]
        if isinstance(result_or_exc, Exception):
            print(f"Task for {url_str} raised an exception: {result_or_exc}")
            results[url_str] = {"status": "error", "message": f"Scraping task failed: {result_or_exc}"}
        elif isinstance(result_or_exc, dict):
             results[url_str] = result_or_exc
        else:
             print(f"Unexpected result type for {url_str}: {type(result_or_exc)}")
             results[url_str] = {"status": "error", "message": "Unexpected internal result type from scraper"}

    print("Finished processing multi-category scrape request (click pagination v1.3).")
    return results
