import os
import json
import re
import time
import random
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse, urljoin
import traceback
import sys

# --- START: Playwright Windows asyncio fix ---
if sys.platform == "win32" and sys.version_info >= (3, 8):
    print("Applying WindowsSelectorEventLoopPolicy for Playwright compatibility.")
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# --- END: Playwright Windows asyncio fix ---

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
    title="Comparably Scraper API with Playwright & Date Filter",
    description="API to scrape company reviews from Comparably using Playwright, with date filtering.",
    version="1.7.1" # Version bump for Windows fix
)

# --- BeautifulSoup (still needed for parsing HTML) ---
from bs4 import BeautifulSoup

# --- Playwright ---
from playwright.async_api import (
    async_playwright,
    Browser,
    Page,
    BrowserContext,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError
)

# --- User Agent ---
try:
    from fake_useragent import UserAgent
    ua = UserAgent()
except ImportError:
    print("Warning: fake-useragent not installed. Using a generic User-Agent.")
    ua = None # type: ignore

# --- Constants ---
REVIEW_CATEGORIES = ["leadership", "compensation", "team", "environment", "outlook"]
MAX_PAGES_PER_CATEGORY = 15
PAGE_TIMEOUT_S = 30  # Seconds for page loads (converted to MS for Playwright)
ELEMENT_TIMEOUT_S = 18 # Seconds for element waits (converted to MS for Playwright)

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
            text = quote.get_text(strip=True).replace('\u0000', '') # Null byte removal
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
            name_candidate = h1_text
            if " Reviews" in h1_text: name_candidate = h1_text.split(" Reviews")[0].strip()
            if name_candidate.lower() not in REVIEW_CATEGORIES and len(name_candidate) > 3:
                 details['company_name'] = name_candidate
        if details['company_name'] == default_name or details['company_name'].lower() in REVIEW_CATEGORIES :
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text(strip=True); name_from_title = title_text.split(" Reviews")[0].split(" | Comparably")[0].strip()
                if name_from_title and name_from_title != details['company_name'] and len(name_from_title) > 3:
                    details['company_name'] = name_from_title
    except Exception as e: print(f"Error extracting company details for {company_base_url_str}: {e}")
    return details


# --- MODIFIED HELPER: Scrapes a single category with Playwright ---
async def _scrape_specific_category_all_pages_playwright(
    browser: Browser,
    company_base_url_str: str,
    category_name: str,
    company_slug: str,
    user_agent_str: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    task_id = f"PlaywrightCatScraper-{category_name}-{company_slug[:10]}"
    print(f"  [{task_id}] Started.")
    category_questions: List[Question] = []
    processed_reviews_in_this_category_run = set()

    context: Optional[BrowserContext] = None
    page: Optional[Page] = None

    PLAYWRIGHT_PAGE_TIMEOUT_MS = PAGE_TIMEOUT_S * 1000
    PLAYWRIGHT_ELEMENT_TIMEOUT_MS = ELEMENT_TIMEOUT_S * 1000
    PLAYWRIGHT_SHORT_ELEMENT_TIMEOUT_MS = (ELEMENT_TIMEOUT_S // 2) * 1000

    try:
        context = await browser.new_context(
            user_agent=user_agent_str,
            java_script_enabled=True,
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()
        await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        category_url_start = f"{company_base_url_str.rstrip('/')}/reviews/{category_name}/"
        print(f"  [{task_id}] Navigating to initial page: {category_url_start}")
        await page.goto(category_url_start, timeout=PLAYWRIGHT_PAGE_TIMEOUT_MS, wait_until="domcontentloaded")

        await asyncio.sleep(0.8)
        page_title = await page.title()
        if "Error" in page_title or "Not Found" in page_title or "Page Not Found" in page_title:
            print(f"  [{task_id}] Error page detected (Title: {page_title}). Skipping category '{category_name}'.")
            return category_name, []

        page_count = 0
        while page_count < MAX_PAGES_PER_CATEGORY:
            page_count += 1
            current_page_url_in_cat = page.url
            print(f"  [{task_id}] Scraping category '{category_name}', page {page_count} (URL: {current_page_url_in_cat})")

            review_block_sel_css = "div.cppRH"
            try:
                await page.wait_for_selector(review_block_sel_css, state="attached", timeout=PLAYWRIGHT_ELEMENT_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                print(f"  [{task_id}] Timeout waiting for review content (selector: '{review_block_sel_css}') on page {page_count} for category '{category_name}'.")
                if page_count == 1: print(f"  [{task_id}] Category '{category_name}' appears empty or inaccessible.")
                break

            html_content = await page.content()
            soup = BeautifulSoup(html_content, 'html.parser')
            questions_this_page = parse_review_page_html(soup, company_slug, category_name, start_date_filter, end_date_filter)

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
                if reviews_added_this_page_count > 0: print(f"  [{task_id}] Added {reviews_added_this_page_count} unique reviews from page {page_count} for category '{category_name}'.")
            else:
                print(f"  [{task_id}] No review questions parsed/matched from page {page_count} for category '{category_name}'.")
                if not await page.query_selector(review_block_sel_css):
                    if page_count > 1 : print(f"  [{task_id}] No review containers found, likely end of category '{category_name}'."); break
                    if page_count == 1: print(f"  [{task_id}] Category '{category_name}' seems empty (first page)."); break

            next_page_button_handle = None
            try:
                selectors_for_next = [ "a.pagination-link[rel='next']", "a[aria-label='Next Page']", "a[title='Next Page']", "li.pagination-next > a", "a.pagination-next", "nav[aria-label*='pagination'] li:last-child a[href]" ]
                for sel in selectors_for_next:
                    candidate_buttons = await page.query_selector_all(sel)
                    for btn_candidate_loc in candidate_buttons:
                        if await btn_candidate_loc.is_visible() and await btn_candidate_loc.is_enabled():
                            aria_label = (await btn_candidate_loc.get_attribute("aria-label") or "").lower()
                            rel_attr = (await btn_candidate_loc.get_attribute("rel") or "").lower()
                            if "prev" not in aria_label and "prev" not in rel_attr:
                                next_page_button_handle = btn_candidate_loc
                                print(f"  [{task_id}] Found 'Next Page' button for '{category_name}' with selector: '{sel}'")
                                break
                    if next_page_button_handle: break

                if not next_page_button_handle:
                    print(f"  [{task_id}] No clickable 'Next Page' button found after page {page_count}. End of category '{category_name}'.")
                    break

                print(f"  [{task_id}] Attempting to click 'Next Page' for '{category_name}'...")
                await next_page_button_handle.scroll_into_view_if_needed()
                await asyncio.sleep(0.5)
                try:
                    await next_page_button_handle.click(timeout=PLAYWRIGHT_SHORT_ELEMENT_TIMEOUT_MS)
                except PlaywrightTimeoutError:
                    print(f"  [{task_id}] Standard click timed out/failed for 'Next Page' in '{category_name}', trying JS click...")
                    await page.evaluate("arguments[0].click();", next_page_button_handle)

                try:
                    await page.wait_for_load_state('domcontentloaded', timeout=PLAYWRIGHT_PAGE_TIMEOUT_MS)
                except PlaywrightTimeoutError:
                    print(f"  [{task_id}] Timeout waiting for page load state after click for '{category_name}'. Content might be dynamic or page unresponsive.")
                await asyncio.sleep(random.uniform(1.5, 2.5))

            except PlaywrightTimeoutError as e_timeout_pag:
                print(f"  [{task_id}] Timeout in Playwright pagination for '{category_name}' (Page {page_count}): {e_timeout_pag}. End of category.")
                break
            except PlaywrightError as e_pag:
                print(f"  [{task_id}] Playwright Pagination error for '{category_name}' (Page {page_count}): {type(e_pag).__name__} - {e_pag}. End of category.")
                break
            except Exception as e_pag_generic:
                 print(f"  [{task_id}] Generic Pagination error for '{category_name}' (Page {page_count}): {type(e_pag_generic).__name__} - {e_pag_generic}. End of category.")
                 break
        print(f"  [{task_id}] Finished category '{category_name}'. Found {len(category_questions)} question sections.")
        return category_name, category_questions
    except Exception as e_cat_page_overall:
        print(f"  [{task_id}] CRITICAL ERROR during Playwright category page processing for '{category_name}': {e_cat_page_overall}")
        traceback.print_exc()
        return category_name, category_questions # Return what was collected
    finally:
        if page:
            try: await page.close()
            except Exception as e_close: print(f"  [{task_id}] Error closing page for '{category_name}': {e_close}")
        if context:
            try: await context.close()
            except Exception as e_close: print(f"  [{task_id}] Error closing context for '{category_name}': {e_close}")
        print(f"  [{task_id}] Playwright Page/Context for '{category_name}' closed.")


# --- MODIFIED: Main Orchestrator for a single company with Playwright ---
async def scrape_comparably_async(
    playwright_manager,
    company_base_url_str: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating PLAYWRIGHT parallel category scrape for: {company_slug}")
    start_time_total = time.time()
    all_questions_for_company: List[Question] = []
    company_details_overall: Dict[str, Any] = {}

    browser: Optional[Browser] = None
    context_info: Optional[BrowserContext] = None
    page_info: Optional[Page] = None

    user_agent_to_use = (ua.random if ua
                        else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")

    PLAYWRIGHT_PAGE_TIMEOUT_MS = PAGE_TIMEOUT_S * 1000

    try:
        print(f"  [{company_slug}] Launching Playwright browser...")
        browser = await playwright_manager.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        print(f"  [{company_slug}] Playwright browser launched.")

        print(f"  [{company_slug}] Fetching initial company info with Playwright (User-Agent: {user_agent_to_use.split(')')[0] + ')...'})...")
        context_info = await browser.new_context(
            user_agent=user_agent_to_use,
            java_script_enabled=True,
            viewport={'width': 1920, 'height': 1080}
        )
        page_info = await context_info.new_page()
        await page_info.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        info_fetch_url = f"{company_base_url_str.rstrip('/')}/reviews/"
        try:
            print(f"  [{company_slug}] Navigating to info URL: {info_fetch_url}")
            await page_info.goto(info_fetch_url, timeout=PLAYWRIGHT_PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
            await asyncio.sleep(1.0)
            page_title_info = await page_info.title()
            if "Error" in page_title_info or "Not Found" in page_title_info or "Page Not Found" in page_title_info:
                print(f"  [{company_slug}] /reviews/ page for info resulted in '{page_title_info}', trying base URL: {company_base_url_str}")
                await page_info.goto(company_base_url_str, timeout=PLAYWRIGHT_PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
                await asyncio.sleep(1.0)
                page_title_info = await page_info.title()
                if "Error" in page_title_info or "Not Found" in page_title_info or "Page Not Found" in page_title_info:
                    raise PlaywrightError(f"Could not load a valid page for company info. Last title: {page_title_info}")

            info_html = await page_info.content()
            info_soup = BeautifulSoup(info_html, 'html.parser')
            company_details_overall = extract_company_info(info_soup, company_base_url_str)
            print(f"  [{company_slug}] Initial company info fetched: Name='{company_details_overall.get('company_name', 'N/A')}' URL='{company_details_overall.get('comparably_url', 'N/A')}'")
        except Exception as e_info_pw:
            print(f"  [{company_slug}] Error fetching initial company info with Playwright: {e_info_pw}")
            traceback.print_exc()
            company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": f"Initial info fetch error (Playwright): {str(e_info_pw)}"}
        finally:
            if page_info: await page_info.close()
            if context_info: await context_info.close()
            print(f"  [{company_slug}] Playwright Page/Context for initial info closed.")

        max_concurrent_categories = min(len(REVIEW_CATEGORIES), 3)
        print(f"  [{company_slug}] Starting PLAYWRIGHT parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")

        tasks = []
        semaphore = asyncio.Semaphore(max_concurrent_categories)

        async def run_scrape_with_semaphore_wrapper(cat_name):
            async with semaphore:
                return await _scrape_specific_category_all_pages_playwright(
                    browser, company_base_url_str, cat_name, company_slug,
                    user_agent_to_use, start_date_filter, end_date_filter
                )

        for category_name_iter in REVIEW_CATEGORIES:
            tasks.append(run_scrape_with_semaphore_wrapper(category_name_iter))

        category_scrape_results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, res_or_exc in enumerate(category_scrape_results):
            cat_name_processed = REVIEW_CATEGORIES[i]
            if isinstance(res_or_exc, Exception):
                print(f"  [{company_slug}] PLAYWRIGHT Category scraping task for '{cat_name_processed}' FAILED: {res_or_exc}")
            elif isinstance(res_or_exc, tuple) and len(res_or_exc) == 2:
                _, questions_from_cat = res_or_exc
                if questions_from_cat:
                    print(f"  [{company_slug}] Received {len(questions_from_cat)} Qs from cat '{cat_name_processed}'. Merging...")
                    all_questions_for_company.extend(questions_from_cat)
                else:
                    print(f"  [{company_slug}] Cat '{cat_name_processed}' returned no Qs (or none matched date filter).")
            else:
                print(f"  [{company_slug}] Unexpected result type from category task '{cat_name_processed}': {type(res_or_exc)}")

    except Exception as e_orchestrate:
        print(f"  [{company_slug}] CRITICAL ERROR during Playwright orchestration: {e_orchestrate}")
        traceback.print_exc()
        if not company_details_overall:
             company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str}
        current_status_note = company_details_overall.get("status_note", "")
        separator = " | " if current_status_note else ""
        company_details_overall["status_note"] = f"{current_status_note}{separator}Orchestration error (Playwright): {str(e_orchestrate)}"
        return {"status": "error", "message": f"Orchestration failed: {str(e_orchestrate)}", "data": {"company_info": company_details_overall, "reviews": []}}
    finally:
        if browser:
            try:
                await browser.close()
                print(f"  [{company_slug}] Playwright browser closed.")
            except Exception as e_browser_close:
                print(f"  [{company_slug}] Error closing Playwright browser: {e_browser_close}")

    total_duration = time.time() - start_time_total
    print(f"\nFinished ALL PLAYWRIGHT parallel category scrapes for {company_slug} in {total_duration:.2f}s. Total Qs: {len(all_questions_for_company)}")

    if not company_details_overall.get("company_name") or \
       company_details_overall.get("company_name", "").lower() == company_slug.lower() or \
       company_details_overall.get("company_name", "") == "unknown_company" or \
       company_details_overall.get("company_name", "").lower() in REVIEW_CATEGORIES :
        company_details_overall["company_name"] = company_slug.replace('-', ' ').title()
        current_status_note = company_details_overall.get("status_note", "")
        separator = " | " if current_status_note else ""
        company_details_overall["status_note"] = f"{current_status_note}{separator}Name set to fallback."
        company_details_overall["status_note"] = company_details_overall["status_note"].strip(" | ")

    return {
        "status": "success" if all_questions_for_company or (company_details_overall.get("company_name") != company_slug.replace('-', ' ').title() and company_details_overall.get("company_name","").lower() not in REVIEW_CATEGORIES) else "partial_success_no_reviews",
        "data": {"company_info": company_details_overall, "reviews": [q.model_dump(mode='json') for q in all_questions_for_company]}
    }


# --- FastAPI Endpoint ---

@app.post("/scrape")
async def scrape_companies(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    urls_input = request.urls
    start_date_filter: Optional[datetime] = None
    end_date_filter: Optional[datetime] = None

    # ... (your existing date parsing and validation logic) ...

    results_dict: Dict[str, Dict[str, Any]] = {}
    api_tasks_map: Dict[str, asyncio.Task] = {}

    date_filter_msg = f" (Start: {request.start_date_str or 'N/A'}, End: {request.end_date_str or 'N/A'})"
    print(f"API request: {len(urls_input)} URLs, Playwright & Date Filter{date_filter_msg} (v{app.version}).")

    # +++ START DEBUG PRINTS +++
    try:
        current_loop = asyncio.get_event_loop()
        print(f"[DEBUG] Inside scrape_companies - Current event loop: {current_loop}")
        current_policy = asyncio.get_event_loop_policy()
        print(f"[DEBUG] Inside scrape_companies - Current event loop policy: {current_policy}")
        if sys.platform == "win32":
            if isinstance(current_loop, asyncio.SelectorEventLoop):
                print("[DEBUG] Loop is SelectorEventLoop - GOOD for Playwright on Windows.")
            elif isinstance(current_loop, asyncio.ProactorEventLoop):
                print("[DEBUG] Loop is ProactorEventLoop - BAD for Playwright on Windows.")
            else:
                print(f"[DEBUG] Loop is of unexpected type: {type(current_loop)}")
    except Exception as e_debug:
        print(f"[DEBUG] Error getting debug info: {e_debug}")
    # +++ END DEBUG PRINTS +++

    async with async_playwright() as playwright_manager_instance:
        for url_obj_input in urls_input:
            url_str_input = str(url_obj_input)
            try:
                parsed_url_input = urlparse(url_str_input)
                path_segments_input = [seg for seg in parsed_url_input.path.strip('/').split('/') if seg]
                if len(path_segments_input) >= 2 and path_segments_input[0] == "companies":
                    company_slug_input = path_segments_input[1]
                else:
                    raise ValueError("URL path error, expected '/companies/company-slug/...' structure.")

                task = asyncio.create_task(
                    scrape_comparably_async(
                        playwright_manager_instance, url_str_input, company_slug_input,
                        start_date_filter, end_date_filter
                    )
                )
                api_tasks_map[url_str_input] = task
            except Exception as e_slug_parse:
                print(f"Error parsing slug from URL '{url_str_input}': {e_slug_parse}")
                results_dict[url_str_input] = {
                    "status": "error",
                    "message": f"Invalid Comparably company URL format: {url_str_input}. Error: {e_slug_parse}"
                }

        if api_tasks_map:
            await asyncio.gather(*api_tasks_map.values(), return_exceptions=True)
            for url_str_item, task_item in api_tasks_map.items():
                try:
                    result_or_exc_item = task_item.result()
                    if isinstance(result_or_exc_item, dict):
                         results_dict[url_str_item] = result_or_exc_item
                    else:
                         results_dict[url_str_item] = {"status": "error", "message": "Unexpected internal result type from scrape task."}
                except Exception as e_task:
                    print(f"Task for {url_str_item} EXCEPTION (type: {type(e_task).__name__}): {e_task}")
                    tb_str = "No traceback available."
                    if e_task.__traceback__:
                        tb_str = "".join(traceback.format_exception(None, e_task, e_task.__traceback__))
                    print(f"FULL TRACEBACK for {url_str_item} (Playwright Task):\n{tb_str}")
                    results_dict[url_str_item] = {
                        "status": "error",
                        "message": f"Scraping task failed. Type: {type(e_task).__name__}, Repr: {repr(e_task)}. Check logs."
                    }

    print(f"Finished API request processing (Playwright v{app.version}).")
    return results_dict

# --- Health Check Endpoint ---
@app.get("/health")
async def health_check(): return {"status": "ok", "version": app.version}

# Example for local running:
if __name__ == "__main__":
    import uvicorn
    # If this file is named v3.py (or whatever your filename is):
    # uvicorn.run("v3:app", host="0.0.0.0", port=8000, reload=True) # Replace v3 with actual filename
    # To run the file from your traceback (v5.py):
    # uvicorn.run("v5:app", host="0.0.0.0", port=8000, reload=True)
    # For generic use, assuming the file is the main module:
    current_file_name = os.path.splitext(os.path.basename(__file__))[0]
    print(f"Attempting to run uvicorn for {current_file_name}:app")
    uvicorn.run(f"{current_file_name}:app", host="0.0.0.0", port=8000, reload=True)