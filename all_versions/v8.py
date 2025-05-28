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

# --- FastAPI (will be defined later for single file execution) ---
from fastapi import FastAPI, HTTPException, Body
# app will be initialized later

# --- BeautifulSoup ---
from bs4 import BeautifulSoup

# --- curl_cffi for robust HTTP requests ---
from curl_cffi.requests import Session as CurlCffiSession
from curl_cffi.requests import RequestsError 

# --- User Agent ---
try:
    from fake_useragent import UserAgent
    ua_instance = UserAgent()
except ImportError:
    print("Warning: fake-useragent not installed. Using a generic User-Agent.")
    ua_instance = None

# --- Constants ---
API_TITLE_CONFIG = "Comparably Scraper API - Curl-CFFI Only"
API_DESCRIPTION_CONFIG = "Full scraping using Curl-CFFI for HTTP requests and BeautifulSoup for parsing."
API_VERSION_CONFIG = "3.0.3" # Incremented for recursive fix attempt

REVIEW_CATEGORIES_CONFIG = ["leadership", "compensation", "team", "environment", "outlook"]
# REVIEW_CATEGORIES_CONFIG = ["outlook"] # DEBUG: Focus on one category

MAX_CATEGORY_PAGES_CONFIG = 15 
MAX_REVIEW_PAGES_PER_QUESTION_CONFIG = 10
CURL_REQUEST_TIMEOUT_S_CONFIG = 30
CURL_IMPERSONATE_BROWSER_CONFIG = "chrome110" 

NEXT_PAGE_SELECTORS_CONFIG = [
    "div.pager_controls a.page.next.pager_next",
    "a.qa-PaginationPageLink-Next",
    "a.pagination-link[rel='next']", "a[aria-label*='Next Page' i]", "a[title*='Next Page' i]",
    "li.pagination-next > a", "a.pagination-next", "a.NextPageLink",
    "nav[aria-label*='pagination' i] li:last-child a[href]",
    ".page-next > a", "a.next"
]
REVIEW_BLOCK_CSS_SELECTOR_CONFIG = "div.cppRH" # This constant is used for the class check now

# --- Helper Functions ---
def get_user_agent() -> str:
    return ua_instance.random if ua_instance else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"

def extract_section_name_from_url(href: Optional[str]) -> str:
    if not href: return "unknown_section"
    try:
        path_parts = urlparse(href).path.strip('/').split('/')
        if len(path_parts) >= 4 and path_parts[2] == 'reviews': return path_parts[3]
    except Exception: pass
    match = re.search(r'/reviews/(\w+)', href)
    return match.group(1) if match else "unknown_section"

def _parse_reviews_from_block(
    review_container_soup: BeautifulSoup, 
    start_date_filter: Optional[datetime],
    end_date_filter: Optional[datetime],
    thread_name_for_debug: str = "ParseReview"
) -> List[Dict]:
    reviews_found: List[Dict] = []
    
    direct_children_count = 0
    # print(f"        [{thread_name_for_debug}] _parse_reviews_from_block: Analyzing direct children of review_container_soup (type: {type(review_container_soup)}).")

    # Iterate through direct children and check if they are the review blocks
    for child_idx, child_tag in enumerate(review_container_soup.children):
        if hasattr(child_tag, 'name') and child_tag.name == 'div' and \
           child_tag.has_attr('class') and REVIEW_BLOCK_CSS_SELECTOR_CONFIG.split('.')[-1] in child_tag.get('class', []):
            direct_children_count += 1
            block = child_tag 

            quote = block.find('p', class_='cppRH-review-quote')
            if not quote:
                continue
            text = quote.get_text(strip=True).replace('\u0000', '')
            
            cite_block = block.find('cite', class_='cppRH-review-cite')
            date_meta_tag = None
            if cite_block:
                date_meta_tag = cite_block.find('meta', {'itemprop': 'datePublished'}) or \
                             cite_block.find('meta', attrs={'content': re.compile(r'^\d{4}-\d{2}-\d{2}$')})
                
            if not date_meta_tag or not date_meta_tag.get('content'):
                continue
            try:
                date_str = date_meta_tag['content']
                date_val = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                continue
                
            if start_date_filter and date_val < start_date_filter: continue
            if end_date_filter and date_val > end_date_filter: continue
                
            reviews_found.append({"text": text, "date": date_val})

    print(f"        [{thread_name_for_debug}] _parse_reviews_from_block: Processed {direct_children_count} direct 'div.cppRH' children. Found {len(reviews_found)} reviews.")

    if not reviews_found and not direct_children_count: 
        # This fallback might be too noisy if the structure is consistently direct children.
        # Let's keep it for now but be mindful of its output.
        # print(f"        [{thread_name_for_debug}] _parse_reviews_from_block: No direct 'div.cppRH' children found or yielded reviews. Trying find_all recursively again as a fallback.")
        review_blocks_fallback = review_container_soup.find_all('div', class_=REVIEW_BLOCK_CSS_SELECTOR_CONFIG, recursive=True)
        # print(f"        [{thread_name_for_debug}] _parse_reviews_from_block (Fallback): Found {len(review_blocks_fallback)} 'div.cppRH' (recursive=True).")
        if not review_blocks_fallback and len(str(review_container_soup)) < 5000:
             print(f"        [{thread_name_for_debug}] _parse_reviews_from_block: No cppRH blocks found even with recursive=True fallback. Segment was:\n{review_container_soup.prettify()[:1000]}")

        for block_idx, block in enumerate(review_blocks_fallback): 
            quote = block.find('p', class_='cppRH-review-quote')
            if not quote: continue
            text = quote.get_text(strip=True).replace('\u0000', '')
            cite_block = block.find('cite', class_='cppRH-review-cite')
            date_meta_tag = None
            if cite_block:
                date_meta_tag = cite_block.find('meta', {'itemprop': 'datePublished'}) or \
                             cite_block.find('meta', attrs={'content': re.compile(r'^\d{4}-\d{2}-\d{2}$')})
            if not date_meta_tag or not date_meta_tag.get('content'): continue
            try:
                date_str = date_meta_tag['content']
                date_val = datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError: continue
            if start_date_filter and date_val < start_date_filter: continue
            if end_date_filter and date_val > end_date_filter: continue
            reviews_found.append({"text": text, "date": date_val})
        if reviews_found and not direct_children_count: # If fallback found something and direct did not
            print(f"        [{thread_name_for_debug}] _parse_reviews_from_block: Found {len(reviews_found)} reviews using recursive fallback (direct search failed).")
            
    return reviews_found

def extract_company_info(soup: BeautifulSoup, company_base_url_str: str) -> Dict:
    # ... (same as before) ...
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
            if name_candidate and name_candidate.lower() not in REVIEW_CATEGORIES_CONFIG and len(name_candidate) > 3:
                details['company_name'] = name_candidate
        if details['company_name'] == default_name or details['company_name'].lower() in REVIEW_CATEGORIES_CONFIG:
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text(strip=True); name_from_title = title_text.split(" Reviews")[0].split(" | Comparably")[0].strip()
                if name_from_title and len(name_from_title) > 3 and name_from_title.lower() not in REVIEW_CATEGORIES_CONFIG:
                     details['company_name'] = name_from_title
    except Exception as e: print(f"Error extracting company details for {company_base_url_str}: {e}")
    return details

# --- Scraper Service Logic ---
def _scrape_category_deep_reviews_curl_cffi(
    company_base_url_str: str,
    category_name_arg: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    thread_name = f"CurlCffi-{category_name_arg}-{company_slug[:10]}"
    print(f"  [{thread_name}] Started for category: {category_name_arg}")

    collected_questions_for_this_category: List[Question] = []
    processed_reviews_keys_globally_for_category = set()
    
    base_curl_headers = {
        'User-Agent': get_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin', 
        'Sec-Fetch-User': '?1',
    }

    with CurlCffiSession(impersonate=CURL_IMPERSONATE_BROWSER_CONFIG, allow_redirects=True) as session:
        current_category_page_url = urljoin(company_base_url_str.rstrip('/') + "/", f"reviews/{category_name_arg}/")
        
        category_page_count = 0
        while category_page_count < MAX_CATEGORY_PAGES_CONFIG and current_category_page_url:
            category_page_count += 1
            print(f"  [{thread_name}] CurlCffi fetching Cat Page {category_page_count} (URL: {current_category_page_url})")
            
            current_referer = session.headers.get('Referer', company_base_url_str)
            fetch_headers = base_curl_headers.copy()
            fetch_headers['Referer'] = current_referer

            try:
                response_cat_page = session.get(
                    current_category_page_url,
                    headers=fetch_headers,
                    timeout=CURL_REQUEST_TIMEOUT_S_CONFIG
                )
                response_cat_page.raise_for_status()
                # --- UNCOMMENT THIS BLOCK TO SAVE HTML FOR DEBUGGING ---
                # try:
                #     debug_filename = f"DEBUG_cat_page_{category_name_arg}_{category_page_count}.html"
                #     with open(debug_filename, "wb") as f_debug:
                #         f_debug.write(response_cat_page.content)
                #     print(f"    [{thread_name}] DEBUG: Saved HTML for Cat Page {category_page_count} to {debug_filename}")
                # except Exception as e_debug_save:
                #     print(f"    [{thread_name}] DEBUG: Error saving HTML: {e_debug_save}")
                # --- END UNCOMMENT ---

                soup_current_category_page = BeautifulSoup(response_cat_page.content, 'lxml') 
                current_category_page_url_after_fetch = str(response_cat_page.url)
                session.headers.update({'Referer': current_category_page_url_after_fetch})

            except RequestsError as e_req:
                status_code_msg = f" (Status: {e_req.response.status_code})" if hasattr(e_req, 'response') and e_req.response else ""
                print(f"  [{thread_name}] CurlCffi Error{status_code_msg} fetching CATEGORY page {current_category_page_url}: {e_req}")
                break 
            except Exception as e_gen:
                print(f"  [{thread_name}] Generic Error fetching CATEGORY page {current_category_page_url}: {e_gen}")
                traceback.print_exc()
                break
            
            time.sleep(random.uniform(1.0, 2.0))

            # The main content area seems to be <div class="mReviews-section">
            # Within this, each question and its reviews are in <div data-question-id='...' class="reviewsPager">
            # And inside that, the actual list of reviews for that question is <div class="reviewsList">
            # So, we find all "reviewsPager" divs, then for each, find its "reviewsList"
            
            main_content_area = soup_current_category_page.find('div', class_='mReviews-section')
            if not main_content_area:
                print(f"    [{thread_name}] No 'div.mReviews-section' found on Cat Page {category_page_count}.")
                if category_page_count == 1:
                     print(f"  [{thread_name}] Initial category page for '{category_name_arg}' appears empty or inaccessible.")
                break

            question_pager_blocks = main_content_area.find_all('div', class_='reviewsPager', attrs={'data-question-id': True})
            print(f"    [{thread_name}] Found {len(question_pager_blocks)} 'div.reviewsPager' blocks on Cat Page {category_page_count}.")


            if not question_pager_blocks:
                if category_page_count == 1: # Check for any reviews if no question blocks
                     individual_review_blocks_on_page = soup_current_category_page.find_all('div', class_=REVIEW_BLOCK_CSS_SELECTOR_CONFIG) # Fallback
                     print(f"    [{thread_name}] No 'div.reviewsPager'. Found {len(individual_review_blocks_on_page)} individual 'div.cppRH' blocks instead.")
                     if not individual_review_blocks_on_page:
                        print(f"  [{thread_name}] Initial category page for '{category_name_arg}' appears empty or inaccessible (no reviewsPager or cppRH).")
                elif category_page_count > 1:
                    print(f"  [{thread_name}] No 'div.reviewsPager' found on Cat Page {category_page_count}, likely end.")
                break

            for q_pager_idx, q_pager_block_soup in enumerate(question_pager_blocks):
                # The q_block_soup should be the div.reviewsList *inside* the current q_pager_block_soup
                q_block_soup = q_pager_block_soup.find('div', class_='reviewsList')
                if not q_block_soup:
                    print(f"    [{thread_name}] Pager_Block {q_pager_idx+1} has no inner 'div.reviewsList'. Skipping.")
                    continue
                
                q_elem = q_block_soup.find('h2', class_='section-subtitle')
                if not q_elem:
                    print(f"    [{thread_name}] reviewsList within Pager_Block {q_pager_idx+1} has no 'h2.section-subtitle'. Skipping.")
                    continue
                question_text = q_elem.get_text(strip=True)
                print(f"    [{thread_name}] Q{q_pager_idx+1}: '{question_text[:60]}...'")

                all_reviews_for_this_q_pydantic: List[Review] = []
                current_q_reviews_html_segment = q_block_soup # This is now the div.reviewsList for the specific question
                current_q_reviews_source_url = current_category_page_url_after_fetch

                q_review_page_num = 0
                while q_review_page_num < MAX_REVIEW_PAGES_PER_QUESTION_CONFIG:
                    q_review_page_num += 1
                                        
                    reviews_data_from_current_segment = _parse_reviews_from_block(
                        current_q_reviews_html_segment, start_date_filter, end_date_filter, thread_name_for_debug=thread_name
                    )
                    
                    newly_added_this_q_sub_page_count = 0
                    for r_data in reviews_data_from_current_segment:
                        r_parsed = Review(**r_data)
                        r_key = (hash(question_text), hash(r_parsed.text), r_parsed.date)
                        if r_key not in processed_reviews_keys_globally_for_category:
                            all_reviews_for_this_q_pydantic.append(r_parsed)
                            processed_reviews_keys_globally_for_category.add(r_key)
                            newly_added_this_q_sub_page_count +=1
                    
                    if newly_added_this_q_sub_page_count > 0:
                        print(f"        [{thread_name}] Added {newly_added_this_q_sub_page_count} unique reviews for this Q (Q-Page {q_review_page_num}).")
                    elif q_review_page_num > 1 and not reviews_data_from_current_segment:
                        print(f"        [{thread_name}] No reviews on Q-Page {q_review_page_num} for '{question_text[:30]}...'.")
                    elif not reviews_data_from_current_segment and newly_added_this_q_sub_page_count == 0 and q_review_page_num == 1: 
                        print(f"        [{thread_name}] No reviews parsed from initial segment for Q-Page {q_review_page_num}.")

                    next_q_review_page_href = None
                    # Pagination for Q-reviews is inside the 'div.reviewsPager' (q_pager_block_soup)
                    # specifically in its 'div.pager_footer'
                    pagination_scope_for_q = q_pager_block_soup.find('div', class_='pager_footer') or \
                                             q_pager_block_soup # Fallback to the pager block itself

                    if pagination_scope_for_q:
                        for sel in NEXT_PAGE_SELECTORS_CONFIG: # Use general next page selectors
                            buttons = pagination_scope_for_q.select(sel)
                            for btn_tag in buttons:
                                href = btn_tag.get('href')
                                
                                aria_label_str_q_btn = btn_tag.get("aria-label", "")
                                rel_value_q_btn = btn_tag.get("rel")
                                rel_str_q_btn = ""
                                if isinstance(rel_value_q_btn, list):
                                    rel_str_q_btn = " ".join(rel_value_q_btn)
                                elif rel_value_q_btn: 
                                    rel_str_q_btn = rel_value_q_btn
                                text_content_str_q_btn = btn_tag.get_text(strip=True)
                                combined_test_str_q_btn = f"{aria_label_str_q_btn} {rel_str_q_btn} {text_content_str_q_btn}".lower()
                                current_btn_is_prev = "prev" in combined_test_str_q_btn
                                
                                current_btn_is_disabled = any(cls in (btn_tag.get('class', [])) for cls in ['disabled', 'inactive']) or btn_tag.has_attr('disabled')
                                
                                if current_btn_is_prev or current_btn_is_disabled: continue
                                
                                if href and href != "#" and not href.startswith("javascript:"):
                                    next_q_review_page_href = urljoin(current_q_reviews_source_url, href)
                                    break
                            if next_q_review_page_href: break
                    
                    if not next_q_review_page_href:
                        break 

                    try:
                        time.sleep(random.uniform(0.7, 1.2)) 
                        q_review_fetch_headers = base_curl_headers.copy()
                        q_review_fetch_headers['Referer'] = current_q_reviews_source_url
                        
                        response_q_review_page = session.get(next_q_review_page_href, headers=q_review_fetch_headers, timeout=CURL_REQUEST_TIMEOUT_S_CONFIG)
                        response_q_review_page.raise_for_status()
                        
                        # When fetching next Q-review page, the new segment is the *entire new page's reviewsList for that question*
                        # We need to find the specific question block again on this new page.
                        new_page_soup = BeautifulSoup(response_q_review_page.content, 'lxml')
                        current_q_reviews_source_url = str(response_q_review_page.url)

                        # Re-find the specific question's review list on the new page
                        # Assuming the data-question-id attribute is stable on the reviewsPager div
                        current_data_question_id = q_pager_block_soup.get('data-question-id')
                        if current_data_question_id:
                            new_q_pager_block = new_page_soup.find('div', class_='reviewsPager', attrs={'data-question-id': current_data_question_id})
                            if new_q_pager_block:
                                current_q_reviews_html_segment = new_q_pager_block.find('div', class_='reviewsList')
                                if not current_q_reviews_html_segment:
                                    print(f"        [{thread_name}] WARNING: Fetched Q-REVIEW page {next_q_review_page_href} but could not find reviewsList for question ID {current_data_question_id}.")
                                    current_q_reviews_html_segment = BeautifulSoup("", 'lxml') # Empty to stop
                                    break
                            else:
                                print(f"        [{thread_name}] WARNING: Fetched Q-REVIEW page {next_q_review_page_href} but could not find reviewsPager for question ID {current_data_question_id}.")
                                current_q_reviews_html_segment = BeautifulSoup("", 'lxml') 
                                break
                        else: # Fallback if data-question-id wasn't found on original block (should not happen ideally)
                             print(f"        [{thread_name}] WARNING: Could not get data-question-id from original q_pager_block_soup. Q-review pagination might be unreliable.")
                             # This might be too broad, but let's try taking the first reviewsList
                             potential_first_list = new_page_soup.find('div', class_='reviewsList')
                             if potential_first_list and potential_first_list.find('h2', class_='section-subtitle').get_text(strip=True) == question_text:
                                 current_q_reviews_html_segment = potential_first_list
                             else: # If it's not the same question, it's likely a category page.
                                print(f"        [{thread_name}] WARNING: Fetched Q-REVIEW page {next_q_review_page_href} seems to be a different page structure or full category page. Stopping Q-pagination.")
                                current_q_reviews_html_segment = BeautifulSoup("", 'lxml') 
                                break
                                 
                    except RequestsError as e_q_rev_req:
                        status_code_msg = f" (Status: {e_q_rev_req.response.status_code})" if hasattr(e_q_rev_req, 'response') and e_q_rev_req.response else ""
                        print(f"        [{thread_name}] curl_cffi Error{status_code_msg} fetching Q-REVIEW page {next_q_review_page_href}: {e_q_rev_req}")
                        break 
                    except Exception as e_gen:
                        print(f"        [{thread_name}] Generic Error Q-REVIEW page {next_q_review_page_href}: {e_gen}")
                        traceback.print_exc()
                        break
                
                if all_reviews_for_this_q_pydantic:
                    all_reviews_for_this_q_pydantic.sort(key=lambda r: r.date, reverse=True)
                    existing_q_obj = next((q for q in collected_questions_for_this_category if q.question_text == question_text), None)
                    if existing_q_obj: 
                        for r_new in all_reviews_for_this_q_pydantic:
                            if not any(er.text == r_new.text and er.date == r_new.date for er in existing_q_obj.review_section.reviews):
                                existing_q_obj.review_section.reviews.append(r_new)
                        existing_q_obj.review_section.reviews.sort(key=lambda r: r.date, reverse=True)
                    else:
                        print(f"    [{thread_name}] Creating ReviewSection with section_name: {category_name_arg} for Q: '{question_text[:30]}...' ({len(all_reviews_for_this_q_pydantic)} reviews)")
                        review_section = ReviewSection(section_name=category_name_arg, reviews=all_reviews_for_this_q_pydantic)
                        question_obj = Question(question_text=question_text, review_section=review_section)
                        collected_questions_for_this_category.append(question_obj)
                else:
                    print(f"    [{thread_name}] No reviews collected for Q: '{question_text[:60]}...' after trying {q_review_page_num} Q-pages.")

            next_category_page_href_found = None
            # For CATEGORY pagination, look for pager outside of any specific question's reviewsPager
            # It's typically under <div class="boxed filters"> <div class="mReviews"> then a pager_footer or similar
            mreviews_section = soup_current_category_page.find('div', class_='mReviews-section')
            cat_page_nav_scope = None
            if mreviews_section:
                # Find pager_footer that is a direct child of mReviews-section or its child pager
                # and NOT inside a reviewsPager (which is for Q-reviews)
                potential_pagers = mreviews_section.find_all('div', class_='pager_footer', recursive=True)
                for pp in potential_pagers:
                    if not pp.find_parent('div', class_='reviewsPager'):
                        cat_page_nav_scope = pp
                        break
            
            if not cat_page_nav_scope: # Fallback if specific structure not found
                cat_page_nav_scope = soup_current_category_page.find('div', class_='pager_controls') or \
                                     soup_current_category_page.find('nav', attrs={'aria-label': lambda x: x and 'pagination' in x.lower()}) or \
                                     soup_current_category_page.find('ul', class_=lambda x: x and 'pagination' in x.lower()) or \
                                     soup_current_category_page # Last resort
            
            for sel in NEXT_PAGE_SELECTORS_CONFIG:
                potential_btns_soup = cat_page_nav_scope.select(sel)
                for btn_s in potential_btns_soup:
                    # Double check it's not Q-level pagination
                    if btn_s.find_parent('div', class_='reviewsPager'): 
                        continue
                    
                    aria_label_str_cat_btn = btn_s.get("aria-label", "")
                    rel_value_cat_btn = btn_s.get("rel")
                    rel_str_cat_btn = ""
                    if isinstance(rel_value_cat_btn, list):
                        rel_str_cat_btn = " ".join(rel_value_cat_btn)
                    elif rel_value_cat_btn:
                        rel_str_cat_btn = rel_value_cat_btn
                    text_content_str_cat_btn = btn_s.get_text(strip=True)
                    combined_test_str_cat_btn = f"{aria_label_str_cat_btn} {rel_str_cat_btn} {text_content_str_cat_btn}".lower()
                    current_cat_btn_is_prev = "prev" in combined_test_str_cat_btn
                    
                    current_cat_btn_is_disabled = any(cls in (btn_s.get('class', [])) for cls in ['disabled', 'inactive']) or btn_s.has_attr('disabled')
                    
                    href_val = btn_s.get('href')
                    if not current_cat_btn_is_prev and not current_cat_btn_is_disabled and href_val and href_val != '#':
                        next_category_page_href_found = urljoin(current_category_page_url_after_fetch, href_val)
                        break 
                if next_category_page_href_found: break
            
            # print(f"    [{thread_name}] Next category page Href found: {next_category_page_href_found}")

            if not next_category_page_href_found:
                print(f"  [{thread_name}] No 'Next Category Page' link found in HTML after Cat Page {category_page_count}.")
                current_category_page_url = None
            else:
                current_category_page_url = next_category_page_href_found

    print(f"  [{thread_name}] Finished category '{category_name_arg}'. Total Qs: {len(collected_questions_for_this_category)}")
    return category_name_arg, collected_questions_for_this_category


def scrape_comparably_sync(
    company_base_url_str: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating Curl-CFFI parallel category scrape for: {company_slug}")
    start_time_total = time.time()
    all_questions_for_company: List[Question] = []
    company_details_overall: Dict[str, Any] = {}
    
    initial_fetch_url = urljoin(company_base_url_str.rstrip('/') + "/", f"reviews/{REVIEW_CATEGORIES_CONFIG[0]}/")
    print(f"  [{company_slug}] Fetching initial company info with Curl-CFFI from: {initial_fetch_url}")
    
    initial_headers = {
        'User-Agent': get_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    try:
        with CurlCffiSession(impersonate=CURL_IMPERSONATE_BROWSER_CONFIG, allow_redirects=True) as info_session:
            response_info = info_session.get(initial_fetch_url, headers=initial_headers, timeout=CURL_REQUEST_TIMEOUT_S_CONFIG)
            response_info.raise_for_status()
            info_html = response_info.content
            info_soup = BeautifulSoup(info_html, 'lxml')
            
            page_title = info_soup.title.string if info_soup.title else ""
            if "Error" in page_title or "Not Found" in page_title or "Access Denied" in info_soup.get_text():
                print(f"  [{company_slug}] Initial page {initial_fetch_url} had error indicators (Title: {page_title}), trying base URL: {company_base_url_str}")
                response_info_base = info_session.get(company_base_url_str, headers=initial_headers, timeout=CURL_REQUEST_TIMEOUT_S_CONFIG)
                response_info_base.raise_for_status()
                info_html_base = response_info_base.content
                info_soup_base = BeautifulSoup(info_html_base, 'lxml')
                page_title_base = info_soup_base.title.string if info_soup_base.title else ""
                if "Error" in page_title_base or "Not Found" in page_title_base or "Access Denied" in info_soup_base.get_text():
                     raise Exception(f"Could not load a valid page for company info (Title: {page_title_base})")
                company_details_overall = extract_company_info(info_soup_base, company_base_url_str)
            else:
                company_details_overall = extract_company_info(info_soup, company_base_url_str)
            
            print(f"  [{company_slug}] Initial company info fetched: Name='{company_details_overall.get('company_name')}'")
    except RequestsError as e_req:
        status_code_msg = f" (Status: {e_req.response.status_code})" if hasattr(e_req, 'response') and e_req.response else ""
        print(f"  [{company_slug}] CurlCffi Error{status_code_msg} fetching initial company info: {e_req}")
        company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": f"Initial info fetch error (Request): {str(e_req)}"}
    except Exception as e_info:
        print(f"  [{company_slug}] Error fetching initial company info: {e_info}")
        company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": f"Initial info fetch error: {str(e_info)}"}

    max_concurrent_categories = min(len(REVIEW_CATEGORIES_CONFIG), 5)
    print(f"  [{company_slug}] Starting Curl-CFFI parallel scrape for {len(REVIEW_CATEGORIES_CONFIG)} categories (max {max_concurrent_categories} concurrent)...")
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories, thread_name_prefix="CurlCffiPool") as executor:
        for cat_name_from_list in REVIEW_CATEGORIES_CONFIG: 
            future = executor.submit(
                _scrape_category_deep_reviews_curl_cffi,
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
                if processed_cat_name != original_category_name_processed:
                     print(f"  [{company_slug}] WARNING: Mismatch in returned category name. Expected '{original_category_name_processed}', got '{processed_cat_name}'.")
                
                if questions_from_category:
                    print(f"  [{company_slug}] Received {len(questions_from_category)} Qs from cat '{original_category_name_processed}'. Merging...")
                    all_questions_for_company.extend(questions_from_category)
                else:
                    print(f"  [{company_slug}] Cat '{original_category_name_processed}' returned no Qs.")
            except Exception as e_future_exc:
                print(f"  [{company_slug}] Curl-CFFI Category task for '{original_category_name_processed}' FAILED in executor: {e_future_exc}")
                traceback.print_exc()

    total_duration = time.time() - start_time_total
    print(f"\nFinished ALL Curl-CFFI scrapes for {company_slug} in {total_duration:.2f}s. Total Qs collected: {len(all_questions_for_company)}")
    
    if not company_details_overall.get("company_name") or company_details_overall.get("company_name", "").lower() == company_slug.lower() or company_details_overall.get("company_name", "") == "unknown_company":
        current_name = company_details_overall.get("company_name", "unknown_company")
        fallback_name = company_slug.replace('-', ' ').title()
        if current_name.lower() in REVIEW_CATEGORIES_CONFIG or current_name == "unknown_company":
             company_details_overall["company_name"] = fallback_name
             if "status_note" not in company_details_overall: company_details_overall["status_note"] = "Name set to fallback slug-based name."

    return {
        "status": "success" if all_questions_for_company or (company_details_overall.get("company_name") != company_slug.replace('-', ' ').title() and company_details_overall.get("company_name") != "unknown_company") else "partial_success_no_reviews",
        "data": {
            "company_info": company_details_overall,
            "reviews": [q.model_dump(mode='json') for q in all_questions_for_company]
        }
    }

# --- FastAPI App Initialization ---
app = FastAPI(
    title=API_TITLE_CONFIG,
    description=API_DESCRIPTION_CONFIG,
    version=API_VERSION_CONFIG
)

@app.post("/scrape")
async def scrape_companies_endpoint(request: ScrapeRequest = Body(...)) -> Dict[str, Dict[str, Any]]:
    urls = request.urls
    start_date_filter: Optional[datetime] = None
    end_date_filter: Optional[datetime] = None

    if request.start_date_str:
        try:
            start_date_filter = datetime.strptime(request.start_date_str, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid start_date_str. Use YYYY-MM-DD.")
    if request.end_date_str:
        try:
            end_date_filter = datetime.strptime(request.end_date_str, "%Y-%m-%d")
            end_date_filter = end_date_filter.replace(hour=23, minute=59, second=59)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid end_date_str. Use YYYY-MM-DD.")

    if start_date_filter and end_date_filter and start_date_filter > end_date_filter:
        raise HTTPException(status_code=400, detail="start_date_str cannot be after end_date_str.")

    if not urls:
        raise HTTPException(status_code=400, detail="No URLs provided.")

    results: Dict[str, Dict[str, Any]] = {}
    valid_scrape_params: List[Dict[str, str]] = []

    print(f"API request: {len(urls)} URLs, Method: Curl-CFFI Only (v{API_VERSION_CONFIG}).")

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

    scraped_results_or_exceptions = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []

    task_idx = 0
    for params in valid_scrape_params:
        original_url_str = params['original_url']
        if original_url_str in results:
            continue
        
        if task_idx < len(scraped_results_or_exceptions):
            result_or_exc = scraped_results_or_exceptions[task_idx]
            if isinstance(result_or_exc, Exception):
                print(f"Task for {original_url_str} EXCEPTION (type: {type(result_or_exc).__name__}): {result_or_exc}")
                tb_str = "".join(traceback.format_exception(None, result_or_exc, result_or_exc.__traceback__))
                print(f"FULL TRACEBACK for {original_url_str} (CurlCffiOnly):\n{tb_str}")
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed. Type: {type(result_or_exc).__name__}. Check logs."}
            elif isinstance(result_or_exc, dict):
                results[original_url_str] = result_or_exc
            else:
                results[original_url_str] = {"status": "error", "message": "Unexpected internal result type from scraping task"}
            task_idx += 1
        else:
            results[original_url_str] = {"status": "error", "message": "Scraping task result missing (logic error)."}

    print(f"Finished API request processing (CurlCffiOnly v{API_VERSION_CONFIG}).")
    return results


@app.get("/")
async def read_root():
    return {"message": f"Welcome to the {API_TITLE_CONFIG} - v{API_VERSION_CONFIG}"}

# --- Main execution block (for running directly) ---
if __name__ == "__main__":
    async def main_test():
        test_company_url = "https://www.comparably.com/companies/google"
        test_start_date = None 
        test_end_date = None 

        start_dt = datetime.strptime(test_start_date, "%Y-%m-%d") if test_start_date else None
        end_dt = datetime.strptime(test_end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59) if test_end_date else None
        
        parsed = urlparse(test_company_url)
        slug = parsed.path.strip('/').split('/')[-1]
        base_url = f"{parsed.scheme}://{parsed.netloc}/companies/{slug}"

        print(f"Testing scraper for {slug} at {base_url}")
        result = scrape_comparably_sync(base_url, slug, start_dt, end_dt)
        
        output_file = f"{slug}_reviews_direct_test_curl_cffi.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=4, ensure_ascii=False, default=str)
        print(f"Test results saved to {output_file}")

    # --- UNCOMMENT TO RUN THE DIRECT TEST ---
    asyncio.run(main_test())
    # --- OR RUN THE FASTAPI APP ---
    # try:
    #     import uvicorn
    #     print("\nTo run the FastAPI app, use: uvicorn your_script_name:app --reload")
    #     print("Example: uvicorn your_script_name.py:app --reload\n")
        # uvicorn.run(app, host="0.0.0.0", port=8000) 
    # except ImportError:
    #     print("Uvicorn not installed. Cannot run FastAPI app directly. Install with: pip install uvicorn[standard]")