# scrapper/app/service/comparably_scraper_service.py

import time
import random
import traceback
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from curl_cffi.requests import Session as CurlCffiSession, RequestsError

from app.schema.scrape_schema import Review, ReviewSection, Question # Pydantic models
from app.core.config import ( # Constants
    REVIEW_CATEGORIES, MAX_CATEGORY_PAGES, MAX_REVIEW_PAGES_PER_QUESTION,
    SELENIUM_ELEMENT_TIMEOUT_S, CURL_REQUEST_TIMEOUT_S, CURL_IMPERSONATE_BROWSER,
    NEXT_PAGE_SELECTORS
)
from app.utils.scraper_helpers import ( # Helper functions
    setup_selenium_driver, _parse_reviews_from_block, extract_company_info
)

def _scrape_category_deep_reviews_selenium_curl(
    company_base_url_str: str,
    category_name_arg: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Tuple[str, List[Question]]:
    thread_name = f"SelCatCurlQ-{category_name_arg}-{company_slug[:10]}"
    print(f"  [{thread_name}] Started for category: {category_name_arg}")

    collected_questions_for_this_category: List[Question] = []
    processed_reviews_keys_globally_for_category = set()

    category_driver = None
    try:
        category_driver = setup_selenium_driver()
        category_wait = WebDriverWait(category_driver, SELENIUM_ELEMENT_TIMEOUT_S)
        category_button_wait = WebDriverWait(category_driver, max(5, SELENIUM_ELEMENT_TIMEOUT_S // 3))

        category_url_start = urljoin(company_base_url_str.rstrip('/') + "/", f"reviews/{category_name_arg}/")
        print(f"  [{thread_name}] Selenium navigating to initial Cat Page: {category_url_start}")
        category_driver.get(category_url_start)
        time.sleep(random.uniform(1.5, 2.5))

        user_agent_hdr = category_driver.execute_script("return navigator.userAgent;")
        base_curl_headers = {
            'User-Agent': user_agent_hdr,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        }

        category_page_count = 0
        while category_page_count < MAX_CATEGORY_PAGES:
            category_page_count += 1
            current_category_page_url = category_driver.current_url
            print(f"  [{thread_name}] Selenium on Cat Page {category_page_count} (URL: {current_category_page_url})")

            try:
                category_wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.reviewsList, div.cppRH"))
                )
            except TimeoutException:
                print(f"  [{thread_name}] Timeout waiting for review content on Cat Page {category_page_count}.")
                if category_page_count == 1:
                    print(f"  [{thread_name}] Initial category page for '{category_name_arg}' appears empty or inaccessible.")
                break

            current_selenium_cookies = {c['name']: c['value'] for c in category_driver.get_cookies()}
            soup_current_category_page = BeautifulSoup(category_driver.page_source, 'html.parser')

            question_blocks_on_cat_page = soup_current_category_page.find_all('div', class_='reviewsList')
            if not question_blocks_on_cat_page and category_page_count > 1:
                 print(f"  [{thread_name}] No 'div.reviewsList' found on Cat Page {category_page_count}, was likely end.")
                 break

            for q_block_idx, q_block_soup in enumerate(question_blocks_on_cat_page):
                q_elem = q_block_soup.find('h2', class_='section-subtitle')
                if not q_elem: continue
                question_text = q_elem.get_text(strip=True)
                print(f"    [{thread_name}] Q{q_block_idx+1}: '{question_text[:60]}...'")

                all_reviews_for_this_q_pydantic: List[Review] = [] # For Pydantic models
                current_q_reviews_html_segment = q_block_soup
                current_q_reviews_source_url = current_category_page_url

                with CurlCffiSession(impersonate=CURL_IMPERSONATE_BROWSER) as curl_q_session:
                    curl_q_session.cookies.update(current_selenium_cookies)

                    q_review_page_num = 0
                    while q_review_page_num < MAX_REVIEW_PAGES_PER_QUESTION:
                        q_review_page_num += 1

                        # _parse_reviews_from_block now returns list of dicts
                        reviews_data_from_current_segment = _parse_reviews_from_block(
                            current_q_reviews_html_segment, start_date_filter, end_date_filter
                        )

                        newly_added_this_q_sub_page_count = 0
                        for r_data in reviews_data_from_current_segment:
                            # Convert dict to Pydantic model here
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


                        next_q_review_page_href = None
                        pagination_scope_for_q = current_q_reviews_html_segment.find(['nav', 'ul', 'div'],
                            class_=lambda x: x and any(p in x.lower() for p in ['pagination', 'pager', 'page-links', 'qa-Pagination', 'cp-Pagination']),
                            recursive=True
                        ) or current_q_reviews_html_segment

                        for sel in NEXT_PAGE_SELECTORS:
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

                        if not next_q_review_page_href: break

                        try:
                            time.sleep(random.uniform(0.7, 1.5))
                            q_review_fetch_headers = base_curl_headers.copy()
                            q_review_fetch_headers['Referer'] = current_q_reviews_source_url

                            response_q_review_page = curl_q_session.get(next_q_review_page_href, headers=q_review_fetch_headers, timeout=CURL_REQUEST_TIMEOUT_S)
                            response_q_review_page.raise_for_status()

                            current_q_reviews_html_segment = BeautifulSoup(response_q_review_page.text, 'html.parser')
                            current_q_reviews_source_url = str(response_q_review_page.url)
                            if current_q_reviews_html_segment.find('h2', class_='section-subtitle'):
                                print(f"        [{thread_name}] WARNING: Fetched Q-review page {next_q_review_page_href} looks like a full category page. Stopping Q-pagination.")
                                current_q_reviews_html_segment = BeautifulSoup("", 'html.parser')
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
                        print(f"    [{thread_name}] Creating ReviewSection with section_name: {category_name_arg} for Q: '{question_text[:30]}'")
                        review_section = ReviewSection(section_name=category_name_arg, reviews=all_reviews_for_this_q_pydantic)
                        question_obj = Question(question_text=question_text, review_section=review_section)
                        collected_questions_for_this_category.append(question_obj)


            next_category_page_button = None
            cat_page_nav_scope = soup_current_category_page.find('nav', attrs={'aria-label': lambda x: x and 'pagination' in x.lower()}) or \
                                 soup_current_category_page.find('ul', class_=lambda x: x and 'pagination' in x.lower()) or \
                                 soup_current_category_page

            for sel in NEXT_PAGE_SELECTORS:
                potential_btns_soup = cat_page_nav_scope.select(sel)
                for btn_s in potential_btns_soup:
                    if btn_s.find_parent('div', class_='reviewsList'): continue

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

                    if not current_cat_btn_is_prev and not current_cat_btn_is_disabled and btn_s.get('href') and btn_s.get('href') != '#':
                        try:
                            selenium_btns = category_driver.find_elements(By.CSS_SELECTOR, sel)
                            for sel_btn in selenium_btns:
                                if sel_btn.is_displayed() and sel_btn.get_attribute('href') == urljoin(current_category_page_url, btn_s.get('href')):
                                    try:
                                        sel_btn.find_element(By.XPATH, "./ancestor::div[contains(@class, 'reviewsList')]")
                                        continue
                                    except NoSuchElementException:
                                        pass

                                    sel_aria_label = (sel_btn.get_attribute("aria-label") or "").lower()
                                    sel_rel = (sel_btn.get_attribute("rel") or "").lower()
                                    if "prev" not in sel_aria_label and "prev" not in sel_rel :
                                        next_category_page_button = category_button_wait.until(EC.element_to_be_clickable(sel_btn))
                                        if next_category_page_button: break
                            if next_category_page_button: break
                        except (NoSuchElementException, TimeoutException): continue
                if next_category_page_button: break

            if not next_category_page_button :
                print(f"  [{thread_name}] No clickable 'Next Category Page' button found by Selenium after Cat Page {category_page_count}.")
                break

            print(f"  [{thread_name}] Selenium clicking 'Next Category Page'...")
            try:
                category_driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});", next_category_page_button)
                time.sleep(0.3)
                next_category_page_button.click()
                time.sleep(random.uniform(2.0, 3.5))
            except ElementClickInterceptedException:
                print(f"  [{thread_name}] Click intercepted, trying JS click for 'Next Category Page'...")
                category_driver.execute_script("arguments[0].click();", next_category_page_button)
                time.sleep(random.uniform(2.0, 3.5))
            except Exception as e_click:
                print(f"  [{thread_name}] Error clicking 'Next Category Page': {e_click}")
                break

    except Exception as e_cat_main:
        print(f"  [{thread_name}] MAJOR ERROR in category '{category_name_arg}': {e_cat_main}")
        traceback.print_exc()
    finally:
        if category_driver:
            category_driver.quit()

    print(f"  [{thread_name}] Finished category '{category_name_arg}'. Total Qs: {len(collected_questions_for_this_category)}")
    return category_name_arg, collected_questions_for_this_category


def scrape_comparably_sync(
    company_base_url_str: str,
    company_slug: str,
    start_date_filter: Optional[datetime] = None,
    end_date_filter: Optional[datetime] = None
) -> Dict[str, Any]:
    print(f"Orchestrating SELENIUM_CAT_CURL_Q_REVIEW parallel category scrape for: {company_slug}")
    start_time_total = time.time()
    all_questions_for_company: List[Question] = []
    company_details_overall: Dict[str, Any] = {}
    initial_info_driver = None

    try:
        print(f"  [{company_slug}] Fetching initial company info with Selenium...")
        initial_info_driver = setup_selenium_driver()
        info_fetch_url = urljoin(company_base_url_str.rstrip('/') + "/", "reviews/")
        initial_info_driver.get(info_fetch_url)
        time.sleep(random.uniform(1.8,2.5))
        if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title or "Access Denied" in initial_info_driver.page_source:
            print(f"  [{company_slug}] /reviews/ page for info failed (Title: {initial_info_driver.title}), trying base URL: {company_base_url_str}")
            initial_info_driver.get(company_base_url_str)
            time.sleep(random.uniform(1.8,2.5))
            if "Error" in initial_info_driver.title or "Not Found" in initial_info_driver.title or "Access Denied" in initial_info_driver.page_source:
                raise Exception(f"Could not load a valid page for company info (Title: {initial_info_driver.title})")
        info_html = initial_info_driver.page_source
        info_soup = BeautifulSoup(info_html, 'html.parser')
        company_details_overall = extract_company_info(info_soup, company_base_url_str) # From helpers
        print(f"  [{company_slug}] Initial company info fetched: Name='{company_details_overall.get('company_name')}'")
    except Exception as e_info:
        print(f"  [{company_slug}] Error fetching initial company info: {e_info}")
        company_details_overall = {"company_name": company_slug.replace('-', ' ').title(), "comparably_url": company_base_url_str, "status_note": f"Initial info fetch error: {str(e_info)}"}
    finally:
        if initial_info_driver:
            try: initial_info_driver.quit()
            except Exception as e_close: print(f"  [{company_slug}] Error closing Selenium info browser: {e_close}")

    max_concurrent_categories = min(len(REVIEW_CATEGORIES), 3)
    print(f"  [{company_slug}] Starting SELENIUM_CAT_CURL_Q_REVIEW parallel scrape for {len(REVIEW_CATEGORIES)} categories (max {max_concurrent_categories} concurrent)...")
    futures_map = {}
    with ThreadPoolExecutor(max_workers=max_concurrent_categories, thread_name_prefix="SelCatCurlQPool") as executor:
        for cat_name_from_list in REVIEW_CATEGORIES:
            future = executor.submit(
                _scrape_category_deep_reviews_selenium_curl,
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
                print(f"  [{company_slug}] SELENIUM_CAT_CURL_Q_REVIEW Category task for '{original_category_name_processed}' FAILED in executor: {e_future_exc}")
                traceback.print_exc()

    total_duration = time.time() - start_time_total
    print(f"\nFinished ALL SELENIUM_CAT_CURL_Q_REVIEW scrapes for {company_slug} in {total_duration:.2f}s. Total Qs collected: {len(all_questions_for_company)}")

    if not company_details_overall.get("company_name") or company_details_overall.get("company_name", "").lower() == company_slug.lower() or company_details_overall.get("company_name", "") == "unknown_company":
        current_name = company_details_overall.get("company_name", "unknown_company")
        fallback_name = company_slug.replace('-', ' ').title()
        if current_name.lower() in REVIEW_CATEGORIES or current_name == "unknown_company":
             company_details_overall["company_name"] = fallback_name
             if "status_note" not in company_details_overall: company_details_overall["status_note"] = "Name set to fallback slug-based name."

    return {
        "status": "success" if all_questions_for_company or (company_details_overall.get("company_name") != company_slug.replace('-', ' ').title() and company_details_overall.get("company_name") != "unknown_company") else "partial_success_no_reviews",
        "data": {
            "company_info": company_details_overall,
            "reviews": [q.model_dump(mode='json') for q in all_questions_for_company] # Use Pydantic's model_dump
        }
    }