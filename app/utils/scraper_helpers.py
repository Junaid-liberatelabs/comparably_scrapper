import re
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urlparse
import traceback

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from app.core.config import SELENIUM_PAGE_TIMEOUT_S, REVIEW_CATEGORIES # Import constants

# --- User Agent ---
try:
    from fake_useragent import UserAgent
    ua_instance = UserAgent()
except ImportError:
    print("Warning: fake-useragent not installed. Using a generic User-Agent.")
    ua_instance = None

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
    # Potentially add thread_name here for more specific logging if needed
) -> List[Dict]: # Returning list of dicts to be converted to Review model later
    reviews_found: List[Dict] = []
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
        try:
            date_str = date_meta_tag['content']
            date_val = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            # print(f"  [ParseReviewBlock] Error parsing date: {date_str} for review text: {text[:50]}...") # Example debug
            continue
        if start_date_filter and date_val < start_date_filter: continue
        if end_date_filter and date_val > end_date_filter: continue
        reviews_found.append({"text": text, "date": date_val}) # Store as dict
    return reviews_found

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

def setup_selenium_driver() -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("window-size=1920,1080")
    user_agent_str = ua_instance.random if ua_instance else "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
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