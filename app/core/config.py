API_TITLE = "Comparably Scraper API - Selenium CatNav, Curl-CFFI Q-ReviewNav"
API_DESCRIPTION = "Selenium for Category page navigation, Curl-CFFI for Q-Review pagination."
API_VERSION = "2.1.0" # Version update for refactor

REVIEW_CATEGORIES = ["leadership", "compensation", "team", "environment", "outlook"]
MAX_CATEGORY_PAGES = 15
MAX_REVIEW_PAGES_PER_QUESTION = 20
SELENIUM_PAGE_TIMEOUT_S = 30
SELENIUM_ELEMENT_TIMEOUT_S = 18  # For waiting for elements on a page
CURL_REQUEST_TIMEOUT_S = 30
CURL_IMPERSONATE_BROWSER = "chrome110"

# Order can matter; more specific or reliable ones first
NEXT_PAGE_SELECTORS = [
    "a.qa-PaginationPageLink-Next",  # Comparably specific observed
    "a.pagination-link[rel='next']", "a[aria-label*='Next Page' i]", "a[title*='Next Page' i]",
    "li.pagination-next > a", "a.pagination-next", "a.NextPageLink",
    "nav[aria-label*='pagination' i] li:last-child a[href]",
    ".page-next > a", "a.next"
]
REVIEW_BLOCK_CSS_SELECTOR = "div.cppRH" # Though not directly used by config, good to keep with scraping constants