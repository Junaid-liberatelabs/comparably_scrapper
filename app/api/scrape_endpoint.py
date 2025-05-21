# scrapper/app/api/scrape_endpoint.py

import asyncio
import traceback
from typing import Dict, Any, Optional, List
from datetime import datetime
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Body

from app.schema.scrape_schema import ScrapeRequest
from app.service.comparably_scraper_service import scrape_comparably_sync
from app.core.config import API_VERSION # To log correct version

router = APIRouter()

@router.post("/scrape", tags=["Scraping"])
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

    print(f"API request: {len(urls)} URLs, Selenium CatNav & Curl Q-ReviewNav (v{API_VERSION}).") # Using API_VERSION from config

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
            # scrape_comparably_sync is a synchronous (blocking) function
            # asyncio.to_thread runs it in a separate thread, making the endpoint non-blocking
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
        if original_url_str in results:  # Already processed due to parsing error
            continue
        
        if task_idx < len(scraped_results_or_exceptions):
            result_or_exc = scraped_results_or_exceptions[task_idx]
            if isinstance(result_or_exc, Exception):
                print(f"Task for {original_url_str} EXCEPTION (type: {type(result_or_exc).__name__}): {result_or_exc}")
                tb_str = "".join(traceback.format_exception(None, result_or_exc, result_or_exc.__traceback__))
                print(f"FULL TRACEBACK for {original_url_str} (SelCatCurlQ):\n{tb_str}")
                results[original_url_str] = {"status": "error", "message": f"Scraping task failed. Type: {type(result_or_exc).__name__}. Check logs."}
            elif isinstance(result_or_exc, dict):
                results[original_url_str] = result_or_exc
            else:
                results[original_url_str] = {"status": "error", "message": "Unexpected internal result type from scraping task"}
            task_idx += 1
        else:
            # This case should ideally not be reached if logic is correct
            results[original_url_str] = {"status": "error", "message": "Scraping task result missing (logic error)."}

    print(f"Finished API request processing (SelCatCurlQ v{API_VERSION}).")
    return results