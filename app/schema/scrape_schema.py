from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, HttpUrl

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