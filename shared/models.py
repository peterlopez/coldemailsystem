"""
Data models for Cold Email System.
Contains data classes and structures used across the system.
"""

from dataclasses import dataclass
from typing import Optional

@dataclass
class InstantlyLead:
    """Represents a lead from Instantly API."""
    id: str
    email: str
    status: int
    campaign_id: str
    email_reply_count: int = 0
    created_at: Optional[str] = None
    
    def __post_init__(self):
        # Store original status for drain tracking
        self.status = self.status  # Keep as string for classification

@dataclass
class Lead:
    """Represents a lead from BigQuery for processing."""
    email: str
    domain: str
    sequence_target: str
    annual_revenue: Optional[float] = None
    location: Optional[str] = None
    country_code: Optional[str] = None