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
    campaign_id: str
    status: str  # Keep as string to match original sync_once.py

@dataclass
class Lead:
    """Represents a lead from BigQuery for processing."""
    email: str
    domain: str
    sequence_target: str
    annual_revenue: Optional[float] = None
    location: Optional[str] = None
    country_code: Optional[str] = None