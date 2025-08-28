#!/usr/bin/env python3
"""
Email Validation System
Mimics Instantly.ai's internal validation logic instead of using their restrictive API.

Based on research of Instantly's verification system:
- Valid: Safe to send (default behavior)
- Accept_all: Catch-all domains (can be enabled)  
- Risky: Role-based, disposable (can be enabled)
- Invalid: Should not send (syntax/domain issues)
"""

import re
import socket
import dns.resolver
import logging
from typing import Tuple, Dict, List, Optional
from dataclasses import dataclass
from email_validator import validate_email, EmailNotValidError

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Result of email validation matching Instantly's categories."""
    email: str
    should_send: bool
    status: str  # 'valid', 'accept_all', 'risky', 'invalid'
    reason: str
    risk_factors: Dict[str, bool]

class EmailValidator:
    """Email validator that mimics Instantly's internal logic."""
    
    def __init__(self):
        # Load disposable domains list (common providers)
        self.disposable_domains = {
            '10minutemail.com', 'guerrillamail.com', 'mailinator.com',
            'tempmail.org', 'temp-mail.org', 'throwaway.email',
            'getnada.com', 'maildrop.cc', '33mail.com'
        }
        
        # Role-based prefixes (high bounce risk)
        self.role_prefixes = {
            'info', 'admin', 'support', 'help', 'sales', 'contact',
            'webmaster', 'postmaster', 'noreply', 'no-reply'
        }
    
    def validate(self, email_address: str, 
                allow_catch_all: bool = True,
                allow_risky: bool = False) -> ValidationResult:
        """
        Validate email using Instantly's logic.
        
        Args:
            email_address: Email to validate
            allow_catch_all: Accept catch-all domains (default: True)  
            allow_risky: Accept risky emails (default: False)
        """
        
        email_address = email_address.lower().strip()
        
        try:
            # Layer 1: Syntax validation (RFC 5322)
            try:
                validated = validate_email(email_address)
                normalized_email = validated.email
            except EmailNotValidError as e:
                return ValidationResult(
                    email=email_address,
                    should_send=False,
                    status='invalid',
                    reason=f"Invalid syntax: {str(e)}",
                    risk_factors={}
                )
            
            # Layer 2: Domain checks
            domain = normalized_email.split('@')[1]
            domain_status = self._check_domain(domain)
            
            if not domain_status['exists']:
                return ValidationResult(
                    email=email_address,
                    should_send=False, 
                    status='invalid',
                    reason="Domain does not exist",
                    risk_factors=domain_status
                )
            
            # Layer 3: Risk assessment
            risk_factors = self._assess_risk_factors(normalized_email, domain)
            
            # Layer 4: Decision logic (matches Instantly's behavior)
            return self._make_decision(
                normalized_email, domain_status, risk_factors,
                allow_catch_all, allow_risky
            )
            
        except Exception as e:
            logger.error(f"Validation error for {email_address}: {e}")
            return ValidationResult(
                email=email_address,
                should_send=False,
                status='invalid', 
                reason=f"Validation error: {str(e)}",
                risk_factors={}
            )
    
    def _check_domain(self, domain: str) -> Dict[str, bool]:
        """Check domain existence and mail server configuration."""
        result = {
            'exists': False,
            'has_mx': False,
            'has_a': False,
            'is_catch_all': False
        }
        
        try:
            # Check MX records (preferred)
            try:
                mx_records = dns.resolver.resolve(domain, 'MX')
                result['has_mx'] = len(mx_records) > 0
                result['exists'] = True
            except dns.resolver.NXDOMAIN:
                pass
            except dns.resolver.NoAnswer:
                pass
            
            # Fallback: Check A records
            if not result['has_mx']:
                try:
                    a_records = dns.resolver.resolve(domain, 'A')
                    result['has_a'] = len(a_records) > 0
                    result['exists'] = True
                except dns.resolver.NXDOMAIN:
                    pass
                except dns.resolver.NoAnswer:
                    pass
            
            # Check for catch-all (simplified detection)
            if result['exists']:
                result['is_catch_all'] = self._detect_catch_all(domain)
            
        except Exception as e:
            logger.debug(f"Domain check error for {domain}: {e}")
        
        return result
    
    def _detect_catch_all(self, domain: str) -> bool:
        """
        Simplified catch-all detection.
        In production, this would use SMTP testing with random addresses.
        """
        # Common catch-all patterns (conservative approach)
        catch_all_indicators = {
            'shopify.com', 'wordpress.com', 'wix.com', 'squarespace.com'
        }
        
        return domain in catch_all_indicators
    
    def _assess_risk_factors(self, email: str, domain: str) -> Dict[str, bool]:
        """Assess various risk factors for the email."""
        local_part = email.split('@')[0]
        
        return {
            'disposable_domain': domain in self.disposable_domains,
            'role_based': local_part in self.role_prefixes,
            'suspicious_pattern': self._has_suspicious_pattern(email),
            'free_provider': domain in {'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'}
        }
    
    def _has_suspicious_pattern(self, email: str) -> bool:
        """Check for suspicious email patterns."""
        local_part = email.split('@')[0]
        
        # Too many numbers (spammy pattern)
        if sum(c.isdigit() for c in local_part) > len(local_part) * 0.7:
            return True
        
        # Too many consecutive numbers
        if re.search(r'\d{6,}', local_part):
            return True
            
        # Random character patterns
        if re.search(r'[a-z]{1}[0-9]{4,}', local_part):
            return True
            
        return False
    
    def _make_decision(self, email: str, domain_status: Dict, 
                      risk_factors: Dict, allow_catch_all: bool, 
                      allow_risky: bool) -> ValidationResult:
        """Make final decision based on all factors."""
        
        # Invalid: Domain issues
        if not domain_status['exists'] or (not domain_status['has_mx'] and not domain_status['has_a']):
            return ValidationResult(
                email=email,
                should_send=False,
                status='invalid',
                reason="No mail servers found",
                risk_factors=risk_factors
            )
        
        # Invalid: Disposable domains 
        if risk_factors['disposable_domain']:
            return ValidationResult(
                email=email,
                should_send=False,
                status='invalid',  # Instantly treats these as invalid
                reason="Disposable email provider",
                risk_factors=risk_factors
            )
        
        # Risky: Role-based emails
        if risk_factors['role_based']:
            return ValidationResult(
                email=email,
                should_send=allow_risky,
                status='risky',
                reason="Role-based email address",
                risk_factors=risk_factors
            )
        
        # Risky: Suspicious patterns
        if risk_factors['suspicious_pattern']:
            return ValidationResult(
                email=email, 
                should_send=allow_risky,
                status='risky',
                reason="Suspicious email pattern",
                risk_factors=risk_factors
            )
        
        # Accept_all: Catch-all domains
        if domain_status['is_catch_all']:
            return ValidationResult(
                email=email,
                should_send=allow_catch_all,
                status='accept_all',
                reason="Catch-all domain",
                risk_factors=risk_factors
            )
        
        # Valid: Passed all checks
        return ValidationResult(
            email=email,
            should_send=True,
            status='valid',
            reason="Email appears valid",
            risk_factors=risk_factors
        )

# Global validator instance
validator = EmailValidator()

def validate_email_for_instantly(email: str, 
                                allow_catch_all: bool = True,
                                allow_risky: bool = False) -> ValidationResult:
    """
    Validate email for Instantly sending using internal logic.
    
    Args:
        email: Email address to validate
        allow_catch_all: Accept catch-all domains (default: True - matches Instantly)
        allow_risky: Accept risky emails (default: False - matches Instantly default)
    
    Returns:
        ValidationResult with should_send decision
    """
    return validator.validate(email, allow_catch_all, allow_risky)

def test_validation():
    """Test the validation system with sample emails."""
    test_emails = [
        "contact@shopify.com",        # Should be valid
        "info@modernnavora.com",      # Should be valid (from our failed list)
        "support@pinnersoq.com",      # Should be valid (from our failed list)
        "invalid@nonexistent.fake",   # Should be invalid
        "info@company.com",           # Role-based, should be risky
        "user@10minutemail.com",      # Disposable, should be invalid
        "test12345@gmail.com"         # Valid personal email
    ]
    
    print("🧪 Testing Email Validation System")
    print("=" * 50)
    
    for email in test_emails:
        result = validate_email_for_instantly(email)
        status_icon = "✅" if result.should_send else "❌"
        print(f"{status_icon} {email}")
        print(f"   Status: {result.status}")
        print(f"   Reason: {result.reason}")
        print(f"   Should Send: {result.should_send}")
        print()

if __name__ == "__main__":
    test_validation()