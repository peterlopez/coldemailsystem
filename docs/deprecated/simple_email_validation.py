#!/usr/bin/env python3
"""
Simple Email Validation System
Mimics Instantly.ai's logic using only Python standard library.
No external dependencies required.
"""

import re
import socket
import logging
from typing import Tuple, Dict, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Result of email validation matching Instantly's categories."""
    email: str
    should_send: bool
    status: str  # 'valid', 'accept_all', 'risky', 'invalid'
    reason: str

class SimpleEmailValidator:
    """Email validator that mimics Instantly's logic using standard library."""
    
    def __init__(self):
        # Disposable domains (common providers)
        self.disposable_domains = {
            '10minutemail.com', 'guerrillamail.com', 'mailinator.com',
            'tempmail.org', 'temp-mail.org', 'throwaway.email',
            'getnada.com', 'maildrop.cc', '33mail.com', 'yopmail.com'
        }
        
        # Role-based prefixes
        self.role_prefixes = {
            'info', 'admin', 'support', 'help', 'sales', 'contact',
            'webmaster', 'postmaster', 'noreply', 'no-reply', 'marketing'
        }
        
        # Common catch-all patterns
        self.catch_all_domains = {
            'shopify.com', 'myshopify.com', 'wordpress.com', 'wix.com'
        }
    
    def validate(self, email_address: str, 
                allow_catch_all: bool = True,
                allow_risky: bool = False) -> ValidationResult:
        """Validate email using Instantly's logic."""
        
        email_address = email_address.lower().strip()
        
        # Layer 1: Basic syntax validation
        if not self._is_valid_syntax(email_address):
            return ValidationResult(
                email=email_address,
                should_send=False,
                status='invalid',
                reason="Invalid email syntax"
            )
        
        # Extract domain
        try:
            local_part, domain = email_address.split('@')
        except ValueError:
            return ValidationResult(
                email=email_address,
                should_send=False,
                status='invalid',
                reason="Invalid email format"
            )
        
        # Layer 2: Domain validation
        if not self._domain_exists(domain):
            return ValidationResult(
                email=email_address,
                should_send=False,
                status='invalid',
                reason="Domain does not exist"
            )
        
        # Layer 3: Check for disposable domains (invalid)
        if domain in self.disposable_domains:
            return ValidationResult(
                email=email_address,
                should_send=False,
                status='invalid',
                reason="Disposable email provider"
            )
        
        # Layer 4: Check for role-based (risky)
        if local_part in self.role_prefixes:
            return ValidationResult(
                email=email_address,
                should_send=allow_risky,
                status='risky',
                reason="Role-based email address"
            )
        
        # Layer 5: Check for suspicious patterns (risky)
        if self._has_suspicious_pattern(local_part):
            return ValidationResult(
                email=email_address,
                should_send=allow_risky,
                status='risky', 
                reason="Suspicious email pattern"
            )
        
        # Layer 6: Check for catch-all domains
        if domain in self.catch_all_domains or self._likely_catch_all(domain):
            return ValidationResult(
                email=email_address,
                should_send=allow_catch_all,
                status='accept_all',
                reason="Catch-all domain"
            )
        
        # Layer 7: Valid email
        return ValidationResult(
            email=email_address,
            should_send=True,
            status='valid',
            reason="Email appears valid"
        )
    
    def _is_valid_syntax(self, email: str) -> bool:
        """Basic email syntax validation using regex."""
        # RFC 5322 compliant regex (simplified)
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        if not re.match(pattern, email):
            return False
        
        # Additional checks
        if '..' in email:  # No consecutive dots
            return False
        
        if email.startswith('.') or email.endswith('.'):
            return False
        
        local_part, domain = email.split('@')
        
        # Local part checks
        if len(local_part) > 64:  # RFC limit
            return False
        
        # Domain checks  
        if len(domain) > 255:  # RFC limit
            return False
        
        return True
    
    def _domain_exists(self, domain: str) -> bool:
        """Check if domain exists using DNS lookup."""
        try:
            # Try to resolve domain
            socket.gethostbyname(domain)
            return True
        except socket.gaierror:
            try:
                # Fallback: try MX record check
                import subprocess
                result = subprocess.run(['nslookup', '-type=MX', domain], 
                                      capture_output=True, text=True, timeout=5)
                return 'mail exchanger' in result.stdout.lower()
            except:
                return False
    
    def _has_suspicious_pattern(self, local_part: str) -> bool:
        """Check for suspicious patterns in email local part."""
        
        # Too many consecutive numbers
        if re.search(r'\d{6,}', local_part):
            return True
        
        # High ratio of numbers to letters
        if len(local_part) > 3:
            num_digits = sum(c.isdigit() for c in local_part)
            if num_digits / len(local_part) > 0.7:
                return True
        
        # Common spam patterns
        spam_patterns = [
            r'^[a-z]{1,2}\d{4,}$',  # a1234, ab5678
            r'^\d+[a-z]{1,2}\d+$',  # 123a45
            r'^(test|temp|fake)\d*$'  # test123, temp, fake
        ]
        
        for pattern in spam_patterns:
            if re.match(pattern, local_part):
                return True
        
        return False
    
    def _likely_catch_all(self, domain: str) -> bool:
        """Simple heuristics for catch-all domains."""
        catch_all_indicators = [
            'shopify', 'myshopify', 'wordpress', 'wix', 'squarespace',
            'weebly', 'godaddy', 'bluehost'
        ]
        
        return any(indicator in domain for indicator in catch_all_indicators)

# Global validator instance
validator = SimpleEmailValidator()

def validate_email_for_instantly(email: str, 
                                allow_catch_all: bool = True,
                                allow_risky: bool = False) -> ValidationResult:
    """
    Validate email for Instantly sending using internal logic.
    
    Args:
        email: Email address to validate
        allow_catch_all: Accept catch-all domains (default: True)
        allow_risky: Accept risky emails (default: False)
    """
    return validator.validate(email, allow_catch_all, allow_risky)

def test_validation():
    """Test validation with emails that failed Instantly's API."""
    
    # These emails failed Instantly's API but have valid domains
    failed_emails = [
        "info@mabaia.com",
        "enquiry@fairleysofstirling.com", 
        "info@modernnavora.com",
        "support@pinnersoq.com",
        "contact@superzone.fr",
        "support@sinezy.fr",
        "help@weckjars.ca",
        "info@vanderkooij.com.au",
        "info@claudia-amaral.com"
    ]
    
    print("🧪 Testing Simple Email Validation")
    print("=" * 50)
    print("Testing emails that Instantly's API rejected as 'invalid':")
    print()
    
    valid_count = 0
    
    for email in failed_emails:
        result = validate_email_for_instantly(email, allow_catch_all=True, allow_risky=True)
        status_icon = "✅" if result.should_send else "❌"
        print(f"{status_icon} {email}")
        print(f"   Status: {result.status}")
        print(f"   Reason: {result.reason}")
        
        if result.should_send:
            valid_count += 1
        print()
    
    print("=" * 50)
    print(f"📊 Results: {valid_count}/{len(failed_emails)} emails would be accepted")
    print(f"📊 Success rate: {(valid_count/len(failed_emails)*100):.1f}%")
    print(f"📊 Instantly API success rate: 0% (all rejected)")
    print()
    
    if valid_count > len(failed_emails) * 0.5:
        print("✅ Our validation is much more permissive than Instantly's API!")
    else:
        print("⚠️ Our validation might still be too restrictive")

if __name__ == "__main__":
    test_validation()