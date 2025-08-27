#!/usr/bin/env python3
"""
Manual Email Domain Verification
Check specific emails from the failed verification list using basic domain/MX record checks.
"""

import socket
import subprocess
import sys

def check_domain_mx_records(domain):
    """Check if domain has MX records using nslookup."""
    try:
        result = subprocess.run(['nslookup', '-type=MX', domain], 
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 and 'mail exchanger' in result.stdout.lower():
            return True, result.stdout.strip()
        else:
            return False, result.stdout.strip()
    except Exception as e:
        return False, f"Error: {e}"

def check_domain_exists(domain):
    """Check if domain resolves to an IP address."""
    try:
        socket.gethostbyname(domain)
        return True
    except socket.gaierror:
        return False

def analyze_email(email):
    """Analyze a single email address."""
    print(f"\n🔍 Analyzing: {email}")
    
    if '@' not in email:
        print("   ❌ Invalid email format")
        return
    
    domain = email.split('@')[1]
    print(f"   🌐 Domain: {domain}")
    
    # Check if domain exists
    domain_exists = check_domain_exists(domain)
    print(f"   📍 Domain exists: {'✅ YES' if domain_exists else '❌ NO'}")
    
    if domain_exists:
        # Check MX records
        has_mx, mx_info = check_domain_mx_records(domain)
        print(f"   📮 Has MX records: {'✅ YES' if has_mx else '❌ NO'}")
        
        if has_mx:
            print(f"   📧 Should be verifiable")
        else:
            print(f"   ⚠️  No MX records - explains verification failure")
    else:
        print(f"   💀 Domain doesn't exist - explains verification failure")

def main():
    """Test specific emails that failed verification."""
    print("🕵️ Manual Email Verification Check")
    print("=" * 50)
    
    # Emails from the failed verification list
    failed_emails = [
        "info@mabaia.com",
        "enquiry@fairleysofstirling.com", 
        "info@modernnavora.com",
        "support@pinnersoq.com",
        "contact@superzone.fr",
        "support@sinezy.fr",
        "support@massology.zendesk.com",
        "help@weckjars.ca",
        "info@vanderkooij.com.au",
        "info@claudia-amaral.com"
    ]
    
    print(f"📋 Testing {len(failed_emails)} emails that failed Instantly verification...")
    
    valid_domains = 0
    invalid_domains = 0
    
    for email in failed_emails:
        analyze_email(email)
        
        domain = email.split('@')[1]
        if check_domain_exists(domain):
            valid_domains += 1
        else:
            invalid_domains += 1
    
    print("\n" + "=" * 50)
    print("📊 SUMMARY:")
    print(f"   Valid domains: {valid_domains}/{len(failed_emails)} ({valid_domains/len(failed_emails)*100:.1f}%)")
    print(f"   Invalid domains: {invalid_domains}/{len(failed_emails)} ({invalid_domains/len(failed_emails)*100:.1f}%)")
    
    if valid_domains > invalid_domains:
        print("\n💡 CONCLUSION: Many domains are valid but failed Instantly verification")
        print("   This suggests Instantly's verification may be too strict or there's an API issue")
    else:
        print("\n💡 CONCLUSION: Most domains are genuinely invalid")
        print("   This suggests the lead data quality is poor")

if __name__ == "__main__":
    main()