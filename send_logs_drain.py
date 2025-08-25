#!/usr/bin/env python3
"""
Send drain workflow logs to external service for analysis
Enhanced version that handles drain-specific logs
"""

import requests
import json
import os
from datetime import datetime

def send_logs_to_webhook():
    """Send logs to a webhook service for remote analysis"""
    
    # Collect log files
    log_files = {}
    
    # Read drain-workflow.log if exists
    try:
        with open('drain-workflow.log', 'r') as f:
            log_files['drain-workflow'] = f.read()
        print("✅ Captured drain-workflow.log")
    except FileNotFoundError:
        print("⚠️ drain-workflow.log not found")
    
    # Read workflow.log if exists (fallback)
    try:
        with open('workflow.log', 'r') as f:
            log_files['workflow'] = f.read()
        print("✅ Captured workflow.log")
    except FileNotFoundError:
        print("⚠️ workflow.log not found")
    
    # Read cold-email-sync.log if exists (created by drain_once.py)
    try:
        with open('cold-email-sync.log', 'r') as f:
            log_files['sync'] = f.read()
        print("✅ Captured cold-email-sync.log")
    except FileNotFoundError:
        print("⚠️ cold-email-sync.log not found")
        
    # Read test.log if exists
    try:
        with open('test.log', 'r') as f:
            log_files['test'] = f.read()
        print("✅ Captured test.log")
    except FileNotFoundError:
        print("⚠️ test.log not found")
    
    # Prepare content for paste
    timestamp = datetime.utcnow().isoformat()
    run_id = os.environ.get('GITHUB_RUN_ID', 'unknown')
    run_number = os.environ.get('GITHUB_RUN_NUMBER', 'unknown')
    dry_run = os.environ.get('DRY_RUN', 'unknown')
    workflow_name = os.environ.get('GITHUB_WORKFLOW', 'unknown')
    
    # Create combined log content
    content_parts = [
        f"=== DRAIN WORKFLOW LOGS ===",
        f"Workflow: {workflow_name}",
        f"Timestamp: {timestamp}",
        f"GitHub Run ID: {run_id}",
        f"GitHub Run Number: {run_number}",
        f"Dry Run: {dry_run}",
        "",
    ]
    
    for log_name, log_content in log_files.items():
        if log_content:
            content_parts.extend([
                f"=== {log_name.upper()} LOG ===",
                log_content,
                "",
            ])
    
    combined_content = "\n".join(content_parts)
    
    # Use a simple paste service - dpaste.org (reliable and free)
    paste_url = "https://dpaste.org/api/"
    
    try:
        # dpaste.org API format
        data = {
            'content': combined_content,
            'format': 'text',
            'expires': 86400,  # 1 day
        }
        
        response = requests.post(
            paste_url,
            data=data,
            timeout=15,
            headers={'User-Agent': 'Cold-Email-Drain-Logger/1.0'}
        )
        
        if response.status_code == 201:
            paste_public_url = response.text.strip()
            print(f"✅ Drain logs uploaded successfully!")
            print(f"🔗 View logs at: {paste_public_url}")
            print(f"📊 Content size: {len(combined_content)} characters")
            return True
        else:
            print(f"❌ Paste service failed: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Failed to upload drain logs: {e}")
        return False

if __name__ == "__main__":
    send_logs_to_webhook()