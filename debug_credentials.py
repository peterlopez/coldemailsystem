#!/usr/bin/env python3
"""
Debug script to examine BigQuery credentials in detail
"""

import json
import os
import base64

print("=== CREDENTIALS DEBUG ===")

creds_path = './config/secrets/bigquery-credentials.json'

# Check if file exists
if not os.path.exists(creds_path):
    print(f"❌ Credentials file not found: {creds_path}")
    exit(1)

print(f"✅ Credentials file exists")

# Read and parse JSON
try:
    with open(creds_path, 'r') as f:
        creds = json.load(f)
    print(f"✅ JSON is valid")
except Exception as e:
    print(f"❌ JSON error: {e}")
    exit(1)

# Examine structure
print(f"Type: {creds.get('type', 'MISSING')}")
print(f"Project: {creds.get('project_id', 'MISSING')}")
print(f"Client email: {creds.get('client_email', 'MISSING')}")

# Examine private key in detail
private_key = creds.get('private_key', '')
print(f"Private key length: {len(private_key)} chars")

if not private_key:
    print("❌ Private key is missing!")
    exit(1)

print(f"Private key first 100 chars: {repr(private_key[:100])}")
print(f"Private key last 100 chars: {repr(private_key[-100:])}")

# Check for proper format
if not private_key.startswith('-----BEGIN'):
    print("❌ Private key doesn't start with -----BEGIN")
else:
    print("✅ Private key starts with -----BEGIN")

if not private_key.rstrip().endswith('-----'):
    print("❌ Private key doesn't end with -----")
else:
    print("✅ Private key ends with -----")

# Extract and test the base64 content
try:
    lines = private_key.strip().split('\n')
    if len(lines) < 3:
        print(f"❌ Private key has only {len(lines)} lines")
    else:
        print(f"✅ Private key has {len(lines)} lines")
        
        # Get the base64 content (everything between BEGIN and END)
        b64_lines = []
        collecting = False
        for line in lines:
            if '-----BEGIN' in line:
                collecting = True
                continue
            elif '-----END' in line:
                break
            elif collecting:
                b64_lines.append(line.strip())
        
        b64_content = ''.join(b64_lines)
        print(f"Base64 content length: {len(b64_content)} chars")
        
        # Check if it's valid base64
        try:
            decoded = base64.b64decode(b64_content)
            print(f"✅ Base64 decodes successfully to {len(decoded)} bytes")
        except Exception as e:
            print(f"❌ Base64 decode error: {e}")
            
            # Show problematic part
            remainder = len(b64_content) % 4
            if remainder != 0:
                print(f"❌ Base64 length {len(b64_content)} is not multiple of 4 (remainder: {remainder})")
                print(f"Last few characters: {repr(b64_content[-10:])}")
            
except Exception as e:
    print(f"❌ Error analyzing private key: {e}")

print("=== END DEBUG ===")