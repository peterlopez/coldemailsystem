#!/usr/bin/env python3
"""
Feature Flags for Safe Rollout - Production Grade

Provides feature flag management with environment overrides
for safe deployment of async verification system.
"""

import os
import hashlib
from typing import Any, Union

# ✅ Feature flags per team guidance
FEATURE_FLAGS = {
    # Core verification controls
    'verification.enabled': True,
    'verification.trigger_pct': 10,  # Start with 10% of leads
    'verification.autodelete_invalid': False,  # ✅ Start false per team guidance
    'verification.cooldown_hours': 24,
    
    # Rate limiting controls
    'verification.trigger_rps': 2.5,
    'verification.poll_rps': 0.75,
    'verification.max_triggers_per_run': 200,
    
    # Polling behavior
    'verification.max_attempts': 6,
    'verification.require_double_confirmation': True,
    'verification.exponential_base_minutes': 5,  # Starting interval: 5 minutes
    
    # Assignment verification
    'assignment.verification_retries': 5,
    'assignment.backoff_delays': [1, 2, 4, 8, 16],  # seconds
    
    # Environment-specific settings
    'verification.env': 'production',  # production/staging/dev
    
    # Monitoring and alerting
    'monitoring.emit_metrics': True,
    'monitoring.correlation_ids': True,
    
    # Security and privacy
    'security.mask_emails_in_logs': True,
    'security.rotate_keys_enabled': False,  # For future automation
}

def get_feature_flag(flag_name: str, default: Any = None) -> Any:
    """
    Get feature flag value with environment override support.
    
    Environment variables override config values using pattern:
    FEATURE_{FLAG_NAME_UPPER_WITH_UNDERSCORES}
    
    Example: verification.enabled → FEATURE_VERIFICATION_ENABLED
    """
    env_key = f"FEATURE_{flag_name.upper().replace('.', '_')}"
    env_value = os.getenv(env_key)
    
    if env_value is not None:
        # Type conversion based on default value type
        config_default = FEATURE_FLAGS.get(flag_name, default)
        
        if isinstance(config_default, bool):
            return env_value.lower() in ('true', '1', 'yes', 'on')
        elif isinstance(config_default, int):
            try:
                return int(env_value)
            except ValueError:
                return config_default
        elif isinstance(config_default, float):
            try:
                return float(env_value)
            except ValueError:
                return config_default
        elif isinstance(config_default, list):
            # Parse comma-separated values
            try:
                return [int(x.strip()) for x in env_value.split(',')]
            except ValueError:
                return config_default
        else:
            return env_value
    
    return FEATURE_FLAGS.get(flag_name, default)

def should_trigger_verification(email: str) -> bool:
    """
    Determine if verification should be triggered based on percentage rollout.
    
    Uses consistent hash-based sampling to ensure same email always gets
    same decision during rollout period.
    """
    if not get_feature_flag('verification.enabled', False):
        return False
    
    trigger_pct = get_feature_flag('verification.trigger_pct', 0)
    if trigger_pct <= 0:
        return False
    if trigger_pct >= 100:
        return True
    
    # Consistent hash-based sampling
    email_hash = int(hashlib.md5(email.encode()).hexdigest()[:8], 16)
    return (email_hash % 100) < trigger_pct

def get_verification_config() -> dict:
    """Get all verification-related configuration as dictionary."""
    return {
        key: get_feature_flag(key) 
        for key in FEATURE_FLAGS.keys() 
        if key.startswith('verification.')
    }

def is_verification_enabled() -> bool:
    """Quick check if verification system is enabled."""
    return get_feature_flag('verification.enabled', False)

# Environment-specific API key management
def get_env_specific_api_key() -> str:
    """✅ Environment-specific API keys per team guidance"""
    env = get_feature_flag('verification.env', 'production')
    
    key_map = {
        'production': 'INSTANTLY_API_KEY_PROD',
        'staging': 'INSTANTLY_API_KEY_STAGING', 
        'dev': 'INSTANTLY_API_KEY_DEV'
    }
    
    key_env_var = key_map.get(env, 'INSTANTLY_API_KEY')
    api_key = os.getenv(key_env_var)
    
    if not api_key:
        # Fallback to generic key for backward compatibility
        api_key = os.getenv('INSTANTLY_API_KEY')
        if not api_key:
            raise ValueError(f"API key not found for environment: {env}")
    
    return api_key

# Configuration validation
def validate_feature_flags() -> list:
    """Validate feature flag configuration and return any issues."""
    issues = []
    
    trigger_pct = get_feature_flag('verification.trigger_pct', 0)
    if not 0 <= trigger_pct <= 100:
        issues.append(f"verification.trigger_pct must be 0-100, got: {trigger_pct}")
    
    trigger_rps = get_feature_flag('verification.trigger_rps', 2.5)
    if trigger_rps <= 0:
        issues.append(f"verification.trigger_rps must be > 0, got: {trigger_rps}")
    
    poll_rps = get_feature_flag('verification.poll_rps', 0.75)
    if poll_rps <= 0:
        issues.append(f"verification.poll_rps must be > 0, got: {poll_rps}")
    
    max_triggers = get_feature_flag('verification.max_triggers_per_run', 200)
    if max_triggers <= 0:
        issues.append(f"verification.max_triggers_per_run must be > 0, got: {max_triggers}")
    
    return issues

if __name__ == "__main__":
    # Test feature flag system
    print("🔧 Feature Flags Configuration Test")
    print("=" * 50)
    
    # Validation
    issues = validate_feature_flags()
    if issues:
        print("❌ Configuration Issues:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("✅ Configuration validation passed")
    
    # Show current config
    print("\n📋 Current Verification Configuration:")
    config = get_verification_config()
    for key, value in config.items():
        print(f"  {key}: {value}")
    
    # Test rollout logic
    test_emails = ['test1@example.com', 'test2@example.com', 'test3@example.com']
    print(f"\n🧪 Rollout Test (trigger_pct: {get_feature_flag('verification.trigger_pct', 0)}%):")
    for email in test_emails:
        should_trigger = should_trigger_verification(email)
        print(f"  {email}: {'✅ TRIGGER' if should_trigger else '⏭️ SKIP'}")