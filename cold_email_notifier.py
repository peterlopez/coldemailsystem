#!/usr/bin/env python3
"""
Cold Email Notification Service
Sends notifications to Slack via Echo's existing message API

Based on Echo API research:
- Base URL: https://echo-api-ssh6.onrender.com
- Authentication: None required for basic endpoints
- Message format: {"title": "...", "content": "...", "recipients": ["#channel"]}
"""

import os
import sys
import requests
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)

@dataclass
class NotificationConfig:
    """Configuration for Echo API notifications"""
    echo_api_base_url: str
    slack_channel: str
    notifications_enabled: bool
    timeout_seconds: int = 60  # Increased for slower Echo API
    max_retries: int = 3

class EchoAPIClient:
    """Client for Echo's existing message API"""
    
    def __init__(self, config: NotificationConfig):
        self.config = config
        self.base_url = config.echo_api_base_url.rstrip('/')
        self.headers = {
            'Content-Type': 'application/json'
            # No authentication required for basic message endpoints
        }
    
    def create_message(self, title: str, content: str, recipients: List[str]) -> Optional[str]:
        """Create a new message in Echo and return message ID"""
        try:
            payload = {
                'title': title,
                'content': content,
                'recipients': recipients
            }
            
            logger.debug(f"Creating Echo message: {title}")
            response = requests.post(
                f'{self.base_url}/api/v1/messages',
                json=payload,
                headers=self.headers,
                timeout=self.config.timeout_seconds
            )
            
            response.raise_for_status()
            message_data = response.json()
            message_id = message_data.get('id')
            
            logger.info(f"✅ Created Echo message: {message_id}")
            return str(message_id)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to create Echo message: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            return None
    
    def send_message(self, message_id: str) -> bool:
        """Send an existing message immediately"""
        try:
            logger.debug(f"Sending Echo message: {message_id}")
            response = requests.post(
                f'{self.base_url}/api/v1/messages/{message_id}/send',
                headers=self.headers,
                timeout=self.config.timeout_seconds
            )
            
            response.raise_for_status()
            logger.info(f"✅ Sent Echo message: {message_id}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to send Echo message {message_id}: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response: {e.response.text}")
            return False
    
    def create_and_send_message(self, title: str, content: str, recipients: List[str]) -> bool:
        """Create and immediately send a message with retry logic"""
        for attempt in range(1, self.config.max_retries + 1):
            try:
                message_id = self.create_message(title, content, recipients)
                if message_id:
                    success = self.send_message(message_id)
                    if success:
                        return True
                    # If send failed but create succeeded, don't retry create
                    logger.warning(f"Message created but send failed on attempt {attempt}")
                    return False
                else:
                    # Create failed, will retry
                    if attempt < self.config.max_retries:
                        retry_delay = 2 ** attempt  # Exponential backoff: 2, 4, 8 seconds
                        logger.warning(f"Create failed on attempt {attempt}, retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                    continue
            except Exception as e:
                if attempt < self.config.max_retries:
                    retry_delay = 2 ** attempt
                    logger.warning(f"Echo API error on attempt {attempt}: {e}, retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Echo API failed after {self.config.max_retries} attempts: {e}")
        
        return False

class ColdEmailNotifier:
    """Main notification service for Cold Email System operations"""
    
    def __init__(self):
        # Load configuration from environment
        self.config = NotificationConfig(
            echo_api_base_url=os.getenv('ECHO_API_BASE_URL', 'https://echo-api-ssh6.onrender.com'),
            slack_channel=os.getenv('SLACK_NOTIFICATION_CHANNEL', '#sales-cold-email-ops'),
            notifications_enabled=os.getenv('SLACK_NOTIFICATIONS_ENABLED', 'true').lower() == 'true'
        )
        
        # Validate configuration
        self._validate_config()
        
        # Initialize Echo API client
        if self.config.notifications_enabled:
            self.echo_client = EchoAPIClient(self.config)
            logger.info(f"📡 Notifications enabled → {self.config.slack_channel}")
        else:
            self.echo_client = None
            logger.info("📴 Notifications disabled")
    
    def _validate_config(self):
        """Validate notification configuration"""
        if not self.config.notifications_enabled:
            return
            
        if not self.config.echo_api_base_url:
            logger.warning("⚠️ ECHO_API_BASE_URL not configured, using default")
            
        logger.info(f"🔧 Echo API: {self.config.echo_api_base_url}")
        logger.info(f"📢 Target channel: {self.config.slack_channel}")
    
    def send_sync_notification(self, sync_data: Dict[str, Any]) -> bool:
        """Send Cold Email Sync completion notification"""
        if not self.config.notifications_enabled:
            logger.info("📴 Sync notifications disabled, skipping")
            return True
        
        try:
            # Format notification content
            title = f"Cold Email Sync Complete - {self._format_timestamp(sync_data.get('timestamp', ''))}"
            content = self._format_sync_content(sync_data)
            recipients = [self.config.slack_channel]
            
            # Send via Echo
            success = self.echo_client.create_and_send_message(title, content, recipients)
            
            if success:
                logger.info("✅ Sync notification sent successfully")
            else:
                logger.error("❌ Failed to send sync notification")
                
            return success
            
        except Exception as e:
            logger.error(f"❌ Error sending sync notification: {e}")
            return False
    
    def send_drain_notification(self, drain_data: Dict[str, Any]) -> bool:
        """Send Cold Email Drain completion notification"""
        if not self.config.notifications_enabled:
            logger.info("📴 Drain notifications disabled, skipping")
            return True
        
        try:
            # Format notification content
            title = f"Cold Email Drain Complete - {self._format_timestamp(drain_data.get('timestamp', ''))}"
            content = self._format_drain_content(drain_data)
            recipients = [self.config.slack_channel]
            
            # Send via Echo
            success = self.echo_client.create_and_send_message(title, content, recipients)
            
            if success:
                logger.info("✅ Drain notification sent successfully")
            else:
                logger.error("❌ Failed to send drain notification")
                
            return success
            
        except Exception as e:
            logger.error(f"❌ Error sending drain notification: {e}")
            return False
    
    def send_verification_polling_notification(self, polling_data: Dict[str, Any]) -> bool:
        """Send Async Verification Polling completion notification"""
        if not self.config.notifications_enabled:
            logger.info("📴 Verification polling notifications disabled, skipping")
            return True
        
        try:
            # Format notification content
            title = f"Async Verification Polling Complete - {self._format_timestamp(polling_data.get('timestamp', ''))}"
            content = self._format_verification_polling_content(polling_data)
            recipients = [self.config.slack_channel]
            
            # Send via Echo
            success = self.echo_client.create_and_send_message(title, content, recipients)
            
            if success:
                logger.info("✅ Verification polling notification sent successfully")
            else:
                logger.error("❌ Failed to send verification polling notification")
                
            return success
            
        except Exception as e:
            logger.error(f"❌ Error sending verification polling notification: {e}")
            return False
    
    def _format_sync_content(self, data: Dict[str, Any]) -> str:
        """Format sync notification content for Slack"""
        try:
            # Extract data with safe defaults
            capacity = data.get('capacity_status', {})
            leads = data.get('leads_processed', {})
            inventory = data.get('final_inventory', {})
            performance = data.get('performance', {})
            errors = data.get('errors', [])
            
            # Calculate key metrics
            current_inventory = capacity.get('current_inventory', 0)
            max_capacity = capacity.get('max_capacity', 24000)
            utilization = capacity.get('utilization_percentage', 0)
            
            smb_added = leads.get('smb_campaign', {}).get('leads_added', 0)
            midsize_added = leads.get('midsize_campaign', {}).get('leads_added', 0)
            total_added = smb_added + midsize_added
            
            instantly_total = inventory.get('instantly_total', current_inventory)
            bigquery_eligible = inventory.get('bigquery_eligible', 0)
            
            duration = data.get('duration_seconds', 0)
            processing_rate = performance.get('processing_rate_per_minute', 0)
            api_success = performance.get('api_success_rate', 0)
            
            # Determine status emojis
            capacity_emoji = "🟢" if utilization < 70 else "🟡" if utilization < 90 else "🔴"
            
            # Build formatted message - STREAMLINED to remove redundancy
            content = f"""🔄 **Cold Email Sync Complete** | {self._format_timestamp(data.get('timestamp', ''))}

📊 **System Status**
{capacity_emoji} Current: {current_inventory:,} / {max_capacity:,} leads ({utilization:.1f}% utilized)
• Available capacity: ~{max_capacity - current_inventory:,} leads remaining
• Added this run: +{total_added} leads
• BigQuery eligible: {bigquery_eligible:,} ready

📈 **Campaign Breakdown**  
• SMB Campaign: {smb_added} leads added
• Midsize Campaign: {midsize_added} leads added
• Total processed: {total_added} leads"""

            # Add async verification status if available
            verification_data = data.get('async_verification', {})
            if verification_data:
                verification_triggered = verification_data.get('triggered', False)
                verification_count = verification_data.get('lead_count', 0)
                if verification_triggered and verification_count > 0:
                    content += f"\n• 🔍 Async verification triggered for {verification_count} leads"
                elif verification_count > 0:
                    content += f"\n• ⚠️ Verification trigger failed for {verification_count} leads"
                else:
                    content += f"\n• 📴 No verification triggers (dry run or no new leads)"
            else:
                content += f"\n• 🔍 Verification handled by Instantly internally"

            content += f"""

⚡ **Performance**
• Duration: {self._format_duration(duration)}
• Rate: {processing_rate:.1f} leads/min
• API Success: {api_success:.1f}%"""

            # Add errors if any
            if errors:
                content += f"\n\n⚠️ **Issues**: {len(errors)} errors logged"
                for error in errors[:3]:  # Show first 3 errors
                    content += f"\n• {str(error)[:100]}..."
            
            # Add GitHub link if available
            github_url = data.get('github_run_url')
            if github_url:
                content += f"\n\n🔗 [View Logs]({github_url})"
            
            return content
            
        except Exception as e:
            logger.error(f"Error formatting sync content: {e}")
            return f"🔄 Cold Email Sync Complete - Error formatting details: {str(e)}"
    
    def _format_drain_content(self, data: Dict[str, Any]) -> str:
        """Format drain notification content for Slack"""
        try:
            # Extract data with safe defaults
            analysis = data.get('analysis_summary', {})
            classifications = data.get('drain_classifications', {})
            deletions = data.get('deletion_results', {})
            dnc = data.get('dnc_updates', {})
            inventory = data.get('inventory_impact', {})
            performance = data.get('performance', {})
            errors = data.get('errors', [])
            
            # Calculate key metrics
            total_analyzed = analysis.get('total_leads_analyzed', 0)
            skipped_24hr = analysis.get('leads_skipped_24hr', 0)
            eligible_drain = analysis.get('leads_eligible_for_drain', 0)
            
            completed = classifications.get('completed', 0)
            replied = classifications.get('replied', 0)
            bounced = classifications.get('bounced_hard', 0)
            unsubscribed = classifications.get('unsubscribed', 0)
            stale = classifications.get('stale_active', 0)
            total_identified = classifications.get('total_identified', 0)
            
            attempted = deletions.get('attempted_deletions', 0)
            successful = deletions.get('successful_deletions', 0)
            failed = deletions.get('failed_deletions', 0)
            success_rate = deletions.get('success_rate_percentage', 0)
            
            new_dnc = dnc.get('new_unsubscribes', 0)
            total_dnc = dnc.get('total_dnc_list', 0)
            
            removed = inventory.get('leads_removed', successful)
            new_inventory = inventory.get('new_inventory_total', 0)
            
            duration = data.get('duration_seconds', 0)
            processing_rate = performance.get('processing_rate_per_minute', 0)
            classification_accuracy = performance.get('classification_accuracy', 0)
            
            # Build formatted message
            content = f"""🧹 **Cold Email Drain Complete** | {self._format_timestamp(data.get('timestamp', ''))}

🔍 **Analysis Summary**
• Total analyzed: {total_analyzed:,} leads
• Skipped (24hr filter): {skipped_24hr:,} leads 
• Eligible for drain: {eligible_drain:,} leads
• Identified for removal: {total_identified:,} leads

📋 **Drain Breakdown**
• ✅ Completed sequences: {completed}
• 💬 Replied: {replied}  
• ⚠️ Hard bounced: {bounced}
• 🚫 Unsubscribed: {unsubscribed}
• 🕐 Stale (90+ days): {stale}

✅ **Deletion Results**
• Success: {successful}/{attempted} ({success_rate:.1f}%) 
• Failed: {failed} → Logged for retry
• New inventory: {new_inventory:,} active leads

🔒 **DNC Protection**
• Added {new_dnc} new unsubscribes
• Total protected: {total_dnc:,} contacts

⚡ **Performance**
• Duration: {self._format_duration(duration)}
• Rate: {processing_rate:.1f} leads/min
• Classification accuracy: {classification_accuracy:.1f}%"""

            # Add errors if any
            if errors:
                content += f"\n\n⚠️ **Issues**: {len(errors)} errors logged"
                for error in errors[:2]:  # Show first 2 errors
                    content += f"\n• {str(error)[:100]}..."
            
            # Add GitHub link if available
            github_url = data.get('github_run_url')
            if github_url:
                content += f"\n\n🔗 [View Logs]({github_url})"
            
            return content
            
        except Exception as e:
            logger.error(f"Error formatting drain content: {e}")
            return f"🧹 Cold Email Drain Complete - Error formatting details: {str(e)}"
    
    def _format_timestamp(self, timestamp_str: str) -> str:
        """Format timestamp for display"""
        try:
            if timestamp_str:
                # Parse ISO timestamp and convert to EST display
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                return dt.strftime('%I:%M %p EST')
            return datetime.now().strftime('%I:%M %p EST')
        except:
            return datetime.now().strftime('%I:%M %p EST')
    
    def _format_duration(self, seconds: float) -> str:
        """Format duration in human readable format"""
        try:
            if seconds < 60:
                return f"{seconds:.0f}s"
            elif seconds < 3600:
                mins = int(seconds // 60)
                secs = int(seconds % 60)
                return f"{mins}m {secs}s"
            else:
                hours = int(seconds // 3600)
                mins = int((seconds % 3600) // 60)
                return f"{hours}h {mins}m"
        except:
            return "N/A"
    
    def _format_verification_polling_content(self, data: Dict[str, Any]) -> str:
        """Format verification polling notification content for Slack"""
        try:
            # Extract data with safe defaults
            verifications = data.get('verifications_checked', 0)
            status_breakdown = data.get('status_breakdown', {})
            queued_for_deletion = data.get('queued_for_deletion', 0)
            deletions = data.get('deletes_processed', 0)
            deletion_breakdown = data.get('deletion_breakdown', {})
            errors = data.get('errors', 0)
            duration = data.get('duration_seconds', 0)
            
            # Calculate percentages for status breakdown
            total_checked = verifications
            status_lines = []
            
            # Order statuses for display - prioritize valid, invalid, pending
            priority_statuses = ['valid', 'invalid', 'pending']
            other_statuses = ['no_result', 'risky', 'accept_all']
            status_emojis = {
                'valid': '✅',
                'invalid': '❌',
                'pending': '⏳',
                'no_result': '🤷',
                'risky': '⚠️',
                'accept_all': '📮'
            }
            
            # Always show valid, invalid, and pending (even if 0)
            for status in priority_statuses:
                count = status_breakdown.get(status, 0)
                percentage = (count / max(total_checked, 1)) * 100 if total_checked > 0 else 0
                emoji = status_emojis.get(status, '•')
                
                # Special formatting based on status
                if status == 'valid':
                    status_lines.append(f"{emoji} Valid: {count} ({percentage:.1f}%) → Kept active")
                elif status == 'invalid':
                    status_lines.append(f"{emoji} Invalid: {count} ({percentage:.1f}%) → Deleted")
                elif status == 'pending':
                    status_lines.append(f"{emoji} Pending: {count} ({percentage:.1f}%) → Awaiting results")
            
            # Show other statuses only if they have counts > 0
            for status in other_statuses:
                if status in status_breakdown and status_breakdown[status] > 0:
                    count = status_breakdown[status]
                    percentage = (count / max(total_checked, 1)) * 100
                    emoji = status_emojis.get(status, '•')
                    status_lines.append(f"{emoji} {status.replace('_', ' ').title()}: {count} ({percentage:.1f}%)")
            
            # Build formatted message
            content = f"""🔍 **Async Verification Polling Complete** | {self._format_timestamp(data.get('timestamp', ''))}

📧 **Verification Results** ({verifications} checked)"""
            
            if status_lines:
                for line in status_lines:
                    content += f"\n• {line}"
            else:
                content += "\n• No verifications processed"
            
            # Add deletion section if any deletions occurred
            if deletions > 0 or queued_for_deletion > 0:
                content += f"""

🗑️ **Deletion Activity**"""
                
                if queued_for_deletion > 0:
                    content += f"\n• Queued for deletion this run: {queued_for_deletion} emails"
                
                if deletions > 0:
                    content += f"\n• Total processed from queue: {deletions} emails"
                    content += f"\n• Added to DNC: {deletions} emails"
                    
                    # Show relationship between queued and deleted if both exist
                    if queued_for_deletion > 0 and deletions != queued_for_deletion:
                        old_queue = deletions - queued_for_deletion
                        if old_queue > 0:
                            content += f"\n• Processed {queued_for_deletion} new + {old_queue} from previous runs"
                
                # Add campaign breakdown if available
                if deletion_breakdown:
                    content += "\n• Campaign impact:"
                    for campaign_id, details in deletion_breakdown.items():
                        campaign_name = details.get('name', 'Unknown')
                        count = details.get('count', 0)
                        content += f"\n  - {campaign_name}: {count} removed"
            
            # Performance section
            if duration > 0:
                processing_rate = (verifications / duration * 60) if verifications > 0 else 0
                content += f"""

⚡ **Performance**
• Duration: {self._format_duration(duration)}
• Processing rate: {processing_rate:.1f} emails/min"""
            
            # Add error section if any
            if errors > 0:
                content += f"\n\n⚠️ **Issues**: {errors} errors occurred"
            
            # Add GitHub link if available
            github_url = data.get('github_run_url')
            if github_url:
                content += f"\n\n🔗 [View Logs]({github_url})"
            
            return content
            
        except Exception as e:
            logger.error(f"Error formatting verification polling content: {e}")
            return f"🔍 Async Verification Polling Complete - Error formatting details: {str(e)}"

# Initialize global notifier instance
notifier = ColdEmailNotifier()

# Test function for development
def test_notification():
    """Test function to verify notification system"""
    test_sync_data = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "duration_seconds": 125.7,
        "capacity_status": {
            "current_inventory": 1250,
            "max_capacity": 24000,
            "utilization_percentage": 5.2
        },
        "leads_processed": {
            "smb_campaign": {"leads_added": 65},
            "midsize_campaign": {"leads_added": 27}
        },
        "verification_results": {
            "verified_successful": 92,
            "total_attempted": 100,
            "verification_failed": 8,
            "success_rate_percentage": 92.0,
            "credits_used": 25.0
        },
        "final_inventory": {
            "instantly_total": 1342,
            "bigquery_eligible": 291469
        },
        "performance": {
            "processing_rate_per_minute": 47.1,
            "api_success_rate": 98.7
        },
        "errors": [],
        "github_run_url": "https://github.com/peterlopez/cold-email-system/actions/runs/123456"
    }
    
    print("🧪 Testing notification system...")
    print(f"📡 Echo API: {notifier.config.echo_api_base_url}")
    print(f"📢 Channel: {notifier.config.slack_channel}")
    print(f"🔧 Enabled: {notifier.config.notifications_enabled}")
    
    return notifier.send_sync_notification(test_sync_data)

if __name__ == "__main__":
    # Allow testing from command line
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        success = test_notification()
        print(f"\n{'✅ Test succeeded' if success else '❌ Test failed'}")
        sys.exit(0 if success else 1)
    else:
        print("Usage: python cold_email_notifier.py test")