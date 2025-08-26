#!/usr/bin/env python3
"""
Notification Handler for Cold Email System
Sends email notifications for sync and drain operations using SendGrid.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Optional, List
from dataclasses import dataclass

try:
    import sendgrid
    from sendgrid.helpers.mail import Mail, Email, To, Content
    SENDGRID_AVAILABLE = True
except ImportError:
    SENDGRID_AVAILABLE = False
    logging.warning("SendGrid not available - install with: pip install sendgrid")

@dataclass
class SyncMetrics:
    """Metrics collected from sync operation"""
    timestamp: str
    duration_seconds: float
    current_capacity: int
    max_capacity: int
    smb_leads_added: int
    midsize_leads_added: int
    total_leads_added: int
    verified_count: int
    failed_verification_count: int
    verification_failure_disposition: str
    final_inventory_count: int
    errors: List[str]
    logs_url: Optional[str] = None

@dataclass
class DrainMetrics:
    """Metrics collected from drain operation"""
    timestamp: str
    duration_seconds: float
    unique_leads_analyzed: int
    leads_skipped_24hr: int
    completed_leads: int
    replied_leads: int
    bounced_leads: int
    unsubscribed_leads: int
    stale_leads: int
    total_identified_for_drain: int
    successfully_deleted: int
    failed_deletions: int
    success_rate_percent: float
    errors: List[str]
    logs_url: Optional[str] = None

class NotificationHandler:
    """Handles email notifications for Cold Email System operations"""
    
    def __init__(self):
        self.enabled = os.getenv('ENABLE_NOTIFICATIONS', 'true').lower() == 'true'
        self.sendgrid_api_key = os.getenv('SENDGRID_API_KEY')
        self.sender_email = os.getenv('SENDER_EMAIL', 'noreply@coldemailsystem.com')
        self.recipient_email = os.getenv('NOTIFICATION_EMAIL')
        
        # Initialize logging
        self.logger = logging.getLogger('notification_handler')
        
        if not self.enabled:
            self.logger.info("📧 Notifications disabled via ENABLE_NOTIFICATIONS")
            return
            
        if not SENDGRID_AVAILABLE:
            self.logger.error("❌ SendGrid library not available")
            self.enabled = False
            return
            
        if not self.sendgrid_api_key:
            self.logger.error("❌ SENDGRID_API_KEY not configured")
            self.enabled = False
            return
            
        if not self.recipient_email:
            self.logger.error("❌ NOTIFICATION_EMAIL not configured")
            self.enabled = False
            return
            
        self.sg = sendgrid.SendGridAPIClient(api_key=self.sendgrid_api_key)
        self.logger.info(f"📧 Notification handler initialized - will send to {self.recipient_email}")

    def send_sync_notification(self, metrics: SyncMetrics) -> bool:
        """Send notification for sync operation completion"""
        if not self.enabled:
            return True
            
        try:
            subject = f"✅ Cold Email Sync Complete - {metrics.timestamp} - {metrics.total_leads_added} leads added"
            
            # Calculate utilization
            utilization = (metrics.final_inventory_count / metrics.max_capacity * 100) if metrics.max_capacity > 0 else 0
            available_slots = metrics.max_capacity - metrics.current_capacity
            
            # Create email content
            content = self._create_sync_email_content(metrics, utilization, available_slots)
            
            return self._send_email(subject, content)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to send sync notification: {e}")
            return False

    def send_drain_notification(self, metrics: DrainMetrics) -> bool:
        """Send notification for drain operation completion"""
        if not self.enabled:
            return True
            
        try:
            subject = f"🧹 Cold Email Drain Complete - {metrics.timestamp} - {metrics.successfully_deleted} leads removed"
            
            # Create email content
            content = self._create_drain_email_content(metrics)
            
            return self._send_email(subject, content)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to send drain notification: {e}")
            return False

    def send_error_notification(self, operation: str, error_message: str, details: str = "") -> bool:
        """Send error notification"""
        if not self.enabled:
            return True
            
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
            subject = f"❌ Cold Email System Error - {operation} - {timestamp}"
            
            content = f"""
            <div style="font-family: monospace, 'Courier New'; max-width: 800px;">
            <h2 style="color: #dc3545;">❌ SYSTEM ERROR</h2>
            <p><strong>Timestamp:</strong> {timestamp}</p>
            <p><strong>Operation:</strong> {operation}</p>
            <p><strong>Error:</strong> {error_message}</p>
            
            {f"<p><strong>Details:</strong></p><pre>{details}</pre>" if details else ""}
            
            <hr style="margin: 20px 0;">
            <p style="color: #666; font-size: 14px;">
            🔧 Check the GitHub Actions logs for more details.<br>
            📧 This is an automated notification from Cold Email System.
            </p>
            </div>
            """
            
            return self._send_email(subject, content)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to send error notification: {e}")
            return False

    def _create_sync_email_content(self, metrics: SyncMetrics, utilization: float, available_slots: int) -> str:
        """Create HTML email content for sync notification"""
        
        # Format duration
        duration_str = f"{metrics.duration_seconds:.1f}s"
        if metrics.duration_seconds > 60:
            minutes = int(metrics.duration_seconds // 60)
            seconds = int(metrics.duration_seconds % 60)
            duration_str = f"{minutes}m {seconds}s"
        
        # Verification success rate
        total_processed = metrics.verified_count + metrics.failed_verification_count
        verification_rate = (metrics.verified_count / total_processed * 100) if total_processed > 0 else 0
        
        # Error section
        error_section = ""
        if metrics.errors:
            error_section = f"""
            <div style="background: #f8d7da; border: 1px solid #f5c6cb; padding: 15px; margin: 15px 0; border-radius: 4px;">
            <h4 style="color: #721c24; margin-top: 0;">⚠️ Errors Encountered:</h4>
            <ul style="color: #721c24; margin-bottom: 0;">
            {"".join(f"<li>{error}</li>" for error in metrics.errors)}
            </ul>
            </div>
            """
        
        return f"""
        <div style="font-family: monospace, 'Courier New'; max-width: 800px;">
        <h2 style="color: #28a745;">📊 SYNC SUMMARY ({metrics.timestamp})</h2>
        <hr style="border: 2px solid #28a745;">
        
        <h3>💾 CAPACITY STATUS</h3>
        <ul>
        <li><strong>Current capacity:</strong> {metrics.current_capacity:,}/{metrics.max_capacity:,} leads ({utilization:.1f}% full)</li>
        <li><strong>Available slots:</strong> {available_slots:,} leads</li>
        </ul>
        
        <h3>📥 LEADS PROCESSED</h3>
        <ul>
        <li><strong>SMB Campaign:</strong> {metrics.smb_leads_added:,} leads added</li>
        <li><strong>Midsize Campaign:</strong> {metrics.midsize_leads_added:,} leads added</li>
        <li><strong>Total added:</strong> {metrics.total_leads_added:,} leads</li>
        </ul>
        
        <h3>✅ EMAIL VERIFICATION</h3>
        <ul>
        <li><strong>Verified & added:</strong> {metrics.verified_count:,} leads ({verification_rate:.1f}%)</li>
        <li><strong>Failed verification:</strong> {metrics.failed_verification_count:,} leads</li>
        <li><strong>Disposition:</strong> {metrics.verification_failure_disposition}</li>
        </ul>
        
        <h3>📊 FINAL INVENTORY</h3>
        <ul>
        <li><strong>Total leads in Instantly:</strong> {metrics.final_inventory_count:,}</li>
        <li><strong>Inventory utilization:</strong> {utilization:.1f}%</li>
        </ul>
        
        {error_section}
        
        <hr style="margin: 20px 0;">
        <p><strong>⏱️ Execution time:</strong> {duration_str}</p>
        {f'<p><strong>🔗 View full logs:</strong> <a href="{metrics.logs_url}">GitHub Actions</a></p>' if metrics.logs_url else ''}
        
        <p style="color: #666; font-size: 14px;">
        📧 This is an automated notification from Cold Email System.<br>
        🔄 Next sync scheduled according to GitHub Actions workflow.
        </p>
        </div>
        """

    def _create_drain_email_content(self, metrics: DrainMetrics) -> str:
        """Create HTML email content for drain notification"""
        
        # Format duration
        duration_str = f"{metrics.duration_seconds:.1f}s"
        if metrics.duration_seconds > 60:
            minutes = int(metrics.duration_seconds // 60)
            seconds = int(metrics.duration_seconds % 60)
            duration_str = f"{minutes}m {seconds}s"
        
        # Calculate evaluation rate
        total_leads = metrics.unique_leads_analyzed + metrics.leads_skipped_24hr
        eval_rate = (metrics.unique_leads_analyzed / total_leads * 100) if total_leads > 0 else 0
        
        # Error section
        error_section = ""
        if metrics.errors:
            error_section = f"""
            <div style="background: #f8d7da; border: 1px solid #f5c6cb; padding: 15px; margin: 15px 0; border-radius: 4px;">
            <h4 style="color: #721c24; margin-top: 0;">⚠️ Errors Encountered:</h4>
            <ul style="color: #721c24; margin-bottom: 0;">
            {"".join(f"<li>{error}</li>" for error in metrics.errors)}
            </ul>
            </div>
            """
        
        return f"""
        <div style="font-family: monospace, 'Courier New'; max-width: 800px;">
        <h2 style="color: #dc3545;">📊 DRAIN SUMMARY ({metrics.timestamp})</h2>
        <hr style="border: 2px solid #dc3545;">
        
        <h3>🔍 ANALYSIS OVERVIEW</h3>
        <ul>
        <li><strong>Total unique leads analyzed:</strong> {metrics.unique_leads_analyzed:,}</li>
        <li><strong>Skipped (24hr filter):</strong> {metrics.leads_skipped_24hr:,} leads</li>
        <li><strong>Evaluation rate:</strong> {eval_rate:.1f}%</li>
        </ul>
        
        <h3>🗑️ DRAIN IDENTIFICATION</h3>
        <ul>
        <li><strong>Completed sequences:</strong> {metrics.completed_leads:,} leads</li>
        <li><strong>Genuine replies:</strong> {metrics.replied_leads:,} leads</li>
        <li><strong>Hard bounces:</strong> {metrics.bounced_leads:,} leads</li>
        <li><strong>Unsubscribes:</strong> {metrics.unsubscribed_leads:,} leads</li>
        <li><strong>Stale active:</strong> {metrics.stale_leads:,} leads</li>
        <li><strong>Total identified:</strong> {metrics.total_identified_for_drain:,} leads</li>
        </ul>
        
        <h3>✅ DELETION RESULTS</h3>
        <ul>
        <li><strong>Successfully deleted:</strong> {metrics.successfully_deleted:,} leads</li>
        <li><strong>Failed deletions:</strong> {metrics.failed_deletions:,} leads</li>
        <li><strong>Success rate:</strong> {metrics.success_rate_percent:.1f}%</li>
        </ul>
        
        {error_section}
        
        <hr style="margin: 20px 0;">
        <p><strong>⏱️ Execution time:</strong> {duration_str}</p>
        {f'<p><strong>🔗 View full logs:</strong> <a href="{metrics.logs_url}">GitHub Actions</a></p>' if metrics.logs_url else ''}
        
        <p style="color: #666; font-size: 14px;">
        📧 This is an automated notification from Cold Email System.<br>
        🔄 Next drain scheduled according to GitHub Actions workflow.
        </p>
        </div>
        """

    def _send_email(self, subject: str, html_content: str) -> bool:
        """Send email using SendGrid"""
        try:
            message = Mail(
                from_email=Email(self.sender_email),
                to_emails=To(self.recipient_email),
                subject=subject,
                html_content=Content("text/html", html_content)
            )
            
            response = self.sg.send(message)
            
            if response.status_code in [200, 201, 202]:
                self.logger.info(f"✅ Email sent successfully (status: {response.status_code})")
                return True
            else:
                self.logger.error(f"❌ Email send failed with status: {response.status_code}")
                self.logger.error(f"Response body: {response.body}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ SendGrid API error: {e}")
            return False

# Example usage and testing functions
def test_sync_notification():
    """Test sync notification with sample data"""
    handler = NotificationHandler()
    
    metrics = SyncMetrics(
        timestamp="2025-01-15 14:30:25 UTC",
        duration_seconds=127.5,
        current_capacity=1950,
        max_capacity=2400,
        smb_leads_added=75,
        midsize_leads_added=25,
        total_leads_added=100,
        verified_count=92,
        failed_verification_count=8,
        verification_failure_disposition="Skipped - invalid format",
        final_inventory_count=2050,
        errors=[],
        logs_url="https://github.com/user/repo/actions/runs/123456"
    )
    
    return handler.send_sync_notification(metrics)

def test_drain_notification():
    """Test drain notification with sample data"""
    handler = NotificationHandler()
    
    metrics = DrainMetrics(
        timestamp="2025-01-15 16:45:10 UTC",
        duration_seconds=892.3,
        unique_leads_analyzed=1888,
        leads_skipped_24hr=1650,
        completed_leads=45,
        replied_leads=8,
        bounced_leads=3,
        unsubscribed_leads=2,
        stale_leads=1,
        total_identified_for_drain=59,
        successfully_deleted=58,
        failed_deletions=1,
        success_rate_percent=98.3,
        errors=["Warning: 1 lead failed deletion due to API timeout"],
        logs_url="https://github.com/user/repo/actions/runs/123457"
    )
    
    return handler.send_drain_notification(metrics)

if __name__ == "__main__":
    print("Testing notification handler...")
    
    # Test with sample data if environment is configured
    if os.getenv('SENDGRID_API_KEY') and os.getenv('NOTIFICATION_EMAIL'):
        print("Sending test sync notification...")
        test_sync_notification()
        
        print("Sending test drain notification...")
        test_drain_notification()
        
        print("Sending test error notification...")
        handler = NotificationHandler()
        handler.send_error_notification("Test Operation", "This is a test error", "Stack trace here")
    else:
        print("❌ SENDGRID_API_KEY and NOTIFICATION_EMAIL required for testing")
        print("Set environment variables and run again to test email delivery")