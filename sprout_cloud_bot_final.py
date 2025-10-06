# sprout_cloud_bot_final.py - Complete working version with correct date formats
import os
import requests
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
import signal
import sys

# Load environment variables
load_dotenv()

# Setup logging for cloud deployment
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('sprout_bot.log')
    ]
)
logger = logging.getLogger(__name__)

class SproutSlackCloudBot:
    def __init__(self):
        # Your actual credentials (hardcoded for reliability)
        self.sprout_token = "MjY4NDU0N3wxNzU5NzA2OTYxfDU2N2Q1MzliLTZlMWItNGQxMC1hMjdjLWIxNjlmNjlhY2EwZg=="
        self.zapier_webhook_url = "https://hooks.zapier.com/hooks/catch/14274742/u9avzcp/"
        
        # API configuration based on documentation [[11]]
        self.sprout_base_url = "https://api.sproutsocial.com/v1"
        self.headers = {
            'Authorization': f'Bearer {self.sprout_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        self.customer_id = None
        self.topics = []
        
        logger.info("🤖 Sprout Social Cloud Bot initialized (FINAL VERSION)")

    def setup(self):
        """Initialize bot with customer info and topics"""
        try:
            # Get customer info
            if not self._get_customer_info():
                return False
                
            # Get listening topics
            if not self._get_listening_topics():
                return False
                
            logger.info(f"✅ Setup complete - monitoring {len(self.topics)} topics")
            return True
            
        except Exception as e:
            logger.error(f"❌ Setup failed: {e}")
            return False

    def _get_customer_info(self):
        """Get customer ID from Sprout Social API [[11]]"""
        try:
            response = requests.get(f"{self.sprout_base_url}/metadata/client", headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('data') and len(data['data']) > 0:
                self.customer_id = data['data'][0]['customer_id']
                customer_name = data['data'][0]['name']
                logger.info(f"✅ Connected to: {customer_name} (ID: {self.customer_id})")
                return True
            else:
                logger.error("No customer data found")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get customer info: {e}")
            return False

    def _get_listening_topics(self):
        """Get all listening topics for the customer [[11]]"""
        try:
            response = requests.get(
                f"{self.sprout_base_url}/{self.customer_id}/metadata/customer/topics",
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get('data'):
                self.topics = data['data']
                logger.info(f"✅ Found {len(self.topics)} listening topics")
                for i, topic in enumerate(self.topics[:5]):  # Log first 5
                    logger.info(f"   📱 {i+1}. {topic['name']} (ID: {topic['id']})")
                return True
            else:
                logger.warning("No topics found")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get topics: {e}")
            return False

    def get_topic_mentions(self, customer_id, topic_id, topic_name, hours_back=3):
        """FIXED - Get recent mentions with correct date format and required network filter [[11]]"""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)
        
        # CORRECTED: Use exact format from API documentation [[11]]
        # Format: "created_time.in(2022-11-28..2022-12-29T23:59:59)"
        start_date_str = start_time.strftime('%Y-%m-%d')
        end_datetime_str = end_time.strftime('%Y-%m-%dT%H:%M:%S')
        
        # FIXED: Must include network filter for Listening Messages endpoint [[11]]
        payload = {
            "filters": [
                # Correct date format with .. (two dots for inclusive range) [[11]]
                f"created_time.in({start_date_str}..{end_datetime_str})",
                # REQUIRED: Network filter for Listening Messages API [[11]]
                "network.eq(TWITTER,INSTAGRAM,FACEBOOK,YOUTUBE,LINKEDIN,REDDIT,TUMBLR,WWW,TIKTOK)"
            ],
            "fields": [
                "text",
                "from.name",
                "from.screen_name",
                "network",
                "perma_link", 
                "created_time",
                "sentiment",
                "hashtags"
            ],
            "metrics": [
                "likes",
                "shares_count", 
                "replies"
            ],
            "limit": 25,  # Reasonable limit for cloud processing
            "sort": ["created_time:desc"]
        }

        try:
            logger.info(f"   📡 Checking: {topic_name}")
            logger.info(f"   🕒 Date range: {start_date_str} to {end_datetime_str}")
            
            response = requests.post(
                f"{self.sprout_base_url}/{customer_id}/listening/topics/{topic_id}/messages",
                headers=self.headers,
                json=payload,
                timeout=60
            )
            
            if response.status_code != 200:
                logger.error(f"   ❌ API Error {response.status_code} for {topic_name}")
                logger.error(f"   💬 Response: {response.text[:300]}...")
                logger.error(f"   📤 Filter sent: created_time.in({start_date_str}..{end_datetime_str})")
                return []
                
            data = response.json()
            mentions = data.get('data', [])
            
            if mentions:
                logger.info(f"   📱 Found {len(mentions)} mentions in '{topic_name}'")
            else:
                logger.info(f"   📭 No mentions found in '{topic_name}'")
            
            return mentions
            
        except requests.exceptions.RequestException as e:
            logger.error(f"   ❌ Request failed for {topic_name}: {e}")
            return []
        except Exception as e:
            logger.error(f"   ❌ Unexpected error for {topic_name}: {e}")
            return []

    def send_to_zapier(self, mention, topic_name):
        """Send mention data to Zapier webhook"""
        try:
            # Get metrics safely
            metrics = mention.get('metrics', {})
            
            # Create webhook payload
            webhook_data = {
                # Basic info
                "topic_name": topic_name,
                "created_time": mention.get('created_time', ''),
                
                # Author info
                "author_name": mention.get('from', {}).get('name', 'Unknown'),
                "author_handle": mention.get('from', {}).get('screen_name', 'unknown'),
                "network": mention.get('network', 'Unknown'),
                
                # Content
                "message_text": mention.get('text', 'No content')[:500],  # Limit length
                "sentiment": mention.get('sentiment', 'unknown'),
                "hashtags": ', '.join(mention.get('hashtags', [])[:5]),
                
                # Engagement metrics
                "likes": metrics.get('likes', 0),
                "replies": metrics.get('replies', 0),
                "shares": metrics.get('shares_count', 0),
                
                # Links and priority
                "permalink": mention.get('perma_link', ''),
                "priority": self._get_priority(mention, metrics),
                
                # Metadata
                "webhook_timestamp": datetime.now().isoformat(),
                "bot_version": "cloud-v3.0-final-corrected"
            }
            
            response = requests.post(
                self.zapier_webhook_url, 
                json=webhook_data, 
                timeout=30,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()
            
            logger.info(f"   ✅ Sent to Zapier: @{webhook_data['author_handle']}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"   ❌ Failed to send to Zapier: {e}")
            return False

    def _get_priority(self, mention, metrics):
        """Calculate priority based on sentiment and engagement"""
        sentiment = mention.get('sentiment', 'unknown').lower()
        total_engagement = metrics.get('likes', 0) + metrics.get('replies', 0) + metrics.get('shares_count', 0)
        
        if sentiment == 'negative' and total_engagement > 10:
            return 'URGENT'
        elif sentiment == 'negative':
            return 'HIGH'
        elif total_engagement > 50:
            return 'HIGH'
        elif total_engagement > 10:
            return 'MEDIUM' 
        else:
            return 'LOW'

    def run_monitoring_cycle(self):
        """Run one complete monitoring cycle"""
        logger.info(f"🔍 Starting monitoring cycle at {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        total_sent = 0
        
        try:
            for i, topic in enumerate(self.topics):
                topic_id = topic['id']
                topic_name = topic['name']
                
                logger.info(f"📊 Processing topic {i+1}/{len(self.topics)}: {topic_name}")
                
                # Get mentions for the last 3 hours
                mentions = self.get_topic_mentions(self.customer_id, topic_id, topic_name, hours_back=3)
                
                # Send each mention to Zapier
                for mention in mentions:
                    if self.send_to_zapier(mention, topic_name):
                        total_sent += 1
                    
                    # Rate limiting - respect API limits (60 requests/minute) [[11]]
                    time.sleep(2)
                
                # Small delay between topics
                time.sleep(1)
            
            logger.info(f"✨ Cycle complete! Sent {total_sent} mentions to Zapier")
            return total_sent
            
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")
            return 0

    def run_forever(self):
        """Run continuous monitoring with proper cloud handling"""
        
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info("🛑 Received shutdown signal - stopping gracefully")
            sys.exit(0)
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        logger.info("🚀 Starting 24/7 cloud monitoring")
        logger.info("📊 Checking every 3 hours for new mentions")
        logger.info("🌐 Monitoring all major social networks")
        
        cycle_count = 0
        
        while True:
            try:
                cycle_count += 1
                logger.info("=" * 60)
                logger.info(f"📅 MONITORING CYCLE #{cycle_count}")
                logger.info(f"🕐 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
                logger.info("=" * 60)
                
                # Run monitoring cycle
                mentions_sent = self.run_monitoring_cycle()
                
                # Log cycle summary
                logger.info("=" * 60)
                if mentions_sent > 0:
                    logger.info(f"📈 SUCCESS: Sent {mentions_sent} mentions to Slack via Zapier")
                else:
                    logger.info("📭 COMPLETE: No new mentions found this cycle")
                
                next_check = datetime.now() + timedelta(hours=3)
                logger.info(f"⏰ Next check scheduled for: {next_check.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                logger.info("😴 Bot sleeping for 3 hours...")
                logger.info("=" * 60)
                
                # Wait 3 hours before next cycle
                time.sleep(3 * 60 * 60)  # 3 hours = 10,800 seconds
                
            except KeyboardInterrupt:
                logger.info("👋 Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Unexpected error in main loop: {e}")
                logger.info("🔄 Waiting 10 minutes before retry...")
                time.sleep(10 * 60)

def main():
    """Main function for cloud deployment"""
    logger.info("🌟 SPROUT SOCIAL → ZAPIER → SLACK CLOUD BOT")
    logger.info("=" * 60)
    logger.info("🔧 Version: v3.0-final-corrected")
    logger.info("📅 Date format: Fixed and API compliant")
    logger.info("🌐 Network filter: Required and included")
    logger.info("⏰ Interval: Every 3 hours")
    logger.info("☁️  Platform: Railway Cloud")
    logger.info("=" * 60)
    
    # Initialize bot
    bot = SproutSlackCloudBot()
    
    # Setup
    if not bot.setup():
        logger.error("❌ SETUP FAILED - Check your API credentials")
        logger.error("💡 Troubleshooting:")
        logger.error("   - Verify Sprout Social API token is valid")
        logger.error("   - Ensure listening topics exist in your account")
        logger.error("   - Check API rate limits")
        sys.exit(1)
    
    # Start monitoring
    logger.info("🎯 All systems ready - starting continuous monitoring")
    logger.info("🚀 Bot will run 24/7 and check every 3 hours")
    bot.run_forever()

if __name__ == "__main__":
    main()
