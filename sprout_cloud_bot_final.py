# sprout_cloud_bot_final_original_posts_only.py
import os
import requests
import time
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
import signal
import sys

load_dotenv()

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
        # Your actual credentials
        self.sprout_token = "MjY4NDU0N3wxNzU5NzA2OTYxfDU2N2Q1MzliLTZlMWItNGQxMC1hMjczLWIxNjlmNjlhY2EwZg=="
        self.zapier_webhook_url = "https://hooks.zapier.com/hooks/catch/14274742/u9avzcp/"
        
        self.sprout_base_url = "https://api.sproutsocial.com/v1"
        self.headers = {
            'Authorization': f'Bearer {self.sprout_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        self.customer_id = None
        self.topics = []
        
        logger.info("🤖 Sprout Social Cloud Bot initialized (ORIGINAL POSTS ONLY)")

    def setup(self):
        """Initialize bot with customer info and topics"""
        try:
            if not self._get_customer_info():
                return False
            if not self._get_listening_topics():
                return False
            logger.info(f"✅ Setup complete - monitoring {len(self.topics)} topics for original posts only")
            return True
        except Exception as e:
            logger.error(f"❌ Setup failed: {e}")
            return False

    def _get_customer_info(self):
        """Get customer ID from Sprout Social API"""
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
        """Get all listening topics for the customer"""
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
                for i, topic in enumerate(self.topics[:5]):
                    logger.info(f"   📱 {i+1}. {topic['name']}")
                return True
            else:
                logger.warning("No topics found")
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get topics: {e}")
            return False

    def get_topic_mentions(self, customer_id, topic_id, topic_name, hours_back=3):
        """UPDATED - Get only original posts, exclude comments and replies"""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)
        
        start_date_str = start_time.strftime('%Y-%m-%d')
        end_datetime_str = end_time.strftime('%Y-%m-%dT%H:%M:%S')
        
        payload = {
            "filters": [
                # Time filter
                f"created_time.in({start_date_str}..{end_datetime_str})",
                
                # REQUIRED: Network filter (removed TikTok as requested)
                "network.eq(TWITTER,INSTAGRAM,FACEBOOK,YOUTUBE,LINKEDIN,REDDIT,TUMBLR,WWW)",
                
                # NEW: Filter to ONLY original posts (exclude comments/replies)
                "post_type.eq(TWEET,RETWEET,RETWEET_COMMENT,FACEBOOK_POST,INSTAGRAM_MEDIA,LINKEDIN_COMPANY_UPDATE,LINKEDIN_PERSONAL_POST,YOUTUBE_VIDEO)"
            ],
            "fields": [
                "text",
                "from.name",
                "from.screen_name",
                "network",
                "perma_link", 
                "created_time",
                "sentiment",
                "hashtags",
                "post_type"  # Added to verify post types
            ],
            "metrics": [
                "likes",
                "shares_count", 
                "replies"
            ],
            "limit": 25,
            "sort": ["created_time:desc"]
        }

        try:
            logger.info(f"   📡 Checking: {topic_name} (Original Posts Only)")
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
                return []
                
            data = response.json()
            mentions = data.get('data', [])
            
            if mentions:
                logger.info(f"   📱 Found {len(mentions)} original posts in '{topic_name}'")
                # Log post types for verification
                post_types = [mention.get('post_type', 'unknown') for mention in mentions[:3]]
                logger.info(f"   📝 Post types: {', '.join(set(post_types))}")
            else:
                logger.info(f"   📭 No original posts found in '{topic_name}'")
            
            return mentions
            
        except Exception as e:
            logger.error(f"   ❌ Error for {topic_name}: {e}")
            return []

    def send_to_zapier(self, mention, topic_name):
        """Send mention data to Zapier webhook"""
        try:
            metrics = mention.get('metrics', {})
            
            webhook_data = {
                "topic_name": topic_name,
                "created_time": mention.get('created_time', ''),
                "author_name": mention.get('from', {}).get('name', 'Unknown'),
                "author_handle": mention.get('from', {}).get('screen_name', 'unknown'),
                "network": mention.get('network', 'Unknown'),
                "post_type": mention.get('post_type', 'Unknown'),  # Added post type
                "message_text": mention.get('text', 'No content')[:500],
                "sentiment": mention.get('sentiment', 'unknown'),
                "hashtags": ', '.join(mention.get('hashtags', [])[:5]),
                "likes": metrics.get('likes', 0),
                "replies": metrics.get('replies', 0),
                "shares": metrics.get('shares_count', 0),
                "permalink": mention.get('perma_link', ''),
                "priority": self._get_priority(mention, metrics),
                "webhook_timestamp": datetime.now().isoformat(),
                "bot_version": "cloud-v4.0-original-posts-only"
            }
            
            response = requests.post(
                self.zapier_webhook_url, 
                json=webhook_data, 
                timeout=30
            )
            response.raise_for_status()
            
            logger.info(f"   ✅ Sent to Zapier: @{webhook_data['author_handle']} ({webhook_data['post_type']})")
            return True
            
        except Exception as e:
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
                
                mentions = self.get_topic_mentions(self.customer_id, topic_id, topic_name, hours_back=3)
                
                for mention in mentions:
                    if self.send_to_zapier(mention, topic_name):
                        total_sent += 1
                    time.sleep(2)
                time.sleep(1)
            
            logger.info(f"✨ Cycle complete! Sent {total_sent} original posts to Zapier")
            return total_sent
            
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")
            return 0

    def run_forever(self):
        """Run continuous monitoring"""
        def signal_handler(signum, frame):
            logger.info("🛑 Received shutdown signal - stopping gracefully")
            sys.exit(0)
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        logger.info("🚀 Starting 24/7 cloud monitoring (ORIGINAL POSTS ONLY)")
        logger.info("📊 Checking every 3 hours for new original posts")
        logger.info("🚫 Filtering out: Comments, replies, and other non-original content")
        
        cycle_count = 0
        
        while True:
            try:
                cycle_count += 1
                logger.info("=" * 60)
                logger.info(f"📅 MONITORING CYCLE #{cycle_count}")
                logger.info(f"🕐 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
                logger.info("=" * 60)
                
                mentions_sent = self.run_monitoring_cycle()
                
                logger.info("=" * 60)
                if mentions_sent > 0:
                    logger.info(f"📈 SUCCESS: Sent {mentions_sent} original posts to Slack via Zapier")
                else:
                    logger.info("📭 COMPLETE: No new original posts found this cycle")
                
                next_check = datetime.now() + timedelta(hours=3)
                logger.info(f"⏰ Next check scheduled for: {next_check.strftime('%Y-%m-%d %H:%M:%S UTC')}")
                logger.info("😴 Bot sleeping for 3 hours...")
                logger.info("=" * 60)
                
                time.sleep(3 * 60 * 60)
                
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
    logger.info("🔧 Version: v4.0-original-posts-only")
    logger.info("📝 Filter: Original posts only (no comments/replies)")  
    logger.info("🚫 Excluded: LinkedIn comments, FB comments, IG comments, etc.")
    logger.info("⏰ Interval: Every 3 hours")
    logger.info("☁️  Platform: Railway Cloud")
    logger.info("=" * 60)
    
    bot = SproutSlackCloudBot()
    
    if not bot.setup():
        logger.error("❌ SETUP FAILED - Check your API credentials")
        sys.exit(1)
    
    logger.info("🎯 All systems ready - starting continuous monitoring")
    bot.run_forever()

if __name__ == "__main__":
    main()
