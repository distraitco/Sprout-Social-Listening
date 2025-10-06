# sprout_cloud_bot_fixed.py - Updated with API fix
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

class CloudSproutBot:
    def __init__(self):
        self.sprout_token = os.getenv('SPROUT_API_TOKEN')
        self.zapier_webhook_url = os.getenv('ZAPIER_WEBHOOK_URL') 
        
        if not self.sprout_token or not self.zapier_webhook_url:
            logger.error("Missing required environment variables")
            sys.exit(1)
        
        self.sprout_base_url = "https://api.sproutsocial.com/v1"
        self.headers = {
            'Authorization': f'Bearer {self.sprout_token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        self.customer_id = None
        self.topics = []
        
        logger.info("🤖 Cloud Sprout Bot initialized")

    def setup(self):
        """Initialize bot with customer info and topics"""
        try:
            if not self._get_customer_info():
                return False
            if not self._get_listening_topics():
                return False
            logger.info(f"✅ Setup complete - monitoring {len(self.topics)} topics")
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
                for topic in self.topics[:3]:
                    logger.info(f"   📱 {topic['name']}")
                return True
            else:
                logger.warning("No topics found")
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get topics: {e}")
            return False

    def get_topic_mentions(self, customer_id, topic_id, topic_name, hours_back=1):
        """FIXED - Get recent mentions from a listening topic"""
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)
        
        # Fixed payload format based on API documentation
        payload = {
            "filters": [
                # Use .. (two dots) for inclusive range, not ... (three dots)
                f"created_time.in({start_time.isoformat()}..{end_time.isoformat()})"
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
                "guid"
            ],
            "metrics": [
                "likes",
                "shares_count", 
                "replies"
            ],
            "limit": 10,  # Start small for testing
            "sort": ["created_time:desc"]
        }

        try:
            logger.info(f"   📡 Checking: {topic_name}")
            logger.info(f"   🕒 Time range: {start_time.strftime('%H:%M')} to {end_time.strftime('%H:%M')}")
            
            response = requests.post(
                f"{self.sprout_base_url}/{customer_id}/listening/topics/{topic_id}/messages",
                headers=self.headers,
                json=payload,
                timeout=60
            )
            
            if response.status_code != 200:
                logger.error(f"   ❌ API Error {response.status_code} for {topic_name}")
                logger.error(f"   💬 Response: {response.text[:200]}...")
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
            metrics = mention.get('metrics', {})
            
            webhook_data = {
                "topic_name": topic_name,
                "created_time": mention.get('created_time', ''),
                "author_name": mention.get('from', {}).get('name', 'Unknown'),
                "author_handle": mention.get('from', {}).get('screen_name', 'unknown'),
                "network": mention.get('network', 'Unknown'),
                "message_text": mention.get('text', 'No content')[:400],
                "sentiment": mention.get('sentiment', 'unknown'),
                "hashtags": ', '.join(mention.get('hashtags', [])[:5]),
                "likes": metrics.get('likes', 0),
                "replies": metrics.get('replies', 0),
                "shares": metrics.get('shares_count', 0),
                "permalink": mention.get('perma_link', ''),
                "priority": self._get_priority(mention, metrics),
                "webhook_timestamp": datetime.now().isoformat(),
                "bot_version": "cloud-v1.1-fixed"
            }
            
            response = requests.post(
                self.zapier_webhook_url, 
                json=webhook_data, 
                timeout=30
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
        elif total_engagement > 25:
            return 'HIGH'
        elif total_engagement > 5:
            return 'MEDIUM' 
        else:
            return 'LOW'

    def run_monitoring_cycle(self):
        """Run one complete monitoring cycle"""
        logger.info(f"🔍 Starting monitoring cycle at {datetime.now().strftime('%H:%M:%S UTC')}")
        
        total_sent = 0
        
        try:
            for topic in self.topics:
                topic_id = topic['id']
                topic_name = topic['name']
                
                # Get mentions
                mentions = self.get_topic_mentions(self.customer_id, topic_id, topic_name, hours_back=3)
                
                # Send each mention to Zapier
                for mention in mentions:
                    if self.send_to_zapier(mention, topic_name):
                        total_sent += 1
                    time.sleep(2)  # Rate limiting
                
                time.sleep(1)  # Small delay between topics
            
            logger.info(f"✨ Cycle complete! Sent {total_sent} mentions to Zapier")
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
        
        logger.info("🚀 Starting 24/7 cloud monitoring")
        logger.info("📊 Checking every 3 hours for new mentions")  # Updated message
        
        cycle_count = 0
        
        while True:
            try:
                cycle_count += 1
                logger.info(f"📅 Monitoring cycle #{cycle_count}")
                
                mentions_sent = self.run_monitoring_cycle()
                
                if mentions_sent > 0:
                    logger.info(f"📈 Sent {mentions_sent} mentions this cycle")
                else:
                    logger.info("📭 No new mentions found this cycle")
                
                logger.info("⏰ Waiting 3 hours until next check...")
                time.sleep(3 * 60 * 60)  # 3 hours
                
            except KeyboardInterrupt:
                logger.info("👋 Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Unexpected error in main loop: {e}")
                logger.info("🔄 Waiting 5 minutes before retry...")
                time.sleep(5 * 60)

def main():
    """Main function for cloud deployment"""
    logger.info("🌟 Sprout Social → Zapier → Slack Cloud Bot Starting (FIXED VERSION)")
    
    bot = CloudSproutBot()
    
    if not bot.setup():
        logger.error("❌ Setup failed - exiting")
        sys.exit(1)
    
    bot.run_forever()

if __name__ == "__main__":
    main()
