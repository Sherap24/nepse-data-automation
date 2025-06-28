#!/usr/bin/env python3
"""
Cloud-Optimized NEPSE Data Collector
Designed for GitHub Actions and serverless environments
"""

import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import time
import sys

try:
    import pytz
    NEPAL_TZ = pytz.timezone('Asia/Kathmandu')
except ImportError:
    os.system("pip install pytz")
    import pytz
    NEPAL_TZ = pytz.timezone('Asia/Kathmandu')

# Configuration
API_BASE_URL = "http://localhost:8000"

class CloudNepseCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'NEPSE-Cloud-Collector/1.0',
            'Accept': 'application/json'
        })
        
        # All available endpoints
        self.endpoints = {
            'floorsheet': '/Floorsheet',
            'price_volume': '/PriceVolume',
            'live_market': '/LiveMarket',
            'summary': '/Summary',
            'top_gainers': '/TopGainers',
            'top_losers': '/TopLosers',
            'nepse_index': '/NepseIndex',
            'supply_demand': '/SupplyDemand'
        }
        
        # Create directories
        os.makedirs("data", exist_ok=True)
        os.makedirs("logs", exist_ok=True)

    def log(self, message):
        """Logging function"""
        timestamp = datetime.now(NEPAL_TZ).strftime('%Y-%m-%d %H:%M:%S')
        log_message = f"[{timestamp}] {message}"
        print(log_message)
        
        with open("logs/cloud_collector.log", "a", encoding="utf-8") as f:
            f.write(log_message + "\n")

    def is_market_open(self, time=None):
        """Check if NEPSE market should be open"""
        if time is None:
            time = datetime.now(NEPAL_TZ)
        
        # Trading days: Sunday=6, Monday=0, Tuesday=1, Wednesday=2, Thursday=3
        trading_days = [6, 0, 1, 2, 3]
        if time.weekday() not in trading_days:
            return False
        
        # Market hours: 10:00-15:00 NPT
        market_open = time.replace(hour=10, minute=0, second=0, microsecond=0)
        market_close = time.replace(hour=15, minute=0, second=0, microsecond=0)
        
        return market_open <= time <= market_close

    def test_api_connection(self):
        """Test if the API is accessible"""
        try:
            response = self.session.get(f"{API_BASE_URL}/", timeout=10)
            if response.status_code == 200:
                self.log("✅ NepseAPI server is accessible")
                return True
            else:
                self.log(f"❌ API returned status {response.status_code}")
                return False
        except Exception as e:
            self.log(f"❌ Cannot connect to API: {e}")
            return False

    def fetch_endpoint_data(self, endpoint_name, endpoint_url):
        """Fetch data from a specific endpoint"""
        try:
            response = self.session.get(f"{API_BASE_URL}{endpoint_url}", timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if isinstance(data, list):
                self.log(f"✅ {endpoint_name}: {len(data)} records")
            else:
                self.log(f"✅ {endpoint_name}: Data retrieved")
            return data
            
        except Exception as e:
            self.log(f"❌ {endpoint_name}: {str(e)[:80]}...")
            return None

    def normalize_data(self, data, endpoint_name, timestamp):
        """Convert data to normalized format"""
        normalized_records = []
        
        if not data:
            return normalized_records
        
        if isinstance(data, list):
            for i, record in enumerate(data):
                if isinstance(record, dict):
                    normalized_record = {
                        'collection_timestamp': timestamp.isoformat(),
                        'collection_time_npt': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        'data_source': endpoint_name,
                        'record_id': f"{endpoint_name}_{i+1}",
                        'market_open': self.is_market_open(timestamp),
                        'collection_method': 'cloud_automated'
                    }
                    
                    # Add all fields from the original record
                    for key, value in record.items():
                        clean_key = str(key).lower().replace(' ', '_')
                        normalized_record[clean_key] = value
                    
                    normalized_records.append(normalized_record)
        
        elif isinstance(data, dict):
            normalized_record = {
                'collection_timestamp': timestamp.isoformat(),
                'collection_time_npt': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'data_source': endpoint_name,
                'record_id': f"{endpoint_name}_summary",
                'market_open': self.is_market_open(timestamp),
                'collection_method': 'cloud_automated'
            }
            
            for key, value in data.items():
                clean_key = str(key).lower().replace(' ', '_')
                normalized_record[clean_key] = value
            
            normalized_records.append(normalized_record)
        
        return normalized_records

    def collect_single_run(self):
        """Single data collection run - perfect for cloud functions"""
        timestamp = datetime.now(NEPAL_TZ)
        self.log(f"🚀 Starting cloud data collection at {timestamp.strftime('%H:%M:%S %Z')}")
        
        # Check if market should be open
        if not self.is_market_open(timestamp):
            self.log("📴 Market is closed - no collection needed")
            return None
        
        # Test API connection first
        if not self.test_api_connection():
            self.log("❌ API connection failed - cannot collect data")
            return None
        
        all_normalized_data = []
        successful_endpoints = []
        
        # Collect from all endpoints
        for endpoint_name, endpoint_url in self.endpoints.items():
            self.log(f"📊 Collecting from {endpoint_name}...")
            
            raw_data = self.fetch_endpoint_data(endpoint_name, endpoint_url)
            if raw_data:
                normalized_data = self.normalize_data(raw_data, endpoint_name, timestamp)
                if normalized_data:
                    all_normalized_data.extend(normalized_data)
                    successful_endpoints.append(endpoint_name)
        
        if not all_normalized_data:
            self.log("❌ No data collected from any endpoint")
            return None
        
        # Create unified DataFrame and save
        try:
            df = pd.DataFrame(all_normalized_data)
            
            # Add metadata
            df['total_endpoints_collected'] = len(successful_endpoints)
            df['endpoints_collected'] = ', '.join(successful_endpoints)
            df['github_run_number'] = os.environ.get('GITHUB_RUN_NUMBER', 'local')
            
            # Generate filename
            filename = f"nepse_cloud_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = os.path.join("data", filename)
            
            # Save CSV
            df.to_csv(filepath, index=False)
            
            # Save summary
            summary = {
                'collection_time': timestamp.isoformat(),
                'total_records': len(df),
                'successful_endpoints': successful_endpoints,
                'records_by_source': df['data_source'].value_counts().to_dict(),
                'filename': filename,
                'market_open': True,
                'collection_method': 'cloud_automated'
            }
            
            summary_file = os.path.join("data", f"cloud_summary_{timestamp.strftime('%Y%m%d_%H%M%S')}.json")
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            self.log(f"💾 Cloud collection successful: {filepath}")
            self.log(f"📊 Total records: {len(df)} from {len(successful_endpoints)} endpoints")
            
            # Log breakdown
            for source, count in summary['records_by_source'].items():
                self.log(f"   {source}: {count} records")
            
            return filepath
            
        except Exception as e:
            self.log(f"❌ Error saving data: {e}")
            return None

def main():
    """Main function for cloud execution"""
    collector = CloudNepseCollector()
    
    # Single collection run
    result = collector.collect_single_run()
    
    if result:
        print(f"SUCCESS: Data collected and saved to {result}")
        sys.exit(0)
    else:
        print("INFO: No data collected (market closed or API unavailable)")
        sys.exit(0)  # Not an error, just no data to collect

if __name__ == "__main__":
    main()
