import os
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()

import argparse
import json
import time
import requests
from datetime import datetime
from typing import Dict, Any, List
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from pymongo import MongoClient

class CoinGeckoCryptoProducer:
    """
    Kafka producer for CoinGecko API cryptocurrency data streaming
    Free tier: 10,000 calls/month, 30 calls/minute (Much better than Alpha Vantage!)
    """

    def __init__(self, bootstrap_servers: str, topic: str, mongo_uri: str, symbols: List[str], api_key: str = None):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.symbols = symbols
        self.base_url = "https://api.coingecko.com/api/v3"
        self.api_key = api_key
        
        # Kafka producer configuration
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            print(f"✓ Kafka producer initialized: {bootstrap_servers} -> {topic}")
        except NoBrokersAvailable:
            print(f"✗ ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"✗ ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None
        
        # MongoDB connection for historical storage
        try:
            self.mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            self.db = self.mongo_client['crypto_streaming']
            self.collection = self.db['crypto_data']
            # Test connection
            self.mongo_client.admin.command('ping')
            print(f"✓ MongoDB connected successfully")
        except Exception as e:
            print(f"✗ ERROR: Failed to connect to MongoDB: {e}")
            self.mongo_client = None
            self.collection = None

    def fetch_crypto_data(self, coin_id: str) -> Dict[str, Any]:
        """
        Fetch current cryptocurrency data from CoinGecko API
        Coin IDs: bitcoin, ethereum, binancecoin, tether, etc.
        """
        try:
            # Prepare headers
            headers = {}
            if self.api_key:
                headers['x-cg-demo-api-key'] = self.api_key
            
            # Get price data with market stats
            params = {
                'ids': coin_id,
                'vs_currencies': 'usd',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true',
                'include_last_updated_at': 'true'
            }
            
            price_url = f"{self.base_url}/simple/price"
            response = requests.get(price_url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            
            price_data = response.json()
            
            if coin_id not in price_data:
                print(f"✗ No data returned for {coin_id}")
                return None
            
            coin_data = price_data[coin_id]
            
            # Format data matching our schema
            data = {
                'timestamp': datetime.now().isoformat() + 'Z',
                'value': float(coin_data['usd']),
                'metric_type': 'crypto_price',
                'sensor_id': coin_id.upper(),
                'symbol': coin_id.upper(),
                'from_currency': coin_id.capitalize(),
                'from_currency_name': coin_id.capitalize(),
                'to_currency': 'USD',
                'to_currency_name': 'US Dollar',
                'exchange_rate': float(coin_data['usd']),
                'last_refreshed': datetime.now().isoformat(),
                'timezone': 'UTC',
                'bid_price': float(coin_data['usd']) * 0.9995,  # Approximate bid
                'ask_price': float(coin_data['usd']) * 1.0005,  # Approximate ask
                'market_cap': float(coin_data.get('usd_market_cap', 0)),
                'volume_24h': float(coin_data.get('usd_24h_vol', 0)),
                'price_change_24h': float(coin_data.get('usd_24h_change', 0)),
                'price_change_percent_24h': float(coin_data.get('usd_24h_change', 0)),
                'last_updated': coin_data.get('last_updated_at', int(time.time()))
            }
            
            return data
            
        except requests.exceptions.Timeout:
            print(f"✗ Timeout fetching data for {coin_id}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"✗ Request error for {coin_id}: {e}")
            return None
        except KeyError as e:
            print(f"✗ Missing data field for {coin_id}: {e}")
            return None
        except Exception as e:
            print(f"✗ Error fetching data for {coin_id}: {e}")
            return None

    def send_to_kafka(self, data: Dict[str, Any]) -> bool:
        """Send data to Kafka topic"""
        if self.producer is None or not data:
            return False
        
        try:
            future = self.producer.send(self.topic, value=data)
            future.get(timeout=10)
            return True
        except Exception as e:
            print(f"✗ Kafka send error: {e}")
            return False

    def store_to_mongodb(self, data: Dict[str, Any]) -> bool:
        """Store data to MongoDB for historical queries"""
        if self.collection is None or not data:
            return False
        
        try:
            # Create a copy to avoid modifying original
            mongo_data = data.copy()
            mongo_data['_id'] = f"{data['sensor_id']}_{data['timestamp']}"
            mongo_data['inserted_at'] = datetime.utcnow()
            
            self.collection.insert_one(mongo_data)
            return True
        except Exception as e:
            # Duplicate key errors are expected (same timestamp)
            if 'duplicate key error' not in str(e).lower():
                print(f"✗ MongoDB insert error: {e}")
            return False

    def produce_stream(self, interval: float = 60):
        """
        Main streaming loop
        Fetches crypto data and sends to Kafka and MongoDB
        
        Args:
            interval: seconds between data fetches (default: 60)
        """
        print(f"\n{'='*60}")
        print(f"Starting CoinGecko Crypto Stream")
        print(f"Symbols: {', '.join(self.symbols)}")
        print(f"Interval: {interval} seconds")
        print(f"Note: CoinGecko free tier = 10,000 calls/month, 30/min")
        print(f"{'='*60}\n")
        
        message_count = 0
        successful_symbols = []
        
        try:
            while True:
                cycle_start = time.time()
                
                for symbol in self.symbols:
                    data = self.fetch_crypto_data(symbol)
                    
                    if data:
                        # Send to Kafka
                        kafka_success = self.send_to_kafka(data)
                        
                        # Store to MongoDB
                        mongo_success = self.store_to_mongodb(data)
                        
                        if kafka_success:
                            message_count += 1
                            status = "✓ Kafka" + (" + MongoDB" if mongo_success else "")
                            change_emoji = "📈" if data.get('price_change_percent_24h', 0) > 0 else "📉"
                            print(f"{status} | {symbol.upper()}: ${data['value']:,.4f} {change_emoji} {data.get('price_change_percent_24h', 0):.2f}% | Count: {message_count}")
                            
                            if symbol not in successful_symbols:
                                successful_symbols.append(symbol)
                        else:
                            print(f"⚠ Failed to send {symbol} to Kafka")
                    else:
                        print(f"⚠ Skipping {symbol} - no data received")
                    
                    # Rate limiting: respect 30 calls/min = 2 seconds between calls
                    time.sleep(2)
                
                # Show summary of working symbols
                if successful_symbols:
                    print(f"\n✓ Successfully streaming: {', '.join([s.upper() for s in successful_symbols])}")
                
                # Calculate wait time for next cycle
                cycle_duration = time.time() - cycle_start
                wait_time = max(0, interval - cycle_duration)
                
                if wait_time > 0:
                    print(f"⏳ Waiting {wait_time:.0f}s until next cycle...\n")
                    time.sleep(wait_time)
                
        except KeyboardInterrupt:
            print("\n\n⚠ Producer interrupted by user")
        except Exception as e:
            print(f"\n✗ Streaming error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.close()
            print(f"\n{'='*60}")
            print(f"Producer stopped. Total messages: {message_count}")
            print(f"{'='*60}")

    def close(self):
        """Cleanup resources"""
        if self.producer is not None:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("✓ Kafka producer closed")
            except Exception as e:
                print(f"✗ Error closing Kafka producer: {e}")
        
        if self.mongo_client is not None:
            try:
                self.mongo_client.close()
                print("✓ MongoDB connection closed")
            except Exception as e:
                print(f"✗ Error closing MongoDB: {e}")

def parse_arguments():
    """Command-line argument parser"""
    parser = argparse.ArgumentParser(
        description='CoinGecko API Cryptocurrency Data Streaming Producer (10K calls/month free!)'
    )
    
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    
    parser.add_argument(
        '--topic',
        type=str,
        default='crypto-stream',
        help='Kafka topic (default: crypto-stream)'
    )
    
    parser.add_argument(
        '--mongo-uri',
        type=str,
        default='mongodb://localhost:27017/',
        help='MongoDB connection URI'
    )
    
    parser.add_argument(
        '--api-key',
        type=str,
        default=None,
        help='CoinGecko Demo API key (optional, get free at coingecko.com)'
    )
    
    parser.add_argument(
        '--symbols',
        type=str,
        nargs='+',
        default=['bitcoin', 'ethereum', 'binancecoin', 'tether', 'usd-coin'],
        help='Crypto coin IDs from CoinGecko (default: bitcoin ethereum binancecoin tether usd-coin)'
    )
    
    parser.add_argument(
        '--interval',
        type=float,
        default=60,
        help='Seconds between data fetches (default: 60, recommended for rate limits)'
    )
    
    return parser.parse_args()

def main():
    """Main execution"""
    print("\n" + "="*60)
    print("COINGECKO API CRYPTOCURRENCY STREAMING PRODUCER")
    print("="*60 + "\n")
    
    args = parse_arguments()
    
    print("Configuration:")
    print(f"  Broker: {args.bootstrap_servers}")
    print(f"  Topic: {args.topic}")
    print(f"  Symbols: {', '.join(args.symbols)}")
    print(f"  Interval: {args.interval}s")
    print(f"  API Key: {'Yes' if args.api_key else 'No (using free tier)'}")
    print()
    
    producer = CoinGeckoCryptoProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        mongo_uri=args.mongo_uri,
        symbols=args.symbols,
        api_key=args.api_key
    )
    
    producer.produce_stream(interval=args.interval)

if __name__ == "__main__":
    main()
