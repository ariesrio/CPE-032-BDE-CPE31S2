"""
Kafka Producer Template for Streaming Data Dashboard
COMPLETED TODO: Big Data Streaming Data Producer

This is a template for students to build a Kafka producer that generates and sends
streaming data to Kafka for consumption by the dashboard.

DO NOT MODIFY THE TEMPLATE STRUCTURE - IMPLEMENT THE TODO SECTIONS
"""

import requests
import argparse
import json
import time
import random
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List
from pymongo import MongoClient # added for MongoDB usage

# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


class StreamingDataProducer:
    """
    Enhanced Kafka producer with stateful synthetic data generation
    This class handles Kafka connection, realistic data generation, and message sending
    """
    
    def __init__(self, bootstrap_servers: str, topic: str, mongo_uri: str = "mongodb+srv://<username>:<password>@<cluster>.mongodb.net/", mongo_db: str = "streaming_data", api_key: str = None):
        """
        Initialize Kafka producer configuration with stock market API integration
        
        Parameters:
        - bootstrap_servers: Kafka broker addresses (e.g., "localhost:9092")
        - topic: Kafka topic to produce messages to
        - mongo_uri: MongoDB connection URI
        - mongo_db: MongoDB database name
        - api_key: Alpha Vantage API key for stock market data
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.api_key = api_key or "demo"  # Use demo key if none provided
        
        # Kafka producer configuration
        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
            # 'security_protocol': 'SSL',  # If using SSL
            # 'ssl_cafile': 'path/to/ca.pem',  # If using SSL
            # 'ssl_certfile': 'path/to/service.cert',  # If using SSL
            # 'ssl_keyfile': 'path/to/service.key',  # If using SSL
            # 'compression_type': 'gzip',  # Optional: Enable compression
            # 'batch_size': 16384,  # Optional: Tune batch size
            # 'linger_ms': 10,  # Optional: Wait for batch fill
        }
        
        # Stock symbols to track
        self.stock_symbols = [
            {"symbol": "AAPL", "name": "Apple Inc."},
            {"symbol": "MSFT", "name": "Microsoft Corporation"},
            {"symbol": "GOOGL", "name": "Alphabet Inc."},
            {"symbol": "AMZN", "name": "Amazon.com Inc."},
            {"symbol": "TSLA", "name": "Tesla Inc."},
            {"symbol": "META", "name": "Meta Platforms Inc."},
            {"symbol": "NVDA", "name": "NVIDIA Corporation"},
            {"symbol": "JPM", "name": "JPMorgan Chase & Co."},
        ]
        
        # API call tracking to avoid rate limits (Alpha Vantage free tier: 5 calls/min, 500 calls/day)
        self.last_api_call = None
        self.api_call_delay = 12  # seconds between API calls (5 per minute = 12 seconds)
        self.current_symbol_index = 0
        
        # Cache for stock data
        self.stock_cache = {}
        self.cache_duration = 60  # Cache data for 60 seconds
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None
        
        # Initialize MongoDB connection for historical storage
        try:
            self.mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            # Test connection
            self.mongo_client.admin.command('ping')
            self.db = self.mongo_client[mongo_db]
            self.collection = self.db['sensor_data']
            print(f"MongoDB connected: {mongo_uri} -> {mongo_db}")
        except Exception as e:
            print(f"WARNING: MongoDB connection failed: {e}. Historical storage disabled.")
            self.mongo_client = None
            self.db = None
            self.collection = None

    def fetch_stock_data(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch real-time stock data from Alpha Vantage API
        
        Parameters:
        - symbol: Stock ticker symbol (e.g., "AAPL", "MSFT")
        
        Returns:
        Dictionary with stock data or None if fetch fails
        """
        # Check cache first
        current_time = datetime.utcnow()
        cache_key = symbol
        
        if cache_key in self.stock_cache:
            cached_data, cache_time = self.stock_cache[cache_key]
            if (current_time - cache_time).total_seconds() < self.cache_duration:
                print(f"Using cached data for {symbol}")
                return cached_data
        
        # Respect API rate limits
        if self.last_api_call:
            elapsed = (current_time - self.last_api_call).total_seconds()
            if elapsed < self.api_call_delay:
                wait_time = self.api_call_delay - elapsed
                print(f"Rate limit: waiting {wait_time:.1f} seconds...")
                time.sleep(wait_time)
        
        try:
            # Alpha Vantage API endpoint for real-time quote
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={self.api_key}"
            
            print(f"Fetching stock data for {symbol} from Alpha Vantage API...")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            self.last_api_call = datetime.utcnow()
            
            # Check for API errors
            if "Error Message" in data:
                print(f"API Error: {data['Error Message']}")
                return None
            
            if "Note" in data:
                print(f"API Rate Limit: {data['Note']}")
                return None
            
            # Extract quote data
            if "Global Quote" in data and data["Global Quote"]:
                quote = data["Global Quote"]
                
                stock_data = {
                    "symbol": symbol,
                    "price": float(quote.get("05. price", 0)),
                    "volume": float(quote.get("06. volume", 0)),
                    "change": float(quote.get("09. change", 0)),
                    "change_percent": quote.get("10. change percent", "0%").replace("%", ""),
                    "latest_trading_day": quote.get("07. latest trading day", ""),
                }
                
                # Cache the result
                self.stock_cache[cache_key] = (stock_data, current_time)
                
                return stock_data
            else:
                print(f"No quote data available for {symbol}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching stock data for {symbol}: {e}")
            return None
        except (KeyError, ValueError) as e:
            print(f"Error parsing stock data for {symbol}: {e}")
            return None

    def generate_sample_data(self) -> Dict[str, Any]:
        """
        Generate streaming data from real stock market API
        
        Fetches real-time stock prices from Alpha Vantage API and formats them
        for the streaming dashboard.
        
        Expected data format (must include these fields for dashboard compatibility):
        {
            "timestamp": "2023-10-01T12:00:00Z",  # ISO format timestamp
            "value": 123.45,                      # Stock price
            "metric_type": "stock_price",         # Type of metric
            "sensor_id": "AAPL",                  # Stock symbol
            "location": "Apple Inc.",             # Company name
            "unit": "USD",                        # Currency unit
        }
        """
        
        # Rotate through stock symbols
        stock_info = self.stock_symbols[self.current_symbol_index]
        self.current_symbol_index = (self.current_symbol_index + 1) % len(self.stock_symbols)
        
        symbol = stock_info["symbol"]
        name = stock_info["name"]
        
        # Fetch real stock data from API
        stock_data = self.fetch_stock_data(symbol)
        
        if stock_data and stock_data["price"] > 0:
            # Use real API data
            sample_data = {
                "timestamp": datetime.utcnow().isoformat() + 'Z',
                "value": round(stock_data["price"], 2),
                "metric_type": "stock_price",
                "sensor_id": symbol,
                "location": name,
                "unit": "USD",
                "volume": stock_data["volume"],
                "change": stock_data["change"],
                "change_percent": stock_data["change_percent"],
            }
            print(f"📈 {symbol}: ${stock_data['price']:.2f} ({stock_data['change_percent']}%)")
        else:
            # Fallback to simulated data if API fails
            print(f"⚠️  API unavailable, using simulated data for {symbol}")
            # Generate realistic stock price (random walk from previous value)
            if symbol not in self.stock_cache:
                base_price = random.uniform(50, 500)  # Initial price
            else:
                base_price = self.stock_cache.get(symbol, ({"price": 100}, None))[0].get("price", 100)
            
            # Random walk: small change from previous price
            change_percent = random.uniform(-2, 2)  # -2% to +2% change
            new_price = base_price * (1 + change_percent / 100)
            
            sample_data = {
                "timestamp": datetime.utcnow().isoformat() + 'Z',
                "value": round(new_price, 2),
                "metric_type": "stock_price",
                "sensor_id": symbol,
                "location": name,
                "unit": "USD",
                "volume": random.randint(1000000, 50000000),
                "change": round(new_price - base_price, 2),
                "change_percent": f"{change_percent:.2f}",
            }
            
            # Update cache with simulated data
            self.stock_cache[symbol] = ({"price": new_price}, datetime.utcnow())
        
        return sample_data

    def serialize_data(self, data: Dict[str, Any]) -> bytes:
        """
        COMPLETED TODO: Implement data serialization
        
        Convert the data dictionary to bytes for Kafka transmission.
        Common formats: JSON, Avro, Protocol Buffers
        
        For simplicity, we use JSON serialization in this template.
        Consider using Avro for better schema evolution in production.
        """
        
        # COMPLETED TODO: Choose and implement your serialization method
        try:
            # JSON serialization (simple but less efficient)
            serialized_data = json.dumps(data).encode('utf-8')
            
            # COMPLETED TODO: Consider using Avro for better performance and schema management
            # from avro import schema, datafile, io
            # serialized_data = avro_serializer.serialize(data)
            
            return serialized_data
        except Exception as e:
            print(f"COMPLETED TODO: Implement proper error handling for serialization: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Implement message sending to Kafka
        
        Parameters:
        - data: Dictionary containing the message data
        
        Returns:
        - bool: True if message was sent successfully, False otherwise
        """
        
        # Check if producer is initialized
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False
        
        # Serialize the data
        serialized_data = self.serialize_data(data)
        if not serialized_data:
            print("ERROR: Serialization failed")
            return False
        
        try:
            # Send message to Kafka
            future = self.producer.send(self.topic, value=serialized_data)
            # Wait for send confirmation with timeout
            result = future.get(timeout=10)
            print(f"Message sent successfully - Topic: {self.topic}, Data: {data}")
            return True
            
        except KafkaError as e:
            print(f"Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during send: {e}")
            return False

    def produce_stream(self, messages_per_second: float = 0.1, duration: int = None):
        """
        COMPLETED TODO: Implement the main streaming loop
        
        Parameters:
        - messages_per_second: Rate of message production (default: 0.1 for 10-second intervals)
        - duration: Total runtime in seconds (None for infinite)
        """
        
        print(f"Starting producer: {messages_per_second} msg/sec ({1/messages_per_second:.1f} second intervals), duration: {duration or 'infinite'}")
        
        start_time = time.time()
        message_count = 0
        
        try:
            while True:
                # Check if we've reached the duration limit
                if duration and (time.time() - start_time) >= duration:
                    print(f"Reached duration limit of {duration} seconds")
                    break
                
                # Generate and send data
                data = self.generate_sample_data()
                success = self.send_message(data)
                
                if success:
                    # Store to MongoDB for historical analysis
                    self.store_to_mongodb(data)
                    message_count += 1
                    if message_count % 10 == 0:  # Print progress every 10 messages
                        print(f"Sent {message_count} messages...")
                
                # Calculate sleep time to maintain desired message rate
                sleep_time = 1.0 / messages_per_second
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        except Exception as e:
            print(f"Streaming error: {e}")
        finally:
            # Implement proper cleanup
            self.close()
            print(f"Producer stopped. Total messages sent: {message_count}")

    def store_to_mongodb(self, data: Dict[str, Any]) -> bool:
        """
        Store streaming data to MongoDB for historical analysis
        
        Parameters:
        - data: Dictionary containing the message data
        
        Returns:
        - bool: True if data was stored successfully, False otherwise
        """
        if self.collection is None:
            return False
        
        try:
            # Convert timestamp string to datetime for better querying
            data_copy = data.copy()
            if 'timestamp' in data_copy:
                # Parse ISO format timestamp
                timestamp_str = data_copy['timestamp']
                if timestamp_str.endswith('Z'):
                    timestamp_str = timestamp_str[:-1] + '+00:00'
                data_copy['timestamp'] = datetime.fromisoformat(timestamp_str)
            
            # Insert document into MongoDB
            result = self.collection.insert_one(data_copy)
            return result.inserted_id is not None
            
        except Exception as e:
            print(f"MongoDB storage error: {e}")
            return False
    
    def close(self):
        """Implement producer cleanup and resource release"""
        if self.producer:
            try:
                # Ensure all messages are sent
                self.producer.flush(timeout=10)
                # Close producer connection
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")
        
        # Close MongoDB connection
        if self.mongo_client:
            try:
                self.mongo_client.close()
                print("MongoDB connection closed successfully")
            except Exception as e:
                print(f"Error closing MongoDB connection: {e}")


def parse_arguments():
    """COMPLETED TODO: Configure command-line arguments for flexibility"""
    parser = argparse.ArgumentParser(description='Kafka Streaming Data Producer')
    
    # COMPLETED TODO: Add additional command-line arguments as needed
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    
    parser.add_argument(
        '--topic',
        type=str,
        default='streaming-data',
        help='Kafka topic to produce to (default: streaming-data)'
    )
    
    parser.add_argument(
        '--rate',
        type=float,
        default=0.1,
        help='Messages per second (default: 0.1 for 10-second intervals)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Run duration in seconds (default: infinite)'
    )
    
    parser.add_argument(
        '--mongo-uri',
        type=str,
        default='mongodb+srv://<username>:<password>@<cluster>.mongodb.net/',
        help='MongoDB Atlas connection URI (format: mongodb+srv://username:password@cluster.mongodb.net/)'
    )
    
    parser.add_argument(
        '--mongo-db',
        type=str,
        default='streaming_data',
        help='MongoDB database name (default: streaming_data)'
    )
    
    parser.add_argument(
        '--api-key',
        type=str,
        default='demo',
        help='Alpha Vantage API key (default: demo). Get free key at https://www.alphavantage.co/support/#api-key'
    )
    
    return parser.parse_args()


def main():
    """
    COMPLETED TODO: Customize the main execution flow as needed
    
    Implementation Steps:
    1. Parse command-line arguments
    2. Initialize Kafka producer
    3. Start streaming data
    4. Handle graceful shutdown
    """
    
    print("=" * 60)
    print("STREAMING DATA PRODUCER - STOCK MARKET DATA")
    print("Data Source: Alpha Vantage API")
    print("=" * 60)
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Initialize producer with MongoDB and API configuration
    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db,
        api_key=args.api_key
    )
    
    # Start producing stream
    try:
        producer.produce_stream(
            messages_per_second=args.rate,
            duration=args.duration
        )
    except Exception as e:
        print(f"Main execution error: {e}")
    finally:
        print("Producer execution completed")


# COMPLETED TODO: Testing Instructions
if __name__ == "__main__":
    main()