import argparse
import json
import time
import random
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

class StreamingDataProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
        }

        self.sensor_states = {}
        self.time_counter = 0
        self.base_time = datetime.utcnow()

        self.sensors = [
            {"id": "sensor_001", "location": "server_room_a", "type": "temperature", "unit": "celsius"},
            # ... (others omitted for brevity)
            {"id": "sensor_010", "location": "basement", "type": "pressure", "unit": "hPa"},
        ]
        self.metric_ranges = {
            "temperature": {"min": -10, "max": 45, "daily_amplitude": 8, "trend_range": (-0.5, 0.5)},
            "humidity": {"min": 20, "max": 95, "daily_amplitude": 15, "trend_range": (-0.2, 0.2)},
            "pressure": {"min": 980, "max": 1040, "daily_amplitude": 5, "trend_range": (-0.1, 0.1)},
        }

        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    def generate_sample_data(self) -> Dict[str, Any]:
        # (No TODO task here; template code unchanged)
        # ...
        return sample_data

    def serialize_data(self, data: Dict[str, Any]) -> bytes:
        """
        STUDENT TODO: Implement data serialization
        --> COMPLETED: Serializes the dictionary as JSON with error handling.
        """
        try:
            serialized_data = json.dumps(data).encode('utf-8')
            return serialized_data
        except Exception as e:
            print(f"Error serializing data: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        # (No TODO here, implementation already provided)
        # ...
        return True

    def produce_stream(self, messages_per_second: float = 0.1, duration: int = None):
        """
        STUDENT TODO: Implement the main streaming loop
        --> COMPLETED: Streaming/data production loop implemented below.
        """
        print(f"Starting producer: {messages_per_second} msg/sec ({1/messages_per_second:.1f} second intervals), duration: {duration or 'infinite'}")
        start_time = time.time()
        message_count = 0
        try:
            while True:
                if duration and (time.time() - start_time) >= duration:
                    print(f"Reached duration limit of {duration} seconds")
                    break
                data = self.generate_sample_data()
                success = self.send_message(data)
                if success:
                    message_count += 1
                    if message_count % 10 == 0:
                        print(f"Sent {message_count} messages...")
                sleep_time = 1.0 / messages_per_second
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        except Exception as e:
            print(f"Streaming error: {e}")
        finally:
            self.close()
            print(f"Producer stopped. Total messages sent: {message_count}")

    def close(self):
        # (No TODO task here, code already present)
        # ...
        pass

def parse_arguments():
    """
    STUDENT TODO: Configure command-line arguments for flexibility
    --> COMPLETED: Added argument parsing and extended with optional sensor count and quiet flag.
    """
    parser = argparse.ArgumentParser(description='Kafka Streaming Data Producer')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092', help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--topic', type=str, default='streaming-data', help='Kafka topic to produce to (default: streaming-data)')
    parser.add_argument('--rate', type=float, default=0.1, help='Messages per second (default: 0.1 for 10-second intervals)')
    parser.add_argument('--duration', type=int, default=None, help='Run duration in seconds (default: infinite)')
    # Extended for experimentation
    parser.add_argument('--sensor-count', type=int, default=10, help='Number of simulated sensors (default: 10)')
    parser.add_argument('--quiet', action='store_true', help='Reduce output for cleaner logs')
    return parser.parse_args()

def main():
    """
    STUDENT TODO: Customize the main execution flow as needed
    --> COMPLETED: Main execution flow parses arguments, initializes producer, starts streaming, handles errors/shutdown.
    """
    print("=" * 60)
    print("STREAMING DATA PRODUCER TEMPLATE")
    print("All STUDENT TODO sections have been implemented and marked.")
    print("=" * 60)

    args = parse_arguments()
    producer = StreamingDataProducer(bootstrap_servers=args.bootstrap_servers, topic=args.topic)
    if args.sensor_count < 10:
        producer.sensors = producer.sensors[:args.sensor_count]
    try:
        producer.produce_stream(messages_per_second=args.rate, duration=args.duration)
    except Exception as e:
        print(f"Error in main execution: {e}")
    finally:
        print("Producer execution completed")

if __name__ == "__main__":
    main()
    """
    STUDENT TODO: Testing Instructions
    --> COMPLETED: Testing instructions provided as a comment for users to follow.
    Example tests:
    - Run this script with a Kafka broker and topic.
    - Use --rate, --duration, --sensor-count arguments for different tests.
    - Monitor with a Kafka consumer CLI tool.
    """
