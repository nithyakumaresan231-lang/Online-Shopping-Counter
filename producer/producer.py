import os
import time
import json
import csv
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_config():
    return {
        'bootstrap_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'topic': os.environ.get('KAFKA_TOPIC', 'shopping-transactions'),
        'delay': float(os.environ.get('PRODUCER_DELAY', '1.0')),
        'csv_path': os.path.join(os.path.dirname(__file__), '..', 'data', 'ecommerce_transactions.csv')
    }

def create_producer(bootstrap_servers):
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        logging.info(f"Connected to Kafka broker(s) at {bootstrap_servers}")
        return producer
    except KafkaError as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error connecting to Kafka: {e}")
        return None

def main():
    config = get_config()
    logging.info(f"Starting producer with config: {config}")

    producer = create_producer(config['bootstrap_servers'])
    if not producer:
        logging.error("Exiting due to Kafka connection failure.")
        return

    csv_path = config['csv_path']
    if not os.path.exists(csv_path):
        logging.error(f"CSV file not found: {csv_path}")
        return

    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            logging.info("Started publishing records...")
            
            for row in reader:
                # Convert specific fields to proper types if needed, but the prompt says:
                # "Preserve all original dataset fields in each JSON message."
                # We will send the dictionary directly, which will map to JSON string values.
                # If we need types to be numeric, json will serialize it as string unless we parse it.
                # In TRD: "Convert numeric fields to appropriate numeric types." is for Spark side. 
                # Let's keep it simple or convert known fields to float/int? 
                # The Data Schema says quantity integer, unit_price numeric, discount_percent numeric.
                # Spark can cast it during parsing. We will just send what csv.DictReader provides (strings),
                # or parse it here. Better to parse them here to make it valid JSON numbers, matching TRD's JSON example.
                
                try:
                    row['quantity'] = int(row['quantity'])
                    row['unit_price'] = float(row['unit_price'])
                    row['discount_percent'] = float(row['discount_percent'])
                except ValueError:
                    pass # Ignore conversion errors, keep as string

                future = producer.send(config['topic'], value=row)
                
                # Wait for the message to be sent to catch errors
                try:
                    future.get(timeout=10)
                    print(f"Published order_id: {row.get('order_id')}")
                except KafkaError as e:
                    logging.error(f"Failed to publish message: {e}")

                time.sleep(config['delay'])

    except KeyboardInterrupt:
        logging.info("Producer interrupted by user (Ctrl+C).")
    except Exception as e:
        logging.error(f"Error reading CSV or publishing: {e}")
    finally:
        if producer:
            logging.info("Flushing and closing Kafka producer...")
            producer.flush()
            producer.close()
            logging.info("Producer closed cleanly.")

if __name__ == '__main__':
    main()
