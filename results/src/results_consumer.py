import os
import json
import csv
import logging
import sys
import time
from kafka import KafkaConsumer


def create_consumer(bootstrap_servers, group_id, topic, max_retries=10, retry_delay=10):
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False
            )
            print(f"Connected to Kafka broker on attempt {attempt + 1}")
            return consumer
        except Exception as e:
            print(f"Attempt {attempt + 1} to connect to Kafka broker failed: {e}")
            time.sleep(retry_delay*2)
    raise Exception("Failed to connect to Kafka broker after multiple attempts")


def write_csv(file_path, record):
    mode = 'a' if os.path.exists(file_path) else 'w'
    with open(file_path, mode, newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=record.keys())
        if mode == 'w':
            writer.writeheader()
        writer.writerow(record)


def consume_and_write(bootstrap_servers, group_id, topic):
    consumer = create_consumer(bootstrap_servers, group_id, topic)
    csv_file_path = f"./{topic}.csv"
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
    try:
        for message in consumer:
            try:
                record = json.loads(message.value)
                print(f"record: {record}")
                if not isinstance(record, dict):
                    raise ValueError("The record is not a dictionary.")
                write_csv(csv_file_path, record)
                consumer.commit()
            except Exception as e:
                print(f"Unexpected error for message: {message.value}. Error: {e}")
    except Exception as e:
        print(f"Error while consuming messages from Kafka: {e}")
    finally:
        consumer.close()



if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python results_consumer.py <topic>")
        sys.exit(1)

    topic = sys.argv[1]

    consume_and_write(
        bootstrap_servers='kafka:9092',
        group_id='kafka_consumer_group',
        topic=topic
    )
