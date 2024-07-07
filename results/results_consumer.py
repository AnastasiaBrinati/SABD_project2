from kafka import KafkaConsumer
import json

def main():
    consumer = KafkaConsumer(
        'results_q1_1',  # Adjust the topic name as needed
        bootstrap_servers='kafka:9092',
        group_id='results_consumer_group',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        print(f"Received message: {message.value}")


if __name__ == "__main__":
    main()
