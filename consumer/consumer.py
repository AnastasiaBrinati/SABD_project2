from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'nifi_topic',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8'))

for message in consumer:
    print(f"Received message: {message.value}")
