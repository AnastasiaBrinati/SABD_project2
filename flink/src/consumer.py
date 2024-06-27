from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import TimeCharacteristic


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    # Define Kafka source
    kafka_source = FlinkKafkaConsumer(
        topics='input_topic',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'flink_consumer_group'}
    )

    # Define Kafka sink
    kafka_sink = FlinkKafkaProducer(
        topic='output_topic',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'localhost:9092'}
    )

    # Create a DataStream from the Kafka source
    kafka_stream = env.add_source(kafka_source)

    # Define the processing logic
    processed_stream = kafka_stream.map(lambda value: process(value), output_type=Types.STRING())

    # Add the Kafka sink to the processed stream
    processed_stream.add_sink(kafka_sink)

    # Execute the Flink job
    env.execute("Flink Kafka Consumer Example")


def process(value):
    # Processing logic here, e.g., converting to uppercase
    return value.upper()


if __name__ == '__main__':
    main()
