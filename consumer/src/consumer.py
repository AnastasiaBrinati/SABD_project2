import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import MapFunction


class ParseJsonArrayFunction(MapFunction):
    def map(self, value):
        # Deserialize the JSON array string to a Python list
        json_data = json.loads(value)
        if isinstance(json_data, list):
            # Convert list to tuple
            return tuple(json_data)
        else:
            raise ValueError("Expected a JSON array")


class PrintFunction(MapFunction):
    def map(self, value):
        print(f"Record received: {value}")
        return value


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")

    kafka_consumer = FlinkKafkaConsumer(
        topics='sabd',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'sabd_consumers', 'security.protocol': 'PLAINTEXT'}
    )

    ds = env.add_source(kafka_consumer)

    # Parse the JSON array strings to tuples
    parsed_stream = ds.map(ParseJsonArrayFunction(), output_type=Types.TUPLE([
        Types.STRING(),  # '2023-04-01T00:00:00.000000'
        Types.STRING(),  # 'AAH92V6H'
        Types.STRING(),  # 'HGST HUH721212ALN604'
        Types.STRING(),  # '0'
        Types.STRING(),  # '1122'
        Types.STRING(),  # '0.0'
        Types.STRING(),  # '0.0'
        Types.STRING(),  # '0.0'
        Types.STRING()   # '5.0'
    ]))

    # Print the parsed tuples
    parsed_stream.map(PrintFunction()).set_parallelism(1)

    env.execute("Flink Kafka Consumer Example")


if __name__ == '__main__':
    main()
