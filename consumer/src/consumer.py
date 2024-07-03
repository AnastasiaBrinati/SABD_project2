import json
import sys
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import MapFunction

from queries.queries import query_1, query_2, query_3


class ParseJsonArrayFunction(MapFunction):
    def map(self, value):
        # Deserialize the JSON array string to a Python list
        json_data = json.loads(value)
        if isinstance(json_data, list):
            # Convert list to tuple
            return tuple(json_data)
        else:
            raise ValueError("Expected a JSON array")


# just for debugging
class PrintFunction(MapFunction):
    def map(self, value):
        print(f"Record received: {value}")
        return value


def main(query, window):
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
        Types.STRING()  # '5.0'
    ]))

    # Print the parsed tuples
    if query == 'q1':
        # Print the parsed tuples using the chosen print function
        ds = query_1(parsed_stream, window)
    if query == 'q2':
        # Print the parsed tuples using the chosen print function
        parsed_stream.map(PrintFunction()).set_parallelism(1)
    if query == 'q3':
        # Print the parsed tuples using the chosen print function
        parsed_stream.map(PrintFunction()).set_parallelism(1)

    env.execute("Flink Kafka Consumer Example")


if __name__ == '__main__':
    # Get the print function argument from the command line
    if len(sys.argv) != 3:
        print("Usage: python script.py <query> <window>")
        sys.exit(1)

    query = sys.argv[1]
    window_size = sys.argv[2]

    if window_size != '1' and window_size != '3' and window_size != 'all':
        print("Available windows: 1, 3, all")
        sys.exit(1)

    if query != 'q1' and query != 'q2' and query != 'q3':
        print("Available queries: q1, q2, q3")
        sys.exit(1)

    main(query, int(window_size))
