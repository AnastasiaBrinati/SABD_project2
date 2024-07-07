import json
import sys
from datetime import datetime

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, SinkFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import MapFunction

from queries.queries import query_1, query_2, query_3


# Custom Sink Function to write to a single CSV file
class CsvSinkFunction(SinkFunction):
    def __init__(self, file_path):
        self.file_path = file_path

    def open(self, parameters):
        self.file = open(self.file_path, 'a')  # Open file in append mode

    def invoke(self, value, context):
        with open(self.file_path, 'a') as f:
            f.write(value + "\n")

    def close(self):
        if self.file:
            self.file.close()


class ParseJsonArrayFunction(MapFunction):
    def map(self, value):
        # Deserialize the JSON array string to a Python list
        json_data = json.loads(value)
        if isinstance(json_data, list):
            # Convert list to tuple
            return (json_data[0], json_data[1], json_data[2], int(json_data[3]), int(json_data[4]),
                    float(0 if json_data[12] == '' else json_data[12]),
                    float(0 if json_data[25] == '' else json_data[25]))
        else:
            raise ValueError("Expected a JSON array")


# just for debugging
class PrintFunction(MapFunction):
    def map(self, value):
        print(f"Record received: {value}")
        return value


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp) -> int:
        ts = datetime.strptime(element[0], "%Y-%m-%dT%H:%M:%S.%f").timestamp()
        return int(ts) * 1000


def main(query, window):
    global res, side_output_stream
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")

    kafka_consumer = FlinkKafkaConsumer(topics='sabd', deserialization_schema=SimpleStringSchema(),
                                        properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'sabd_consumers',
                                                    'security.protocol': 'PLAINTEXT'})

    # Definizione della strategia di watermark
    watermark_strategy = (WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(MyTimestampAssigner()))
    # for_bounded_out_of_orderness: Questa funzione imposta un ritardo massimo tollerabile di 10 secondi.
    #                               Significa che Flink accetta eventi che arrivano con un massimo di 10 secondi
    #                               di ritardo rispetto all'evento più recente già processato
    # with_timestamp_assigner:      Assegna il timestamp corretto agli eventi basandosi sul campo timestamp
    #                               degli eventi stessi

    ds = env.add_source(kafka_consumer)

    # Parse the JSON array strings to tuples
    parsed_stream = ds.map(ParseJsonArrayFunction(),
                           output_type=Types.TUPLE([Types.STRING(),  # '2023-04-01T00:00:00.000000'
                                                    Types.STRING(),  # 'AAH92V6H'
                                                    Types.STRING(),  # 'HGST HUH721212ALN604'
                                                    Types.INT(),  # '0'
                                                    Types.INT(),  # '1122'
                                                    Types.FLOAT(),  # '0.0'
                                                    Types.FLOAT()  # '0.0'
                                                    ]))

    # Print the parsed tuples
    if query == 'q1':
        res = query_1(parsed_stream, watermark_strategy=watermark_strategy, days=window)
    if query == 'q2':
        # Print the parsed tuples using the chosen print function
        res = query_2(parsed_stream, watermark_strategy=watermark_strategy, days=window)
    if query == 'q3':
        # Print the parsed tuples using the chosen print function
        ds = query_3(parsed_stream, window)

    # Add the custom CSV sink to the transformed stream
    # windowed_stream.add_sink(CsvSinkFunction("results.csv"))

    res.print()

    env.execute("Flink Kafka Consumer Example")


if __name__ == '__main__':
    # Get the print function argument from the command line
    if len(sys.argv) != 3:
        print("Usage: python script.py <query> <window>")
        sys.exit(1)

    query = sys.argv[1]
    window_size = sys.argv[2]

    if window_size != '1' and window_size != '3' and window_size != '0':
        print("Available windows: 1, 3, all")
        sys.exit(1)

    if query != 'q1' and query != 'q2' and query != 'q3':
        print("Available queries: q1, q2, q3")
        sys.exit(1)

    main(query, int(window_size))
