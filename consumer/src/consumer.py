import json
import sys
from datetime import datetime
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.window import TumblingEventTimeWindows, Time
from pyflink.common import WatermarkStrategy, time
from pyflink.common.watermark_strategy import TimestampAssigner

from queries.functions import Query1AggregateFunction
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


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        print("element: " + element[2:28])
        return datetime.strptime(element[2:28], "%Y-%m-%dT%H:%M:%S.%f").timestamp() * 1000


def main(query, window):
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")

    kafka_consumer = FlinkKafkaConsumer(
        topics='sabd',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'sabd_consumers', 'security.protocol': 'PLAINTEXT'}
    )

    # Definizione della strategia di watermark
    watermark_strategy = (WatermarkStrategy.for_bounded_out_of_orderness(time.Duration.of_seconds(10))
                          .with_timestamp_assigner(MyTimestampAssigner()))
    # for_bounded_out_of_orderness: Questa funzione imposta un ritardo massimo tollerabile di 10 secondi.
    #                               Significa che Flink accetta eventi che arrivano con un massimo di 10 secondi
    #                               di ritardo rispetto all'evento più recente già processato
    # with_timestamp_assigner: Assegna il timestamp corretto agli eventi basandosi sul campo timestamp degli eventi stessi

    ds = env.add_source(kafka_consumer).assign_timestamps_and_watermarks(watermark_strategy)

    # Parse the JSON array strings to tuples
    parsed_stream = ds.map(ParseJsonArrayFunction(), output_type=Types.TUPLE([
        Types.STRING(),  # '2023-04-01T00:00:00.000000'
        Types.STRING(),  # 'AAH92V6H'
        Types.STRING(),  # 'HGST HUH721212ALN604'
        Types.INT(),  # '0'
        Types.INT(),  # '1122'
        Types.FLOAT(),  # '0.0'
        Types.FLOAT(),  # '0.0'
        Types.FLOAT(),  # '0.0'
        Types.FLOAT()  # '5.0'
    ]))

    # Print the parsed tuples
    if query == 'q1':
        # Print the parsed tuples using the chosen print function
        filtered_stream = (parsed_stream.map(lambda i: (i[0], int(i[4]), i[25]))
                  .filter(lambda i: 1000 <= i[1] <= 1200))

        if __name__ == '__main__':
            windowed_stream = filtered_stream.key_by(lambda i: i[0]).window(TumblingEventTimeWindows.of(Time.days(window))).aggregate(
                Query1AggregateFunction(),
                accumulator_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT()]),
                output_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT()])
            )
        #res_ds = windowed_stream.apply(aggregate_statistics, output_type=Types.ROW([Types.SQL_TIMESTAMP(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()]))
        windowed_stream.map(PrintFunction()).set_parallelism(1)
    if query == 'q2':
        # Print the parsed tuples using the chosen print function
        ds = query_2(parsed_stream, window)
    if query == 'q3':
        # Print the parsed tuples using the chosen print function
        ds = query_3(parsed_stream, window)

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
