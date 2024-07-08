import json
import sys
from datetime import datetime

from pyflink.common import Configuration
from pyflink.common import Row, Types
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.functions import MapFunction

from queries.queries import query_1, query_2, query_3


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


# Define a simple MapFunction to convert data to string format
class MapToString(MapFunction):
    def map(self, value):
        r = ""
        for i in len(value):
            r + f"{value[i]},"
        return r


def main(query, window):
    global side_output_stream, res
    config = Configuration()
    config.set_string("jobmanager.rpc.address", "jobmanager")  # Modifica con l'hostname del JobManager
    config.set_integer("jobmanager.rpc.port", 6123)  # Porta del JobManager

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    # env.add_jars("./lib/flink-sql-connector-kafka-1.17.1.jar")

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
        result_columns = ["timestamp", "vault_id", "count", "mean_s194", "stddev_s194"]

        serialization_schema = JsonRowSerializationSchema.builder().with_type_info(Types.ROW_NAMED(result_columns,
                                                                                                   [Types.STRING(),
                                                                                                    Types.INT(),
                                                                                                    Types.INT(),
                                                                                                    Types.FLOAT(),
                                                                                                    Types.FLOAT()])).build()

        res = query_1(parsed_stream, watermark_strategy=watermark_strategy, days=window)

        mapped_data = res.map(func=lambda i: Row(i[0], i[1], i[2], i[3], i[4]),
            output_type=Types.ROW_NAMED(result_columns,
                                        [Types.STRING(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()]))

    if query == 'q2':
        fields = Types.ROW_NAMED(["timestamp", "vault id1", "failures1 ([modelA, serialA, ...])", "vault id2",
                                  "failures2 ([modelA, serialA, ...])", "vault id3",
                                  "failures3 ([modelA, serialA, ...])", "vault id4",
                                  "failures4 ([modelA, serialA, ...])", "vault id5",
                                  "failures5 ([modelA, serialA, ...])", "vault id6",
                                  "failures6 ([modelA, serialA, ...])", "vault id7",
                                  "failures7 ([modelA, serialA, ...])", "vault id8",
                                  "failures8 ([modelA, serialA, ...])", "vault id9",
                                  "failures9 ([modelA, serialA, ...])", "vault id10",
                                  "failures10 ([modelA, serialA, ...])", ],
                                 [Types.STRING(), Types.INT(), Types.STRING(), Types.INT(), Types.STRING(), Types.INT(),
                                  Types.STRING(), Types.INT(), Types.STRING(), Types.INT(), Types.STRING(), Types.INT(),
                                  Types.STRING(), Types.INT(), Types.STRING(), Types.INT(), Types.STRING(), Types.INT(),
                                  Types.STRING(), Types.INT(), Types.STRING()])
        serialization_schema = JsonRowSerializationSchema.builder().with_type_info(fields).build()

        # Print the parsed tuples using the chosen print function
        res = query_2(parsed_stream, watermark_strategy=watermark_strategy, days=window)

        mapped_data = res.map(
            func=lambda i: Row(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], i[13],
                               i[14], i[15], i[16], i[17], i[18], i[19], i[20]), output_type=fields)
    if query == 'q3':
        result_columns = ["ts", "vault_id", "min", "25perc", "50perc", "75perc", "max", "count"]

        serialization_schema = JsonRowSerializationSchema.builder().with_type_info(Types.ROW_NAMED(result_columns,
                                                                                                   [Types.STRING(),
                                                                                                    Types.INT(),
                                                                                                    Types.INT(),
                                                                                                    Types.FLOAT(),
                                                                                                    Types.FLOAT(),
                                                                                                    Types.FLOAT(),
                                                                                                    Types.INT(),
                                                                                                    Types.INT()])).build()

        res = query_3(parsed_stream, watermark_strategy=watermark_strategy, days=window)

        mapped_data = res.map(func=lambda i: Row(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]),
            output_type=Types.ROW_NAMED(result_columns,
                                        [Types.STRING(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT(),
                                         Types.FLOAT(), Types.INT(), Types.INT()]))

    kafka_producer = FlinkKafkaProducer(topic=f'{query}_{str(window)}', serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'kafka:9092', 'group.id': 'sabd_producers',
                         'security.protocol': 'PLAINTEXT'})

    # Add the sink to the data stream
    mapped_data.add_sink(kafka_producer)

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
