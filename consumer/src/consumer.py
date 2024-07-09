import json
import sys
from datetime import datetime

from pyflink.common import Configuration, Row
from pyflink.common import Types
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.functions import MapFunction

from queries.custom_maps import query2_output_map_function, TimeMap, query1_output_map_function, \
    query3_output_map_function
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


def main(query_name):
    global side_output_stream, res, mapped_data1, mapped_data2, mapped_data3, serialization_schema, timing1, timing2, timing3
    config = Configuration()
    config.set_string("jobmanager.rpc.address", "jobmanager")  # Modifica con l'hostname del JobManager
    config.set_integer("jobmanager.rpc.port", 6123)  # Porta del JobManager

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(4)
    # env.add_jars("./lib/flink-sql-connector-kafka-1.17.1.jar")

    # Sourcing
    kafka_consumer = FlinkKafkaConsumer(topics='sabd', deserialization_schema=SimpleStringSchema(),
                                        properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'sabd_consumers',
                                                    'security.protocol': 'PLAINTEXT'})

    # Defining the Watermark Strategy
    watermark_strategy = (WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(MyTimestampAssigner()))
    #
    # with_timestamp_assigner: Assegna il timestamp corretto agli eventi basandosi sul campo timestamp degli eventi stessi

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
    if query_name == 'q1':
        result_columns = ["timestamp", "vault_id", "count", "mean_s194", "stddev_s194"]

        serialization_schema = JsonRowSerializationSchema.builder().with_type_info(Types.ROW_NAMED(result_columns,
                                                                                                   [Types.STRING(),
                                                                                                    Types.INT(),
                                                                                                    Types.INT(),
                                                                                                    Types.FLOAT(),
                                                                                                    Types.FLOAT()])).build()

        res1 = query_1(parsed_stream, watermark_strategy=watermark_strategy, days=1)
        res2 = query_1(parsed_stream, watermark_strategy=watermark_strategy, days=3)
        res3 = query_1(parsed_stream, watermark_strategy=watermark_strategy, days=23)

        mapped_output_type = Types.ROW_NAMED(result_columns,
                                             [Types.STRING(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()])
        mapped_data1 = res1.map(func=lambda i: Row(i[0], i[1], i[2], i[3], i[4]), output_type=mapped_output_type)

        mapped_data2 = res2.map(func=lambda i: Row(i[0], i[1], i[2], i[3], i[4]), output_type=mapped_output_type)

        mapped_data3 = res3.map(func=lambda i: Row(i[0], i[1], i[2], i[3], i[4]), output_type=mapped_output_type)

        # Retrieving timing metrics
        t1 = res1.map(func=TimeMap(), output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))
        t2 = res2.map(func=TimeMap(), output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))
        t3 = res3.map(func=TimeMap(), output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))

        timing1 = t1.map(func=lambda i: Row(i[0], i[1]), output_type=Types.ROW_NAMED(
            ['throughput', 'latency'],
            [Types.FLOAT(), Types.FLOAT()]))
        timing2 = t2.map(func=lambda i: Row(i[0], i[1]), output_type=Types.ROW_NAMED(
            ['throughput', 'latency'],
            [Types.FLOAT(), Types.FLOAT()]))
        timing3 = t3.map(func=lambda i: Row(i[0], i[1]), output_type=Types.ROW_NAMED(
            ['throughput', 'latency'],
            [Types.FLOAT(), Types.FLOAT()]))

    if query_name == 'q2':
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

        res1 = query_2(parsed_stream, watermark_strategy=watermark_strategy, days=1)
        res2 = query_2(parsed_stream, watermark_strategy=watermark_strategy, days=3)
        res3 = query_2(parsed_stream, watermark_strategy=watermark_strategy, days=23)

        mapped_data1 = res1.map(query2_output_map_function, output_type=fields)
        mapped_data2 = res2.map(query2_output_map_function, output_type=fields)
        mapped_data3 = res3.map(query2_output_map_function, output_type=fields)

        # Retrieving timing metrics
        t1 = res1.map(func=TimeMap(), output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))
        t2 = res2.map(func=TimeMap(), output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))
        t3 = res3.map(func=TimeMap(), output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))

        timing1 = t1.map(func=lambda i: Row(i[0], i[1]), output_type=Types.ROW_NAMED(
            ['throughput', 'latency'],
            [Types.FLOAT(), Types.FLOAT()]))
        timing2 = t2.map(func=lambda i: Row(i[0], i[1]), output_type=Types.ROW_NAMED(
            ['throughput', 'latency'],
            [Types.FLOAT(), Types.FLOAT()]))
        timing3 = t3.map(func=lambda i: Row(i[0], i[1]), output_type=Types.ROW_NAMED(
            ['throughput', 'latency'],
            [Types.FLOAT(), Types.FLOAT()]))

    if query_name == 'q3':
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

        res1 = query_3(parsed_stream, watermark_strategy=watermark_strategy, days=1)
        res2 = query_3(parsed_stream, watermark_strategy=watermark_strategy, days=3)
        res3 = query_3(parsed_stream, watermark_strategy=watermark_strategy, days=23)

        mapped_output_type = Types.ROW_NAMED(result_columns,
                                             [Types.STRING(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT(),
                                              Types.FLOAT(), Types.INT(), Types.INT()])
        mapped_data1 = res1.map(func=query3_output_map_function, output_type=mapped_output_type)

        mapped_data2 = res2.map(func=query3_output_map_function, output_type=mapped_output_type)

        mapped_data3 = res3.map(func=query3_output_map_function, output_type=mapped_output_type)

        # Retrieving timing metrics
        t1 = res1.map(func=TimeMap(), output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))
        t2 = res2.map(func=TimeMap(), output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))
        t3 = res3.map(func=TimeMap(), output_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()]))

        timing1 = t1.map(func=lambda i: Row(i[0], i[1]), output_type=Types.ROW_NAMED(
            ['throughput', 'latency'],
            [Types.FLOAT(), Types.FLOAT()]))
        timing2 = t2.map(func=lambda i: Row(i[0], i[1]), output_type=Types.ROW_NAMED(
            ['throughput', 'latency'],
            [Types.FLOAT(), Types.FLOAT()]))
        timing3 = t3.map(func=lambda i: Row(i[0], i[1]), output_type=Types.ROW_NAMED(
            ['throughput', 'latency'],
            [Types.FLOAT(), Types.FLOAT()]))

    # Sinking
    timing_result_columns = ["throughput", "latency"]
    timing_serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        Types.ROW_NAMED(timing_result_columns,
                        [
                            Types.FLOAT(),
                            Types.FLOAT()])).build()
    producer_config = {'bootstrap.servers': 'kafka:9092', 'group.id': 'sabd_producers',
                       'security.protocol': 'PLAINTEXT'}

    kafka_producer1 = FlinkKafkaProducer(topic=f"{query_name}_1", serialization_schema=serialization_schema,
                                         producer_config=producer_config)
    kafka_metrics1 = FlinkKafkaProducer(topic=f"m_{query_name}_1", serialization_schema=timing_serialization_schema,
                                        producer_config=producer_config)

    # Add the sink to the data stream
    mapped_data1.add_sink(kafka_producer1)
    timing1.add_sink(kafka_metrics1)

    kafka_producer2 = FlinkKafkaProducer(topic=f'{query_name}_3', serialization_schema=serialization_schema,
                                         producer_config=producer_config)
    kafka_metrics2 = FlinkKafkaProducer(topic=f'm_{query_name}_3', serialization_schema=timing_serialization_schema,
                                        producer_config=producer_config)
    # Add the sink to the data stream
    mapped_data2.add_sink(kafka_producer2)
    timing2.add_sink(kafka_metrics2)

    kafka_producer3 = FlinkKafkaProducer(topic=f'{query_name}_all', serialization_schema=serialization_schema,
                                         producer_config=producer_config)
    kafka_metrics3 = FlinkKafkaProducer(topic=f'm_{query_name}_all', serialization_schema=timing_serialization_schema,
                                        producer_config=producer_config)
    # Add the sink to the data stream
    mapped_data3.add_sink(kafka_producer3)
    timing3.add_sink(kafka_metrics3)

    env.execute("Query " + query_name)


if __name__ == '__main__':
    # Get the print function argument from the command line

    if len(sys.argv) != 2:
        print("Usage: python script.py <query>")
        sys.exit(1)

    query = sys.argv[1]

    if query != 'q1' and query != 'q2' and query != 'q3':
        print("Available queries: q1, q2, q3")
        sys.exit(1)

    main(query)
