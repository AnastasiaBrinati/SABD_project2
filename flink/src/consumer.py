from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import DeserializationSchema
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.table import Row
import json

class JsonArrayToRowDeserializationSchema(DeserializationSchema):
    def __init__(self, type_info):
        self.type_info = type_info

    def deserialize(self, message):
        # Parse the JSON array
        json_array = json.loads(message)
        # Ensure it is a list (array)
        if isinstance(json_array, list):
            # Convert the JSON array to a Flink Row
            return Row(*json_array)
        else:
            raise ValueError("Expected a JSON array")

    def is_end_of_stream(self, next_element):
        return False

    def get_produced_type(self):
        return self.type_info

# Example JSON deserialization schema for the elements in the array
row_type_info = RowTypeInfo(
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
)

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")

    # Custom deserialization schema to handle JSON arrays
    json_array_to_row_deserialization_schema = JsonArrayToRowDeserializationSchema(type_info=row_type_info)

    kafka_consumer = FlinkKafkaConsumer(
        topics='sabd',
        deserialization_schema=json_array_to_row_deserialization_schema,
        properties={'bootstrap.servers': 'kafka:9092', 'group.id': 'sabd_consumers', 'security.protocol': 'PLAINTEXT'}
    )

    ds = env.add_source(kafka_consumer)

    def print_func(record):
        print(f"Record received: {record}")

    ds.map(print_func).set_parallelism(1)

    env.execute("Flink Kafka Consumer Example")

if __name__ == '__main__':
    main()
