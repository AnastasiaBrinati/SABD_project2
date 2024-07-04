from pyflink.common import Types, Time
from pyflink.datastream import DataStream
from pyflink.datastream.window import TumblingEventTimeWindows

from .functions import Query1AggregateFunction, Query2AggregateFunction, Query2ProcessWindowFunction
from .key_selectors import Query2KeySelector


def query_1(ds: DataStream, window_size: Time):
    # Takes only required data for each dataset entry (timestamp, vault_id, temperature)
    ds.map(lambda i: (i[0], i[4], i[25]), output_type=Types.TUPLE(
        [Types.INT(), Types.DOUBLE(), Types.TUPLE([Types.INT(), Types.DOUBLE(), Types.DOUBLE()])]))
    # Filter entries by vault_id, then takes a windows of size :window_size and applies the aggregate function
    ds.filter(lambda i: 1000 <= i[0] <= 1200).key_by(lambda i: i[0]).window(
        TumblingEventTimeWindows.of(Time.days(window_size))
    ).aggregate(
        Query1AggregateFunction(),
        accumulator_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT()]),
        output_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT()]))
    return ds


def query_2(ds: DataStream, window_size: Time):
    # Takes only required data (timestamp, vault_id, model, serial_number, failure_flag)
    ds.map(lambda i: (i[0], i[4], i[2], i[1], i[3]), output_type=Types.TUPLE(
        [Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT()]))
    ds.key_by(Query2KeySelector()).window(TumblingEventTimeWindows.of(Time.days(window_size))).aggregate(
        Query2AggregateFunction(),
        window_function=Query2ProcessWindowFunction(),
        accumulator_type=Types.TUPLE([Types.INT(), Types.LIST(Types.TUPLE([Types.STRING(), Types.STRING()]))])
    )

def query_3(ds: DataStream, window_size: Time):
    return ds

