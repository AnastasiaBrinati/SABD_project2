from pyflink.common import Types, Time
from pyflink.datastream import DataStream, OutputTag
from pyflink.datastream.window import TumblingEventTimeWindows

from .functions import Query1AggregateFunction, Query2AggregateFunction, Query2ProcessWindowFunction
from .key_selectors import Query2KeySelector


def query_1(ds: DataStream, days: int = 1):
    print("Entering query 1 execution")
    # Takes only required data for each dataset entry (timestamp, vault_id, temperature)
    ds.map(lambda i: (i[0], i[4], i[6]), output_type=Types.TUPLE(
        [Types.STRING(), Types.INT(), Types.FLOAT()])
    ).filter(
        lambda i: 1000 <= i[1] <= 1020
    ).key_by(
        lambda i: i[1], key_type=Types.INT()
    ).window(
        TumblingEventTimeWindows.of(Time.minutes(days))
    ).aggregate(
        Query1AggregateFunction(),
        accumulator_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT()]),
        output_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT()])
    )

def query_2(ds: DataStream, window_size: int):
    # Takes only required data (timestamp, vault_id, model, serial_number, failure_flag)
    return ds.map(lambda i: (i[0], i[4], i[2], i[1], i[3]), output_type=Types.TUPLE(
        [Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT()])
    ).key_by(Query2KeySelector()).window(TumblingEventTimeWindows.of(Time.days(window_size))).aggregate(
        Query2AggregateFunction(),
        window_function=Query2ProcessWindowFunction(),
        accumulator_type=Types.TUPLE([Types.INT(), Types.LIST(Types.TUPLE([Types.STRING(), Types.STRING()]))])
    )

def query_3(ds: DataStream, window_size: Time):
    return ds

