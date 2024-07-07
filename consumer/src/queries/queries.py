from pyflink.common import Types, Time, WatermarkStrategy
from pyflink.datastream import DataStream
from pyflink.datastream.window import TumblingEventTimeWindows

from .functions import Query1AggregateFunction, Query2AggregateFunction, Query2ProcessWindowFunction, \
    Query2SortingAggregationFunction, Query2SortingProcessWindowFunction
from .key_selectors import CustomKeySelector


def query_1(ds: DataStream, watermark_strategy: WatermarkStrategy, days: int = 1) -> DataStream:
    print("Entering query 1 execution")
    # Takes only required data for each dataset entry (timestamp, vault_id, temperature)
    return ds.map(
        func=lambda i: (i[0], i[4], i[6]),
        output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.FLOAT()])
    ).assign_timestamps_and_watermarks(watermark_strategy) \
        .filter(lambda i: 1000 <= i[1] <= 1020) \
        .key_by(CustomKeySelector()) \
        .window(TumblingEventTimeWindows.of(Time.days(days))) \
        .aggregate(Query1AggregateFunction(),
                   accumulator_type=Types.TUPLE(
                       [Types.INT(), Types.FLOAT(), Types.FLOAT()]),
                   output_type=Types.TUPLE(
                       [Types.INT(), Types.FLOAT(), Types.FLOAT()])
                   )


def query_2(ds: DataStream, watermark_strategy: WatermarkStrategy, days: int) -> DataStream:
    # Takes only required data (timestamp, vault_id, model, serial_number, failure_flag)
    aggregated = ds.map(
        func=lambda i: (i[0], i[4], i[2], i[1], i[3]),
        output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])) \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .filter(lambda i: i[4] == 1) \
        .key_by(CustomKeySelector()) \
        .window(TumblingEventTimeWindows.of(Time.days(days))) \
        .aggregate(
            Query2AggregateFunction(),
            window_function=Query2ProcessWindowFunction(),
            accumulator_type=Types.TUPLE([Types.INT(), Types.LIST(Types.TUPLE([Types.STRING(), Types.STRING()]))]),
            output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.STRING()])
            )
    aggregated.print()
    return aggregated.key_by(lambda i: i[0]) \
        .window(TumblingEventTimeWindows.of(Time.days(days))) \
        .aggregate(
        aggregate_function=Query2SortingAggregationFunction(),
        window_function=Query2SortingProcessWindowFunction(),
        accumulator_type=Types.LIST(Types.TUPLE([Types.STRING(), Types.INT(), Types.STRING()])),
        output_type=Types.TUPLE([Types.STRING(),
                                 Types.INT(), Types.STRING(),
                                 Types.INT(), Types.STRING(),
                                 Types.INT(), Types.STRING(),
                                 Types.INT(), Types.STRING(),
                                 Types.INT(), Types.STRING(),
                                 Types.INT(), Types.STRING(),
                                 Types.INT(), Types.STRING(),
                                 Types.INT(), Types.STRING(),
                                 Types.INT(), Types.STRING(),
                                 Types.INT(), Types.STRING()])
    )

def query_3(ds: DataStream, days: int):
    return ds
