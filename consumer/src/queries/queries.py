from pyflink.common import Types, Time, WatermarkStrategy
from pyflink.datastream import DataStream
from pyflink.datastream.window import TumblingEventTimeWindows

from .functions import Query1AggregateFunction, Query2AggregateFunction, Query2ProcessWindowFunction, \
    Query2SortingAggregationFunction, Query2SortingProcessWindowFunction, TDigestAggregateFunction, \
    TDigestProcessWindowFunction
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
                       [Types.STRING(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()])
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


def query_3(ds: DataStream, watermark_strategy: WatermarkStrategy, days: int):
    """
    Maps ds to (timestamp, vault_id, serial_number, hours) tuples. Then, key by serial number
    and gets the total number of power-on hours (it a cumulative data, so max() is
    used to get the total).
    """
    hd_hours = ds.assign_timestamps_and_watermarks(watermark_strategy) \
        .map(lambda i: (i[0], i[4], i[1], i[5])) \
        .filter(lambda i: 1090 <= i[1] <= 1120) \
        .key_by(lambda i: i[2]) \
        .window(TumblingEventTimeWindows.of(Time.days(days))) \
        .reduce(lambda x, acc: (x[0] if x[0] < acc[0] else acc[0], x[1], x[2], max(x[3], acc[3])))

    """
    Now, keys by vault_id and aggregates values with t-digest method.
    """
    # Key by vault_id
    return hd_hours.key_by(lambda i: i[1]) \
        .window(TumblingEventTimeWindows.of(Time.days(days))) \
        .aggregate(
            TDigestAggregateFunction(),
            window_function=TDigestProcessWindowFunction(),
            # accumulator_type=Types.TUPLE([Types.INT(), Types.INT(), Types.])
            output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.INT(), Types.INT()])
        )
