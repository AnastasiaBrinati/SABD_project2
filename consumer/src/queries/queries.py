from datetime import datetime

from pyflink.common import Types, Time, WatermarkStrategy
from pyflink.datastream import DataStream
from pyflink.datastream.window import TumblingEventTimeWindows

from .functions import Query1AggregateFunction, Query2AggregateFunction, Query2ProcessWindowFunction, \
    Query2SortingAggregationFunction, Query2SortingProcessWindowFunction, TDigestAggregateFunction, \
    TDigestProcessWindowFunction, Query1ProcessWindowFunction
from .key_selectors import CustomKeySelector


def query_1(ds: DataStream, watermark_strategy: WatermarkStrategy, days: int) -> DataStream:
    # Takes only required data for each dataset entry (timestamp, vault_id, temperature)
    res = ds.map(
        func=lambda i: (i[0], i[4], i[6]),
        output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.FLOAT()])
    ).assign_timestamps_and_watermarks(watermark_strategy) \
        .filter(lambda i: 1000 <= i[1] <= 1020 and float(i[2]) != 0) \
        .key_by(lambda i: i[1]) \
        .window(TumblingEventTimeWindows.of(Time.days(days))) \
        .aggregate(Query1AggregateFunction(),
                   window_function=Query1ProcessWindowFunction(),
                   accumulator_type=Types.TUPLE(
                       [Types.STRING(), Types.INT(), Types.FLOAT(), Types.FLOAT()]),
                   output_type=Types.TUPLE(
                       [Types.STRING(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT()])
                   )
    res.print()
    return res

def query_2(ds: DataStream, watermark_strategy: WatermarkStrategy, days: int) -> DataStream:
    """
    Takes only required data (timestamp, vault_id, model, serial_number, failure_flag). Then, takes
    only tuples with failure_flag=1 and assigns timestamp to each one. Then, uses a key composed by
    the (timestamp, vault_id) couple and takes an event-time based windows with size :days.
    Lastly,
    """
    ds.map(
        func=lambda i: (i[0], i[4], i[2], i[1], i[3]),
        output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])) \
        .filter(lambda i: i[4] == 1) \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(CustomKeySelector()).print()

    aggregated = ds.map(
        func=lambda i: (i[0], i[4], i[2], i[1], i[3]),
        output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT()])) \
        .filter(lambda i: i[4] == 1) \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(CustomKeySelector()) \
        .window(TumblingEventTimeWindows.of(Time.days(days))) \
        .aggregate(
            Query2AggregateFunction(),
            window_function=Query2ProcessWindowFunction(),
            accumulator_type=Types.TUPLE([Types.INT(), Types.LIST(Types.TUPLE([Types.STRING(), Types.STRING()]))]),
            output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.STRING()])
        )

    print("Printing aggregated")
    aggregated.print()

    sorted = aggregated.window_all(TumblingEventTimeWindows.of(Time.days(days))) \
        .aggregate(
        aggregate_function=Query2SortingAggregationFunction(),
        window_function=Query2SortingProcessWindowFunction(),
        accumulator_type=Types.LIST(Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.STRING()])),
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

    sorted.print()
    return sorted


def query_3(ds: DataStream, watermark_strategy: WatermarkStrategy, days: int) -> DataStream:
    """
    Maps ds to (timestamp, vault_id, serial_number, hours) tuples. Then, key by serial number
    and gets the total number of power-on hours (it a cumulative data, so max() is
    used to get the total).
    """
    hd_hours = ds.map(lambda i: (i[0], i[4], i[1], i[5])) \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .filter(lambda i: 1090 <= i[1] <= 1120) \
        .key_by(lambda i: (i[1], i[2])) \
        .window(TumblingEventTimeWindows.of(Time.days(days))) \
        .reduce(lambda x, acc: (x[0] if x[0] < acc[0] else acc[0], x[1], x[2], max(x[3], acc[3])))

    """
    Now, keys by vault_id and aggregates values with t-digest method. Unlike the other queries,
    the accumulator data type is inferred at runtime
    """
    res = hd_hours.key_by(lambda i: i[1]) \
        .window(TumblingEventTimeWindows.of(Time.days(days))) \
        .aggregate(
            TDigestAggregateFunction(),
            window_function=TDigestProcessWindowFunction(),
            output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.INT(), Types.INT()])
        )

    return res
