from typing import Tuple

from pyflink.common import Types, Time
from pyflink.datastream import DataStream, AggregateFunction
from pyflink.datastream.window import TumblingEventTimeWindows


def query_1(ds: DataStream, window_size: Time):
    # Takes only required data for each dataset entry (timestamp, vault_id, temperature)
    ds.map(lambda i: (i[0], i[4], i[25]), output_type=Types.TUPLE(
        [Types.INT(), Types.DOUBLE(), Types.TUPLE([Types.INT(), Types.DOUBLE(), Types.DOUBLE()])]))
    # Filter entries by vault_id, then takes a windows of size :window_size and applies the aggregate function
    ds.filter(lambda i: 1000 <= i[0] <= 1200).key_by(
        lambda i: i[0]
    ).window(
        TumblingEventTimeWindows.of(window_size)
    ).aggregate(
        Query1AggregateFunction(),
        accumulator_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT()]),
        output_type=Types.TUPLE([Types.INT(), Types.FLOAT(), Types.FLOAT()])
    )
    return ds


class Query1AggregateFunction(AggregateFunction):

    def create_accumulator(self) -> Tuple[int, float, float]:
        return 0, 0.0, 0.0

    # For a new value new_value, compute the new count, new mean, the new M2.
    # mean accumulates the mean of the entire dataset
    # M2 aggregates the squared distance from the mean
    # count aggregates the number of samples seen so far
    def add(self, value: Tuple[str, int, float], accumulator: Tuple[int, float, float]):
        (count, mean, M2) = accumulator
        count += 1
        delta = value[2] - mean
        mean += delta / count
        delta2 = value[2] - mean
        M2 += delta * delta2
        return count, mean, M2

    # Retrieve count, mean and variance from the aggregate
    def get_result(self, accumulator: Tuple[int, float, float]) -> Tuple[int, float, float]:
        (count, mean, M2) = accumulator
        if count < 2:
            return int("nan"), float("nan"), float("nan")
        else:
            # (mean, variance, sample_variance) = (mean, M2 / count, M2 / (count - 1))
            return count, mean, M2/count

    # Merges two accumulators (not used)
    def merge(self, acc_a, acc_b):
        return


"""def query_2(ds: DataStream):
    # Takes only required data (timestamp, vault_id, model, serial_number, failure_flag)
    ds.map(lambda i: (i[0], i[4], i[1], i[2], i[3]), output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.INT()]))
    ds.key_by(lambda i: i[0]).key_by(lambda i: i[1]).reduce(lambda i, acc: (i[0], i[1], i[2], i[3], i[4] + acc[4]))"""


