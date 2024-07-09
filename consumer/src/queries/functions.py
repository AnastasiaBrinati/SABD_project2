from typing import Tuple, Iterable, List

from pyflink.common import Types
from pyflink.datastream import AggregateFunction, ProcessWindowFunction, OutputTag
from pyflink.datastream.functions import ProcessAllWindowFunction
from tdigest import TDigest

output_tag = OutputTag("side-output", Types.STRING())


class Query1AggregateFunction(AggregateFunction):

    def create_accumulator(self) -> Tuple[str, int, float, float]:
        return '', 0, 0.0, 0.0

    # For a new value new_value, compute the new count, new mean, the new M2.
    # mean accumulates the mean of the entire dataset
    # M2 aggregates the squared distance from the mean
    # count aggregates the number of samples seen so far
    def add(self, value: Tuple[str, int, float], accumulator: Tuple[str, int, float, float]) -> Tuple[
        str, int, float, float]:
        print(f' Accumulator: {accumulator}')
        (timestamp, count, mean, M2) = accumulator
        if timestamp == '':
            timestamp = value[0]
        else:
            timestamp = min(timestamp, value[0])
        count += 1
        delta = float(value[2]) - mean
        mean += delta / count
        delta2 = float(value[2]) - mean
        M2 += delta * delta2
        return timestamp, count, mean, M2

    # Retrieve count, mean and variance from the aggregate
    def get_result(self, accumulator: Tuple[str, int, float, float]) -> Tuple[str, int, float, float]:
        (timestamp, count, mean, M2) = accumulator
        print(f"result: {count}, {mean}, {M2 / count}")
        return timestamp, count, mean, M2 / count

    # Merges two accumulators
    def merge(self, acc_a: Tuple[str, int, float, float], acc_b: Tuple[str, int, float, float]) -> Tuple[
        str, int, float, float]:
        print("merging")
        return (min(acc_a[0], acc_b[0]), acc_a[1] + acc_b[1],
                (acc_a[2] * acc_a[1] + acc_b[2] * acc_b[1]) / (acc_a[1] + acc_b[1]), (acc_a[3] + acc_b[3]) / 2)


class Query1ProcessWindowFunction(ProcessWindowFunction):

    def process(self, key: int, context: 'ProcessWindowFunction.Context',
                elements: Iterable[Tuple[str, int, float, float]]) -> Iterable[Tuple[str, int, int, float, float]]:
        keyed_elements = []
        windows_timestamps = []
        for e in elements:
            windows_timestamps.append(e[0])
        min_ts = min(windows_timestamps)
        for element in elements:
            keyed_elements.append((min_ts, key, element[1], element[2], element[3]))
        yield from keyed_elements


class Query2AggregateFunction(AggregateFunction):
    """
    Aggregates dataset tuples to sum failures count and track failed hard disks models and serial numbers
    """

    def create_accumulator(self) -> Tuple[int, List[Tuple[str, str]]]:
        """
        Creates an accumulator containing failures count and (model, serial_numer) tuples for each vault
        """
        return 0, list()

    def add(self, value: Tuple[str, int, str, str, int], accumulator: Tuple[int, List[Tuple[str, str]]]) -> Tuple[
        int, List[Tuple[str, str]]]:
        """
        Increments failures count and adds (without repetitions) hard disk model and serial number to the set
        """
        if value[4] == 1:
            accumulator[1].append((value[2], value[3]))
            ret = accumulator[0] + 1, list(set(accumulator[1]))
        else:
            # Do not add element if it has not failed
            ret = accumulator
        print(f'Updated accumulator: {ret} (vault_id : {value[1]})')
        return ret

    def get_result(self, accumulator: Tuple[int, List[Tuple[str, str]]]) -> Tuple[int, str]:
        """
        Retrieve the result from the accumulator, in the form 'failures ([modelA, serialA, ...])'
        """
        count, ms = accumulator[0], "([%s])" % ", ".join(", ".join(mse for mse in ms) for ms in accumulator[1])
        print(f'Results: \t vault_id={count}, models&sn={ms}')
        return count, ms

    def merge(self, acc_a: Tuple[int, List[Tuple[str, str]]], acc_b: Tuple[int, List[Tuple[str, str]]]):
        """
        Merges two accumulator by adding failures counts and merging models and serial numbers sets
        """
        return acc_a[0] + acc_b[0], list(set(acc_a[1] + (acc_b[1])))


class Query2ProcessWindowFunction(ProcessWindowFunction):
    """
    Process a window to evaluate query 2
    """

    def process(self, key: Tuple[str, int], context: 'ProcessWindowFunction.Context',
                elements: Iterable[Tuple[int, str]]) -> Iterable[Tuple[str, int, int, str]]:
        """
        Sorts aggregates elements by failures count, the add the key and returns
        """
        for el in elements:
            yield key[0], key[1], el[0], el[1]


class Query2SortingAggregationFunction(AggregateFunction):
    def create_accumulator(self) -> List[Tuple[str, int, int, str]]:
        return list()

    def add(self, value: Tuple[str, int, int, str], accumulator: List[Tuple[str, int, int, str]]) -> List[
        Tuple[str, int, int, str]]:
        accumulator.append(value)
        return accumulator

    def get_result(self, accumulator: List[Tuple[str, int, int, str]]) -> Tuple[
        str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str]:
        template = ("", 0, "", 0, "", 0, "", 0, "", 0, "", 0, "", 0, "", 0, "", 0, "", 0, "")
        print(f'Getting results: {accumulator}')
        accumulator.sort(key=lambda x: x[2], reverse=True)
        res = []
        for el in accumulator:
            res.append((el[0], el[1], f'{el[2]} {el[3]}'))
        min_ts = min(map(lambda x: x[0], res))
        print(f'Min timestamp: {min_ts}')
        if len(accumulator) > 0:
            return (min_ts, res[0][1], res[0][2], res[1][1] if len(res) > 1 else 0, res[1][2] if len(res) > 1 else "",
                    res[2][1] if len(res) > 2 else 0, res[2][2] if len(res) > 2 else "",
                    res[3][1] if len(res) > 3 else 0, res[3][2] if len(res) > 3 else "",
                    res[4][1] if len(res) > 4 else 0, res[4][2] if len(res) > 4 else "",
                    res[5][1] if len(res) > 5 else 0, res[5][2] if len(res) > 5 else "",
                    res[6][1] if len(res) > 6 else 0, res[6][2] if len(res) > 6 else "",
                    res[7][1] if len(res) > 7 else 0, res[7][2] if len(res) > 7 else "",
                    res[8][1] if len(res) > 8 else 0, res[8][2] if len(res) > 8 else "",
                    res[9][1] if len(res) > 9 else 0, res[9][2] if len(res) > 9 else "")
        else:
            return template

    def merge(self, acc_a: List[Tuple[str, int, int, str]], acc_b: List[Tuple[str, int, int, str]]) -> List[
        Tuple[str, int, int, str]]:
        return acc_a + acc_b


class Query2SortingProcessWindowFunction(ProcessAllWindowFunction):

    def process(self, context: 'ProcessWindowFunction.Context', elements: Iterable[Tuple[
        str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str]]) -> \
    Iterable[
        Tuple[str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str]]:
        yield from elements


class TDigestAggregateFunction(AggregateFunction):
    def create_accumulator(self) -> Tuple[str, int, int, object, int]:
        """
        Creates a list accumulator composed by:
        - the minimum (int)
        - the maximum (int)
        - the TDigest object
        - the records count
        """
        return '', 0, 0, TDigest(), 0

    def add(self, value: Tuple[str, int, str, int], accumulator: Tuple[str, int, int, object, int]) -> Tuple[
        str, int, int, object, int]:
        """
        Updates current TDigest and return min, max and TDigest object

        New digest creation is due to casting-related issues
        """
        new_digest = TDigest() + accumulator[3]
        new_digest.update(value[3])
        min_ts = value[0] if accumulator[0] == '' else min(value[0], accumulator[0])
        min_hours = value[3] if accumulator[1] == 0 else min(value[3], accumulator[1])
        return min_ts, min_hours, max(value[3], accumulator[2]), new_digest, accumulator[4] + 1

    def get_result(self, accumulator: Tuple[str, int, int, object, int]) -> Tuple[
        str, int, float, float, float, int, int]:
        print(f'results from accumulators: \t {accumulator}')
        new_digest = TDigest() + accumulator[3]
        return (accumulator[0], accumulator[1], new_digest.percentile(25), new_digest.percentile(50),
                new_digest.percentile(75), accumulator[2], accumulator[4])

    def merge(self, acc_a: Tuple[str, int, int, object, int], acc_b: Tuple[str, int, int, object, int]) -> Tuple[
        str, int, int, object, int]:
        new_digest = TDigest() + acc_a[3] + acc_b[3]
        return min(acc_a[0], acc_b[0]), min(acc_a[1], acc_b[1]), max(acc_a[2], acc_b[2]), new_digest, acc_a[4] + acc_b[
            4]


class TDigestProcessWindowFunction(ProcessWindowFunction):
    def process(self, key: int, context: 'ProcessWindowFunction.Context',
                elements: Iterable[Tuple[str, int, float, float, float, int, int]]) -> Iterable[
        Tuple[str, int, int, float, float, float, int, int]]:
        keyed_list = []
        for e in elements:
            keyed_list.append((e[0], key, e[1], e[2], e[3], e[4], e[5], e[6]))
        yield from keyed_list
