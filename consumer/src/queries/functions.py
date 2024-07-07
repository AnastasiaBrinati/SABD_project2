from typing import Tuple, Iterable, List

from pyflink.common import Types
from pyflink.datastream import AggregateFunction, ProcessWindowFunction, OutputTag

output_tag = OutputTag("side-output", Types.STRING())


class Query1AggregateFunction(AggregateFunction):

    def create_accumulator(self) -> Tuple[int, float, float]:
        print("creating accumulator")
        return 0, 0.0, 0.0

    # For a new value new_value, compute the new count, new mean, the new M2.
    # mean accumulates the mean of the entire dataset
    # M2 aggregates the squared distance from the mean
    # count aggregates the number of samples seen so far
    def add(self, value: Tuple[str, int, float], accumulator: Tuple[int, float, float]) -> Tuple[int, float, float]:
        print(f"value: {value}")
        (count, mean, M2) = accumulator
        count += 1
        delta = float(value[2]) - mean
        mean += delta / count
        delta2 = float(value[2]) - mean
        M2 += delta * delta2
        return count, mean, M2

    # Retrieve count, mean and variance from the aggregate
    def get_result(self, accumulator: Tuple[int, float, float]) -> Tuple[int, float, float]:
        print("getting result")
        (count, mean, M2) = accumulator
        print(f"{count}, {mean}, {M2 / count}")
        return count, mean, M2 / count

    # Merges two accumulators
    def merge(self, acc_a: Tuple[int, float, float], acc_b: Tuple[int, float, float]) -> Tuple[int, float, float]:
        print("merging")
        return (acc_a[0] + acc_b[0], (acc_a[1] * acc_a[0] + acc_b[1] * acc_b[0]) / (acc_a[0] + acc_b[0]),
                (acc_a[2] + acc_b[2]) / 2)


class Query1ProcessWindowFunction(ProcessWindowFunction):

    def process(self, key: Tuple[str, int], context: 'ProcessWindowFunction.Context',
                elements: Iterable[Tuple[int, float, float]]) -> Iterable[Tuple[int, float, float]]:
        keyed_sorted_list = []
        for element in elements:
            keyed_sorted_list.append((key[0], key[1], element[0], element[1], element[2]))
        yield from keyed_sorted_list


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
                elements: Iterable[Tuple[int, str]]) -> Iterable[Tuple[str, int, str]]:
        """
        Sorts aggregates elements by failures count, the add the key and returns
        """
        """print(f'Processing window - {elements}')
        keyed_sorted_list = []
        if sum(1 for _ in elements) > 0:
            sorted_elements = sorted(elements, key=lambda x: x[0])[:10]
            for sorted_element in sorted_elements:
                keyed_sorted_list.append((key[0], key[1], "%d %s" % (sorted_element[0], sorted_element[1])))
        else:
            keyed_sorted_list.append(
                (key[0], key[1], f'Non sono occorsi fallimenti nel vault {key[1]} nel giorno {key[0]}'))
        yield from keyed_sorted_list"""
        for el in elements:
            yield key[0], key[1], "%d %s" % (el[0], el[1])


class Query2SortingAggregationFunction(AggregateFunction):
    def create_accumulator(self) -> List[Tuple[str, int, str]]:
        return list()

    def add(self, value: Tuple[str, int, str], accumulator: List[Tuple[str, int, str]]) -> List[Tuple[str, int, str]]:
        accumulator.append(value)
        return accumulator

    def get_result(self, accumulator: List[Tuple[str, int, str]]) -> Tuple[
        str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str]:
        template = ("", 0, "", 0, "", 0, "", 0, "", 0, "", 0, "", 0, "", 0, "", 0, "", 0, "")
        accumulator.sort(key=lambda x: x[1], reverse=True)
        if len(accumulator) > 0:
            return (
                accumulator[0][0],
                accumulator[0][1], accumulator[0][2], accumulator[1][1] if len(accumulator) > 1 else 0,
                accumulator[1][2] if len(accumulator) > 1 else "", accumulator[2][1] if len(accumulator) > 2 else 0,
                accumulator[2][2] if len(accumulator) > 2 else "", accumulator[3][1] if len(accumulator) > 3 else 0,
                accumulator[3][2] if len(accumulator) > 3 else "", accumulator[4][1] if len(accumulator) > 4 else 0,
                accumulator[4][2] if len(accumulator) > 4 else "", accumulator[5][1] if len(accumulator) > 5 else 0,
                accumulator[5][2] if len(accumulator) > 5 else "", accumulator[6][1] if len(accumulator) > 6 else 0,
                accumulator[6][2] if len(accumulator) > 6 else "", accumulator[7][1] if len(accumulator) > 7 else 0,
                accumulator[7][2] if len(accumulator) > 7 else "", accumulator[8][1] if len(accumulator) > 8 else 0,
                accumulator[8][2] if len(accumulator) > 8 else "", accumulator[9][1] if len(accumulator) > 9 else 0,
                accumulator[9][2] if len(accumulator) > 9 else "")
        else:
            return template

    def merge(self, acc_a: List[Tuple[str, int, str]], acc_b: List[Tuple[str, int, str]]) -> List[Tuple[str, int, str]]:
        return acc_a + acc_b


class Query2SortingProcessWindowFunction(ProcessWindowFunction):
    def process(self, key: str, context: 'ProcessWindowFunction.Context', elements: Iterable[Tuple[
        str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str]]) -> \
        Iterable[
        Tuple[str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str, int, str]
    ]:
        yield from elements
