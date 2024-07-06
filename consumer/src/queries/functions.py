from typing import Tuple, Iterable, Set

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
    def add(self,
            value: Tuple[str, int, float],
            accumulator: Tuple[int, float, float]) -> Tuple[int, float, float]:
        print(value)
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
        return acc_a[0] + acc_b[0], (acc_a[1] + acc_b[1])/2, (acc_a[2] + acc_b[2])/2


class Query1ProcessWindowFunction(ProcessWindowFunction):

    def process(self,
                key: Types.INT(),
                context: 'ProcessWindowFunction.Context',
                elements: Iterable[Tuple[int, float, float]]) -> Iterable[Tuple[int, float, float]]:
        yield from elements

        yield output_tag, str(elements)


class Query2AggregateFunction(AggregateFunction):
    """
    Aggregates dataset tuples to sum failures count and track failed hard disks models and serial numbers
    """

    def create_accumulator(self) -> Tuple[int, Set[Tuple[str, str]]]:
        """
        Creates an accumulator containing failures count and (model, serial_numer) tuples for each vault
        """
        return 0, set()

    def add(self,
            value: Tuple[str, int, str, str, int],
            accumulator: Tuple[int, Set[Tuple[str, str]]]) -> Tuple[int, Set[Tuple[str, str]]]:
        """
        Increments failures count and adds (without repetitions) hard disk model and serial number to the set
        """
        if value[4] == 1:
            return accumulator[0] + 1, set(list(accumulator[1]) + [(value[2], value[3])])
        else:
            # Do not add element if it has not failed
            return accumulator

    def get_result(self, accumulator: Tuple[int, Set[Tuple[str, str]]]) -> Tuple[int, str]:
        """
        Retrieve the result from the accumulator, in the form 'failures ([modelA, serialA, ...])'
        """
        return accumulator[0], "([%s])" % ", ".join(", ".join(mse for mse in ms) for ms in accumulator[1])

    def merge(self, acc_a: Tuple[int, Set[Tuple[str, str]]], acc_b: Tuple[int, Set[Tuple[str, str]]]):
        """
        Merges two accumulator by adding failures counts and merging models and serial numbers sets
        """
        return acc_a[0] + acc_b[0], acc_a[1].union(acc_b[1])


class Query2ProcessWindowFunction(ProcessWindowFunction):
    """
    Process a window to evaluate query 2
    """

    def process(self,
                key: Tuple[str, int],
                context: 'ProcessWindowFunction.Context',
                elements: Iterable[Tuple[int, str]]) -> Iterable[Tuple[str, int, str]]:
        """
        Sorts aggregates elements by failures count, the add the key and returns
        """
        sorted_elements = sorted(elements, key=lambda x: x[0])[:10]
        keyed_sorted_list = []
        for sorted_element in sorted_elements:
            keyed_sorted_list.append((key[0], key[1], "%d %s" % (sorted_element[0], sorted_element[1])))
        yield keyed_sorted_list
