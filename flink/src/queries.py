from pyflink.common import Types
from pyflink.datastream import DataStream


def query_1(ds: DataStream):
    ds.map(lambda i: (i[1], i[25], (0, 0, 0,)), output_type=Types.TUPLE(
        [Types.INT(), Types.DOUBLE(), Types.TUPLE([Types.INT(), Types.DOUBLE(), Types.DOUBLE()])]))
    ds.filter(lambda i: 1000 <= i[0] <= 1200)
    ds.key_by(lambda i: i[0]).reduce(lambda i, j: (i[0], j[1], update(i[2], i[1])))
    ds.key_by(lambda i: i[0]).reduce(lambda i: (i[0], i[1], finalize(update(i[2], i[1]))))
    return ds


# For a new value new_value, compute the new count, new mean, the new M2.
# mean accumulates the mean of the entire dataset
# M2 aggregates the squared distance from the mean
# count aggregates the number of samples seen so far
def update(existing_aggregate, new_value):
    (count, mean, M2) = existing_aggregate
    count += 1
    delta = new_value - mean
    mean += delta / count
    delta2 = new_value - mean
    M2 += delta * delta2
    return count, mean, M2


# Retrieve the mean, variance and sample variance from an aggregate
def finalize(existing_aggregate):
    (count, mean, M2) = existing_aggregate
    if count < 2:
        return float("nan")
    else:
        (mean, variance, sample_variance) = (mean, M2 / count, M2 / (count - 1))
        return mean, variance, sample_variance
