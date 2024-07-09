from pyflink.common import Row
from pyflink.datastream import MapFunction
from datetime import datetime


class TimeMap(MapFunction):

    def __init__(self):
        self.start_time = datetime.now().second
        self.elapsed_time = self.start_time
        self.count = 0

    def map(self, value):
        self.count += 1
        self.elapsed_time = datetime.now().second - self.start_time

        thr = self.count / self.elapsed_time

        # Returns throughput and latency
        return thr, 1/thr


def query1_output_map_function(i):
    return Row(i[0], i[1], i[2], i[3], i[4])


def query2_output_map_function(i):
    return Row(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10],
               i[11], i[12], i[13], i[14], i[15], i[16], i[17], i[18], i[19], i[20])


def query3_output_map_function(i):
    return Row(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7])
