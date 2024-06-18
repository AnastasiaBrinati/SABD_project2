from __future__ import annotations

import json
import os
from typing import TypedDict


DEFAULT_CONFIG_PATH = os.getenv(
    'CONFIG_PATH', 'config/default.json')


class HdfsConfig(TypedDict):
    host: str
    port: int
    datasetPath: str
    resultsPath: str


class SparkConfig(TypedDict):
    master: str
    appName: str
    port: int


class B2Config(TypedDict):
    bucketName: str
    fileName: str


class NifiConfig(TypedDict):
    host: str
    port: int


class RedisConfig(TypedDict):
    host: str
    port: int
    db: int


class Config:
    """Configuration of the application."""

    def __init__(self, hdfs: HdfsConfig, spark: SparkConfig, b2: B2Config, nifi: NifiConfig, redis: RedisConfig) -> None:
        self._hdfs = hdfs
        self._spark = spark
        self._b2 = b2
        self._nifi = nifi
        self._redis = redis

    @staticmethod
    def from_default_config() -> Config:
        """Utility to load the default configuration from a JSON file."""
        with open(DEFAULT_CONFIG_PATH, 'r') as file:
            config_data = json.load(file)

        return Config(hdfs=None, spark=None, b2=None, nifi=config_data['nifi'], redis=None)

    @property
    def hdfs_host(self) -> str:
        return self._hdfs['host']

    @property
    def hdfs_port(self) -> int:
        return self._hdfs['port']

    @property
    def spark_master(self) -> str:
        return self._spark['master']

    @property
    def spark_app_name(self) -> str:
        return self._spark['appName']

    @property
    def spark_port(self) -> int:
        return self._spark['port']

    @property
    def b2_bucket_name(self) -> str:
        return self._b2['bucketName']

    @property
    def b2_file_name(self) -> str:
        return self._b2['fileName']

    @property
    def nifi_endpoint(self) -> str:
        return "https://" + self._nifi['host'] + ":" + str(self._nifi['port'])

    @property
    def hdfs_dataset_dir(self) -> str:
        return self._hdfs['datasetPath']

    @property
    def hdfs_url(self) -> str:
        return "hdfs://" + self.hdfs_host + ":" + str(self.hdfs_port)

    @property
    def hdfs_dataset_dir_url(self) -> str:
        return self.hdfs_url + self._hdfs['datasetPath']

    @property
    def hdfs_results_dir_url(self) -> str:
        return self.hdfs_url + self._hdfs['resultsPath']

    @property
    def redis_host(self) -> str:
        return self._redis['host']

    @property
    def redis_port(self) -> int:
        return self._redis['port']

    @property
    def redis_db(self) -> int:
        return self._redis['db']
