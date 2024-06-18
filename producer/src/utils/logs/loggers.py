import json
from typing import TypedDict
from loguru import logger


class Logger:
    def __init__(self, logger_name: str):
        self._logger_name = logger_name

    def log(self, data: str):
        logger.info(f"{self._logger_name} : {data}")


class AppLogger(Logger):
    """Generic Logger"""

    def __init__(self):
        # Add the missing argument "logger_name"
        super().__init__(logger_name="AppLogger\n")

    def config_loaded(self, hdfs_config, spark_config, b2_config, nifi_config, redis_config):
        self.log(
            f"{json.dumps(hdfs_config)}, {json.dumps(spark_config)}, {json.dumps(b2_config)}, {json.dumps(nifi_config)}, {json.dumps(redis_config)} loaded successfully..")


class SparkLogger(Logger):
    """Logger for Spark"""

    def __init__(self):
        super().__init__(logger_name="SparkLogger\n")

    def spark_api_loaded(self):
        self.log("Spark API loaded successfully..")


class B2Logger(Logger):
    """Logger for B2"""

    def __init__(self):
        super().__init__(logger_name="B2Logger\n")

    def b2_api_loaded(self):
        self.log.info("B2 Authentication SUCCESS, API loaded..")

    def b2_bucket_loaded(self):
        self.log("Bucket Loaded successfully..")

    def downloading_file(self,):
        self.log(f"Downloading file in memory..")

    def decrypting_file(self):
        self.log("Decrypting file..")

    def file_decrypted(self):
        self.log("File decrypted..")


class NifiLogger(Logger):
    """Logger for Nifi"""

    def __init__(self):
        super().__init__(logger_name="NifiLogger\n")

    def nifi_connection_success(self):
        self.log("Nifi connection has ben estabilished successfully..")

    def nifi_login_success(self):
        self.log("Nifi login successful..")
