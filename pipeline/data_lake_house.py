"""
----------------------------------------------------
----------------- Test Data LakeHouse --------------
----------------------------------------------------
# define the config builder
initialize the config file
"""

from config.builder_config import config
from pyspark.sql import SparkSession
from typing import Dict

class DataLakehouse:

    def __init__(self, config: Dict[str, str]):
        pass

    def _create_spark_session(config: Dict[str, str], app_name: str):
        builder = SparkSession.builder.appName(app_name)

        if config:
            for key, val in config:
                builder =builder.config(key, val)
            