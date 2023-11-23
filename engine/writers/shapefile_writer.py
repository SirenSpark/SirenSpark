from pyspark.sql import DataFrame, SparkSession
from model_base import BaseStep
from typing import Dict, Any
from utils.pandas import toPandas


class ShapefileWriterStep(BaseStep):
    type = "ShapefileWriter"
    options: Dict[str, Any] = {
        "filepath": str
    }


class ShapefileWriter:
    def __init__(self, df: DataFrame, types: Dict[str, Any], filepath: str):
        self.df = df
        self.types = types
        self.filepath = filepath
        # start Spark session
        self.spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

    def run(self):

        pandas_df = toPandas(self.df, self.types)
        pandas_df.to_file(self.filepath)

        return self.df, self.types, "success"