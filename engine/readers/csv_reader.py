from pyspark.sql import SparkSession
import logging
from typing import Dict, Any, Optional
from model_base import BaseStep

class CSVReaderStep(BaseStep):
    type = "CSVReader"
    options: Dict[str, Any] = {
        "filepath": str,
        "header": Optional[bool],  # Whether the CSV file has a header row with column names
        "delimiter": Optional[str]  # The delimiter character used in the CSV file (default is ",")
    }

class CSVReader:
    def __init__(self, filepath, header=True, delimiter=","):
        self.filepath = filepath
        self.header = header
        self.delimiter = delimiter

    def run(self):
        spark = SparkSession.builder.appName("SirenSpark").getOrCreate()
        try:
            df = spark.read.format("csv") \
                .option("header", self.header) \
                .option("delimiter", self.delimiter) \
                .load(self.filepath)
            column_types = {col: str(dtype) for col, dtype in df.dtypes}
            return df, column_types, "success"
        except Exception as e:
            logging.error(
                f"Error reading to CSV file {e}")
            return False, False, 'error'
