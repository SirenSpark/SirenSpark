from pyspark.sql import DataFrame
from model_base import BaseStep
from typing import Dict, Any, Optional
from utils.pandas import toPandas, convertGeomsToText


class JSONFileWriterStep(BaseStep):
    type = "JSONFileWriter"
    options: Dict[str, Any] = {
        "filepath": str
    }


class JSONFileWriter:
    def __init__(self, df: DataFrame, types, filepath: str):
        self.df = df
        self.types = types
        self.filepath = filepath

    def run(self):

        new_df = convertGeomsToText(self.df, self.types)

        pandas_df = new_df.toPandas()
        json_data = pandas_df.to_json(orient='records')

        with open(self.filepath, 'w') as file:
            file.write(json_data)

        return self.df, self.types, 'success'
