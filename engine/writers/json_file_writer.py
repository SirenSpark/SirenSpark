from pyspark.sql import DataFrame
from model_base import BaseStep
from typing import Dict, Any, Optional
from utils.pandas import toPandas, convertGeomsToText
import json


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

        pandas_df = new_df.fillna(0).toPandas()

        for key in self.types:
            if self.types[key]['data_type'] == 'array_json':
                pandas_df[key] = pandas_df[key].apply(self.parse_json_column)

        json_data = pandas_df.to_json(orient='records')

        with open(self.filepath, 'w') as file:
            file.write(json_data)

        return self.df, self.types, 'success'
    
    def parse_json_column(self, arr_json_str):
        if isinstance(arr_json_str, list):
            for key, value in enumerate(arr_json_str):
                try:
                    arr_json_str[key] = json.loads(value.replace("\\", ""))
                except:
                    pass
        elif isinstance(arr_json_str, str):
            try:
                return json.loads(arr_json_str)
            except:
                pass

        return arr_json_str
