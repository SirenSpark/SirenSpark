"""
ArrayJoin
Effectue une jointure pour relations 1-n
"""
from model_base import BaseStep
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, array_join, to_json
from pyspark.sql.types import StructType, StructField, StringType
import json


class ArrayJoinerStep(BaseStep):
    type = "ArrayJoiner"
    options: Dict[str, Any] = {
        'left_join_column': str,
        'right_join_column': str,
        'new_column_name': str
    }


class ArrayJoiner:
    def __init__(self, left_df, right_df, left_types, right_types, left_join_column, right_join_column, new_column_name):
        self.left_df = left_df
        self.left_types = left_types
        self.right_df = right_df
        self.right_types = right_types
        self.left_join_column = left_join_column
        self.right_join_column = right_join_column
        self.new_column_name = new_column_name

    def run(self):
        # start Spark session
        spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

        self.right_df.createOrReplaceTempView("v_right")

        output_data = []
        data_collect = self.left_df.collect()
        for row in data_collect:
            row = row.asDict()
            query = f"SELECT * from v_right where {self.right_join_column} = '{row[self.left_join_column]}'"
            row[self.new_column_name] = spark.sql(query).toJSON().collect()
            output_data.append(row)

        schema = StructType(self.left_df.schema.fields +
                            [StructField(self.new_column_name, StringType(), True)])

        newdf = spark.createDataFrame(output_data, schema)

        types = {**self.left_types, **{self.new_column_name: {
            "data_type": "json",
            "pg_data_type": "text",
        }}}

        return newdf, types, 'success'
