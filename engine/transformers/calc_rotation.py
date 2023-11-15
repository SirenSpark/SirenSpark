"""
CalcRotation
Ajoute une colonne au DataFrame source avec la géométrie du point le plus proche du DataFrame cible
"""
from model_base import BaseStep
from typing import Dict, Any, Optional
from shapely.wkt import loads
from utils.pandas import toPandas, convertGeomsToText, convertGeomsToBinary
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, NumericType, IntegerType
import math


class CalcRotationStep(BaseStep):
    type = "CalcRotation"
    options: Dict[str, Any] = {
        "geom_column_1": str,
        "geom_column_2": str,
        "new_column_name": str,
    }


class CalcRotation:
    def __init__(self, df, types, geom_column_1='geom', geom_column_2='geom', new_column_name='rotation', properties=None):
        self.df = df
        self.types = types
        self.geom_column_1 = geom_column_1
        self.geom_column_2 = geom_column_2
        self.new_column_name = new_column_name
        self.properties = properties

    def run(self):

        new_df = convertGeomsToText(self.df, self.types)
        pandas_df = new_df.fillna(0).toPandas()

        angles = []

        for index, row in pandas_df.iterrows():
            geom_1 = loads(row[self.geom_column_1])
            geom_2 = loads(row[self.geom_column_2])

            # Assuming you are working with 2D geometries, you can use the angle formula
            angle = math.degrees(math.atan2(
                geom_2.y - geom_1.y, geom_2.x - geom_1.x))

            angles.append(angle)

        pandas_df[self.new_column_name] = angles

        pandas_df = pandas_df[new_df.columns + [self.new_column_name]]


        # Transformation du df pandas en df spark
        spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

        schema = StructType(
            new_df.schema.fields + [StructField(self.new_column_name, StringType(), True)])

        spark_df = spark.createDataFrame(pandas_df, schema)

        spark_df = convertGeomsToBinary(spark_df, self.types)

        types = {**self.types, **{self.new_column_name: {
            "data_type": "number",
            "pg_data_type": "number",
        }}}

        return spark_df, types, 'success'
