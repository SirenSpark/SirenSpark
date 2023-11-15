"""
ClosestPoint
Ajoute une colonne au DataFrame source avec la géométrie du point le plus proche du DataFrame cible
"""
from model_base import BaseStep
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
import json


class ClosestPointStep(BaseStep):
    type = "ClosestPoint"
    options: Dict[str, Any] = {
        "left_geom_column": str,
        "right_geom_column": str,
        "new_column_name": str
    }


class ClosestPoint:
    def __init__(self, left_df, right_df, left_types, right_types, left_geom_column='geom', right_geom_column='geom', new_column_name='closest_point'):
        self.left_df = left_df
        self.left_types = left_types
        self.left_geom_column = left_geom_column
        self.right_df = right_df
        self.right_types = right_types
        self.right_geom_column = right_geom_column
        self.new_column_name = new_column_name

    def run(self):
        # start Spark session
        spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

        # create temporary views for tables
        self.left_df.createOrReplaceTempView("v_left")
        self.right_df.createOrReplaceTempView("v_right")

        # Create a union from right geoms
        merged_geometry_df = spark.sql(
            f"SELECT ST_Union_Aggr(ST_GeomFromWKB({self.right_geom_column})) AS merged_geometry FROM v_right")

        merged_geometry_df.createOrReplaceTempView("v_merged")

        # closest_point_df = spark.sql(
        # f"""
        #     SELECT
        #     v_left.*,
        #     ST_AsText(ST_GeomFromWKB(v_left.{self.left_geom_column})) as geom2,
        #     ST_AsText(ST_ClosestPoint(v_merged.merged_geometry, ST_GeomFromWKB(v_left.{self.left_geom_column}))) as closest_point,
        #     ST_Azimuth(ST_GeomFromWKB(v_left.{self.left_geom_column}), ST_ClosestPoint(v_merged.merged_geometry, ST_GeomFromWKB(v_left.{self.left_geom_column}))) as rotation
        #     FROM v_left, v_merged
        # """)

        closest_point_df = spark.sql(
            f"""
            SELECT 
            v_left.*,
            ST_AsEWKB(ST_ClosestPoint(v_merged.merged_geometry, ST_GeomFromWKB(v_left.{self.left_geom_column}))) as {self.new_column_name}
            FROM v_left, v_merged
        """)

        types = {**self.left_types, **{self.new_column_name: {
            "data_type": "geometry",
            "pg_data_type": "geometry",
            "type": "POINT",
            "srid": self.left_types[self.left_geom_column]['srid'],
            "coord_dimension": 2
        }}}

        return closest_point_df, types, 'success'
