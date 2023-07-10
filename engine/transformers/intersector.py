""" 
Intersector
Effectue une intersection entre deux jeux de données
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import expr


class Intersector:
    def __init__(self, table1_df, table2_df, geomattr1='geom', geomattr2='geom', attr_mode='merge', prefix='supplier_'):
        self.table1_df = table1_df
        self.geomattr1 = geomattr1
        self.table2_df = table2_df
        self.geomattr2 = geomattr2
        self.attr_mode = attr_mode
        self.prefix = prefix

    def run(self):
        # start Spark session
        spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

        # handle column name conflicts
        if self.attr_mode == 'prefix':
            # prefix all columns in table2_df with "table2_"
            for col_name in self.table2_df.columns:
                self.table2_df = self.table2_df.withColumnRenamed(
                    col_name, self.prefix + col_name)
            self.geomattr2 = self.prefix + self.geomattr2

        # create temporary views for tables
        self.table1_df.createOrReplaceTempView("v_table1")
        self.table2_df.createOrReplaceTempView("v_table2")

        # build query to intersect tables
        query = f"SELECT * FROM v_table1, v_table2 WHERE ST_Contains(ST_GeomFromWKB(v_table2.{self.geomattr2}), ST_GeomFromWKB(v_table1.{self.geomattr1}))"

        # execute query and get result DataFrame
        intersect_df = spark.sql(query)

        # if attribute mode is "merge", remove columns from table2_df that have the same name as columns in table1_df
        if self.attr_mode == 'merge':
            intersect_df = intersect_df.select(
                *[col("v_table1." + col_name) for col_name in self.table1_df.columns] +
                [col("v_table2." + col_name)
                 for col_name in self.table2_df.columns if col_name not in self.table1_df.columns]
            )

        return intersect_df
