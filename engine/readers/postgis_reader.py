""" 
PostgisReader
Effectue une lecture sur une table postgis
"""
import logging
from pyspark.sql import SparkSession
from utils.postgis import get_table_columns, cast_df_by_table_columns, get_column_types
from utils.evaluator import cast_columns
from typing import Dict, Any, Optional
from model_base import BaseStep

class PostgisReaderStep(BaseStep):
    type = "PostgisReader"
    options: Dict[str, Any] = {
        "db_host": str,
        "db_port": str,
        "db_database": str,
        "db_user": str,
        "db_password": str,
        "table_name": str,
        "table_limit": Optional[int],
        "table_filter": Optional[str]
    }


class PostgisReader:
    def __init__(self, db_host, db_port, db_database, db_user, db_password, table_name, table_filter=None, table_limit=None):
        self.db_host = db_host
        self.db_port = db_port
        self.db_database = db_database
        self.db_user = db_user
        self.db_password = db_password
        self.table_name = table_name
        self.table_filter = table_filter
        self.table_limit = table_limit
        # start Spark session
        self.spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

    def get_columns(self, table_columns):
        columns = []
        for row in table_columns:
            if row['udt_name'] == 'geometry':
                query = f"ST_AsEWKB(ST_SetSRID({row['column_name']}, {row['srid']})) as {row['column_name']}"
                columns.append(query)
            else:
                columns.append(row['column_name'])

        return columns

    def run(self):
        # Query Postgres to get the columns
        table_columns = table_columns = get_table_columns(
            self.db_host, self.db_port, self.db_database, self.db_user, self.db_password, self.table_name)

        # Column types
        column_types = get_column_types(table_columns)

        # Request columns
        columns = self.get_columns(table_columns)

        # load table as a DataFrame
        query1 = "SELECT " + ", ".join(columns) + " FROM " + self.table_name
        if self.table_filter:
            query1 += " WHERE " + self.table_filter
        if self.table_limit:
            query1 += " LIMIT " + str(self.table_limit)

        try:
            table_df = self.spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_database}") \
                .option("user", self.db_user) \
                .option("password", self.db_password) \
                .option("query", query1) \
                .load().cache()
        except Exception as e:
            # extract SQL part of error message
            sql_errors = str(e).split('\n')
            # print('sql_errors : ' + str(sql_errors))
            logging.error(
                f"Error reading to PostGIS table {sql_errors[0]} {sql_errors[1]}")
            return False, False, 'error'

        # Cast all the columns to be sure they are on the good type
        table_df = cast_columns(df=table_df, types=column_types)

        return table_df, column_types, 'success'
