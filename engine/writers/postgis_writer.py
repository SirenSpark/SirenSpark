"""
PostgisWriter
Effectue une écriture sur une table postgis
append : ajoute à une table existante
create : crée la table si besoin
drop : supprime la table et la recrée
truncate : trunctate la table avant d'ajouter
"""
import logging
import psycopg2
from pyspark.sql import DataFrameWriter
from pyspark.sql import SparkSession
from utils.postgis import get_table_columns, cast_df_by_table_columns, create_table_query, table_exists
from utils.evaluator import cast_columns
from typing import Dict, Any, Optional
from enum import Enum
from model_base import BaseStep


class PostgisWriterStepMethod(str, Enum):
    append = "append"
    create = "create"
    drop = "drop"
    truncate = "truncate"


class PostgisWriterStep(BaseStep):
    type = "PostgisWriter"
    options: Dict[str, Any] = {
        "db_host": str,
        "db_port": str,
        "db_database": str,
        "db_user": str,
        "db_password": str,
        "table_name": str,
        "method": Optional[PostgisWriterStepMethod]
    }


class PostgisWriter:
    def __init__(self, df, types, db_host, db_port, db_database, db_user, db_password, table_name, method="append"):
        self.df = df
        self.types = types
        self.db_host = db_host
        self.db_port = db_port
        self.db_database = db_database
        self.db_user = db_user
        self.db_password = db_password
        self.table_name = table_name
        self.method = method
        # start Spark session
        self.spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

    def check_columns(self):
        """
        Compare the columns of the existing table with the columns given into the df
        """

        # Get the table columns with type
        table_columns = get_table_columns(
            self.db_host, self.db_port, self.db_database, self.db_user, self.db_password, self.table_name)

        if len(table_columns) > 0:
            # Check if all DataFrame columns exist in the existing table
            for col in self.df.columns:
                if col not in [c['column_name'] for c in table_columns]:
                    logging.error(
                        f"Column '{col}' does not exist in the existing table")
                    return False

            # Cast DataFrame columns to match with existing_columns types
            self.df = cast_df_by_table_columns(self.df, table_columns)
        return self.df

    def create_table(self):
        """
        Creates a PostGIS table based on self.types
        """

        # Generate the CREATE TABLE query
        logging.info("Run create table")
        query = create_table_query(self.table_name, self.types)
        logging.info(f"Run SQL query to create table: {query}")

        # connect to Postgres
        conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            database=self.db_database,
            user=self.db_user,
            password=self.db_password
        )

        # create a cursor
        cur = conn.cursor()

        try:
            # execute the create table query
            cur.execute(query)

            # commit the transaction
            conn.commit()

            logging.info(f"Table {self.table_name} created successfully")
            return True
        except psycopg2.Error as e:
            logging.error(f"Error creating PostGIS table {e}")
            return False

        finally:
            # close the cursor and connection
            cur.close()
            conn.close()

    def drop_table(self):
        """
        Drop a PostGIS table
        """

        # Generate the DROP TABLE query
        logging.info("Run drop table")
        query = f"DROP TABLE IF EXISTS {self.table_name}"
        logging.info(f"Run SQL query to drop table: {query}")

        # connect to Postgres
        conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            database=self.db_database,
            user=self.db_user,
            password=self.db_password
        )

        # create a cursor
        cur = conn.cursor()

        # Return value
        try:
            # execute the create table query
            cur.execute(query)

            # commit the transaction
            conn.commit()

            logging.info(f"Table {self.table_name} dropped successfully")
            return True
        except psycopg2.Error as e:
            logging.error(f"Error dropping PostGIS table {e}")
            return False

        finally:
            # close the cursor and connection
            cur.close()
            conn.close()

    def truncate_table(self):
        """
        Tuncate a PostGIS table
        """
        # Generate the TRUNCATE TABLE query
        logging.info("Run truncate table")
        query = f"TRUNCATE TABLE {self.table_name}"
        logging.info(f"Run SQL query to truncate table: {query}")

        # connect to Postgres
        conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            database=self.db_database,
            user=self.db_user,
            password=self.db_password
        )

        # create a cursor
        cur = conn.cursor()

        # Return value
        try:
            # execute the create table query
            cur.execute(query)

            # commit the transaction
            conn.commit()

            logging.info(f"Table {self.table_name} truncated successfully")
            return True
        except psycopg2.Error as e:
            logging.error(f"Error truncating PostGIS table {e}")
            return False

        finally:
            # close the cursor and connection
            cur.close()
            conn.close()

    def run(self):
        """
        Run the insertion with methods create or append
        """
        res = True
        if res == True and self.method == "drop":
            res = self.drop_table()

        if res == True and self.method == "truncate":
            if table_exists(self.db_host, self.db_port, self.db_database, self.db_user, self.db_password, self.table_name) != False:
                res = self.truncate_table()
            else:
                logging.info(f"Table {self.table_name} does not exists")
                res = self.create_table()

        if res == True and (self.method == "create" or self.method == "drop"):
            res = self.create_table()

        if res == True and (self.method == "create" or self.method == "drop" or self.method == "append"):
            # Check if all DataFrame columns exist in the existing table
            self.df = self.check_columns()

        if res == True:
            # write DataFrame to PostGIS table
            logging.info(f"Write data into {self.table_name}")

            # Cast all the columns to be sure they are on the good type
            self.df = cast_columns(df=self.df, types=self.types)

            writer = DataFrameWriter(self.df)
            try:
                writer.jdbc(
                    url=f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_database}",
                    table=self.table_name,
                    mode="append",
                    properties={
                        "user": self.db_user,
                        "password": self.db_password
                    }
                )
                return self.df, self.types, 'success'
            except Exception as e:
                # extract SQL part of error message
                sql_errors = str(e).split('\n')
                logging.error(
                    f"Error writing to PostGIS table {sql_errors[0]} {sql_errors[1]}")
                return self.df, self.types, 'error'
        else:
            return self.df, self.types, 'error'
