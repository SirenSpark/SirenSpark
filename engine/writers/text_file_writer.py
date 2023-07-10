"""
TextFileWriter
Ecrit un dataframe Spark dans un fichier texte
"""
from pyspark.sql import DataFrame, DataFrameWriter
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col
from pyspark.sql.functions import concat_ws

import glob
import shutil


class TextFileWriter:
    def __init__(self, df: DataFrame, filepath: str, sep: str = '\t', header: bool = False):
        self.df = df
        self.filepath = filepath
        self.sep = sep
        self.header = header

    def run(self):
        # write DataFrame to text file
        tmpFolder = self.filepath + '_tmp'

        # concatenate all columns into a single string column
        df_str = self.df.select(
            concat_ws(self.sep, *self.df.columns).alias('data'))

        writer = DataFrameWriter(df_str.coalesce(1))
        writer.option("header", self.header).mode(
            "overwrite").option("delimiter", self.sep).text(tmpFolder)

        # Mise en forme
        txt_files = glob.glob(tmpFolder + '/*.txt')
        if len(txt_files) == 1:
            shutil.move(txt_files[0], self.filepath)
            shutil.rmtree(tmpFolder)
