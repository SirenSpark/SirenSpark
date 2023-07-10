"""
JSONWriter
Ecrit un dataframe Spark dans un fichier JSON
"""
from pyspark.sql import DataFrame, DataFrameWriter
import glob
import shutil


class JSONFileWriter:
    def __init__(self, df: DataFrame, filepath: str):
        self.df = df
        self.filepath = filepath

    def run(self):
        # write DataFrame to JSON file
        tmpFolder = self.filepath + '_tmp'
        writer = DataFrameWriter(self.df.coalesce(1))
        writer.option("header", True).mode("overwrite").json(tmpFolder)

        # Mise en forme
        json_files = glob.glob(tmpFolder + '/*.json')
        if len(json_files) == 1:
            shutil.move(json_files[0], self.filepath)
            shutil.rmtree(tmpFolder)
