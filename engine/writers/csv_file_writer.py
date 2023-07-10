""" 
CSVWriter
Écrit un dataframe Spark dans un fichier CSV
"""
from pyspark.sql import DataFrame, DataFrameWriter
import glob
import shutil


class CSVFileWriter:
    def __init__(self, df: DataFrame, filepath: str):
        self.df = df
        self.filepath = filepath

    def run(self):
        # write DataFrame to CSV file
        tmpFolder = self.filepath + '_tmp'
        writer = DataFrameWriter(self.df.coalesce(1))
        writer.option("header", True).mode("overwrite").csv(tmpFolder)

        # Mise en forme
        csv_files = glob.glob(tmpFolder + '/*.csv')
        if len(csv_files) == 1:
            shutil.move(csv_files[0], self.filepath)
            shutil.rmtree(tmpFolder)