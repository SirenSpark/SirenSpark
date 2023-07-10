from pyspark.sql import SparkSession
from lxml import etree

import logging
from typing import Dict, Any
from model_base import BaseStep


class XMLReaderStep(BaseStep):
    type = "XMLReader"
    options: Dict[str, Any] = {
        "filepath": str,
        "xpath": str
    }


class XMLReader:
    def __init__(self, filepath, xpath):
        self.filepath = filepath
        self.xpath = xpath

    def run(self):

        # Create a Spark DataFrame from the extracted data
        spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

        try:

            # Read XML
            root = etree.parse(self.filepath).getroot()

            # Use XPath to extract the layer names
            res = root.xpath(self.xpath)

            rows = []
            for element in res:
                row = {}
                for child in element.getchildren():
                    row[child.tag] = child.text
                rows.append(row)

            # Create a Spark DataFrame from the extracted data
            df = spark.createDataFrame(rows)

            # Create the column_types dictionary
            column_types = {col: str(dtype) for col, dtype in df.dtypes}

            return df, column_types, "success"
        except Exception as e:
            logging.error(f"Error reading XML file: {e}")
            return False, False, 'error'