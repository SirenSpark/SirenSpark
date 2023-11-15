import os
import sys
import logging
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import KryoSerializer, SedonaKryoRegistrator
from pydantic import ValidationError
from models import Definition
from runner.runner import Runner

# configure the logging module
logging.basicConfig(level=logging.INFO)

# set path to JDBC driver JAR file
jars_path = os.path.dirname(os.path.abspath(__file__)) + "/../jars"
jdbc_driver_path = jars_path + "/postgresql-42.6.0.jar"
sedona_driver_path = jars_path + "/sedona-spark-shaded-3.0_2.12-1.5.0.jar"
geotools_driver_path = jars_path + "/geotools-wrapper-1.4.0-28.2.jar"
xml_driver_path = jars_path + "/spark-xml_2.12-0.13.0.jar"

# start Spark session with SedonaRegistrator and JDBC driver
spark = SparkSession.builder.appName("SirenSpark") \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .config("spark.executor.extraClassPath", jdbc_driver_path) \
    .config("spark.serializer", KryoSerializer.getName) \
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName) \
    .config("spark.jars", sedona_driver_path + "," + geotools_driver_path + "," + xml_driver_path) \
    .getOrCreate()

# register Sedona functions with Spark
SedonaRegistrator.registerAll(spark)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Please provide a file path as a command line argument")
        sys.exit(1)
    file_path = sys.argv[1]
    try:
        with open(file_path, "r") as f:
            config_data = f.read()
        definition = Definition.parse_raw(config_data)
        logging.info("Config file is valid")
        Runner(definition=definition).run()
        logging.info("END")

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        sys.exit(1)
    except ValidationError as e:
        logging.error(f"Invalid config file: {e}")
        sys.exit(1)
