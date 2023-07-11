from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf, to_date, to_timestamp
from pyspark.sql.types import *
import logging
import jmespath
import re
import json

# Define a UDF to evaluate the expression for each row in the DataFrame


def evaluate_expression(row, expression, type='string', properties=None):
    value = expression
    placeholders = re.findall(r"\{\{([^}]*)\}\}", value)
    for var in placeholders:
        if var.startswith("data."):
            col = var[5:]
            if row and col in row:
                value = value.replace("{{" + var + "}}", str(row[col]))
        elif var.startswith("properties."):
            col = var[11:]
            value = value.replace(
                "{{" + var + "}}", str(jmespath.search(col, dict(properties))))

    if type != 'string':
        try:
            value = cast_value(value, type)
        except:
            logging.info(f"Cannot cast {value} to {type}")
    return value


def eval_mapping_(mapping, df, types=None, properties=None):
    """
    This function creates a new DF based on the columns into the mapping dictionnary
    """
    # start Spark session
    spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

    # Create an empty DataFrame with columns based on the keys in the mapping dictionary
    fields = [StructField(key, StringType(), True)
              for key in mapping.keys()]
    schema = StructType(fields)
    new_rows = []

    if df:
        # Iterate over the rows in the original DataFrame and accumulate the new rows
        for row in df.rdd.collect():
            new_row = []
            for key in mapping.keys():
                new_value = evaluate_expression(
                    row=row, expression=mapping[key], properties=properties)
                new_row.append(new_value)
            new_rows.append(new_row)

    else:
        new_row = []
        for key in mapping.keys():
            new_value = evaluate_expression(
                row=None, expression=mapping[key], properties=properties)
            new_row.append(new_value)
        new_rows.append(new_row)

    # Create the new DataFrame from the accumulated rows
    new_df = spark.createDataFrame(new_rows, schema)

    # Rename the columns in the new DataFrame to match the keys in the mapping dictionary
    new_df = new_df.toDF(*mapping.keys())

    return new_df


def eval_mapping(mapping, df, types=None, properties=None):
    """
    This function creates a new DF based on the columns into the mapping dictionnary
    """
    # start Spark session
    spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

    # Create an empty DataFrame with columns based on the keys in the mapping dictionary
    fields = [StructField(key, StringType(), True)
              for key in mapping.keys()]
    schema = StructType(fields)

    if df:

        cols = list(df.columns)
        def eval_row(expression, vals):

            # Convert the list of pairs to a dictionary
            row = dict(list(zip(cols, vals)))

            # Return the evaluation
            val = evaluate_expression(row, expression, 'sring', properties)
            return val

        for key in mapping.keys():
            my_udf = udf(
                lambda *row: eval_row(mapping[key], row), StringType())
            df = df.withColumn(key, my_udf(*df))

        cols = list(df.columns)
        for col in cols:
            if col not in mapping:
                df = df.drop(col)

        new_df = df

    else:
        new_row = []
        for key in mapping.keys():
            new_value = evaluate_expression(
                row=None, expression=mapping[key], properties=properties)
            new_row.append(new_value)
        new_df = spark.createDataFrame([new_row], schema)

    # Rename the columns in the new DataFrame to match the keys in the mapping dictionary
    new_df = new_df.toDF(*mapping.keys())

    return new_df


def eval_column(column_name, expression, df, type='string', properties=None):
    """
    This function adds a column named by column_name into the df and the value is the evaluation of expression
    """
    if df:
        cols = list(df.columns)

        def eval_row(*vals):
            # Convert the list of pairs to a dictionary
            row = dict(list(zip(cols, vals)))

            # Print the resulting dictionary
            return evaluate_expression(row, expression, type, properties)

        # Register the custom function as a UDF
        my_udf = udf(eval_row, StringType())

        # Use withColumn to apply the custom function to each row and set the value of a new column
        df = df.withColumn(column_name, my_udf(*df.columns))
    else:
        # Creates a df with one column and one row
        spark = SparkSession.builder.appName("SirenSpark").getOrCreate()
        df = spark.createDataFrame([(expression, )], schema=StructType(
            [StructField(column_name, StringType(), True)]))

    return df


def cast_value(value, type):
    """
    Cast a value to his type
    """
    if type == "boolean":
        return bool(value)
    elif type in ["string", "binary"]:
        return value
    elif type == "double":
        return float(value)
    elif type == "date":
        return to_date(value)
    elif type == "timestamp":
        return to_timestamp(value)
    elif type == "decimal":
        return float(value)
    elif type == "short":
        return int(value)
    elif type == "long":
        return int(value)
    elif type == "float":
        return float(value)
    elif type == "integer":
        return int(value)
    else:
        raise ValueError(f"Invalid type: {type}")


def cast_columns(df, types):
    """
    Cast all the columns of the DF to the good types
    """
    for col, col_type in types.items():
        data_type = col_type.get("data_type", None)
        if data_type != None:
            if data_type == "boolean":
                df = df.withColumn(col, df[col].cast("boolean"))
            elif data_type == "string":
                df = df.withColumn(col, df[col].cast("string"))
            elif data_type == "binary":
                df = df.withColumn(col, df[col].cast("binary"))
            elif data_type == "date":
                df = df.withColumn(col, to_date(df[col]))
            elif data_type == "timestamp":
                df = df.withColumn(col, to_timestamp(df[col]))
            elif data_type == "double":
                df = df.withColumn(col, df[col].cast("double"))
            elif data_type == "integer":
                df = df.withColumn(col, df[col].cast("integer"))
            elif data_type == "decimal":
                df = df.withColumn(col, df[col].cast("decimal"))
            elif data_type == "float":
                df = df.withColumn(col, df[col].cast("float"))
            elif data_type == "short":
                df = df.withColumn(col, df[col].cast("short"))
            elif data_type == "long":
                df = df.withColumn(col, df[col].cast("long"))
    return df
