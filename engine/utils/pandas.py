from pyspark.sql import SparkSession
import geopandas as gpd
import json


def toPandas(df, types):
    """
    Transforme la dataframe en geopandas
    """

    new_df = convertGeomsToText(df, types)
    pandas_df = new_df.fillna(0).toPandas()
    geom_column = 'geom'

    for key in types:
        if types[key]['data_type'] == 'geometry':
            geom_column = key
            pandas_df[key] = gpd.GeoSeries.from_wkt(pandas_df[key])

    return gpd.GeoDataFrame(pandas_df, geometry=geom_column, crs=4326)


def toSpark(pandas_df, types, schema=None):

    for key in types:
        if types[key]['data_type'] == 'geometry':
            pandas_df[key] = gpd.GeoSeries.to_wkb(pandas_df[key])

    spark = SparkSession.builder.appName("SirenSpark").getOrCreate()
    spark_df = spark.createDataFrame(pandas_df, schema)
    return spark_df


def convertGeomsToText(df, types):
    """
    Transforme les géométries d'un dataframe en WKT
    """

    # start Spark session
    spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

    for key in types:
        if types[key]['data_type'] == 'geometry':
            df.createOrReplaceTempView("v_table")
            query = f"SELECT *, ST_AsText(ST_GeomFromWKB({key})) as new_sirenspark_geometry FROM v_table"
            df = spark.sql(query)
            df = df.drop(key)
            df = df.withColumnRenamed("new_sirenspark_geometry", key)

    return df


def convertGeomsToBinary(df, types):
    """
    Transforme les géométries d'un dataframe en WKB
    """

    # start Spark session
    spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

    for key in types:
        if types[key]['data_type'] == 'geometry':
            df.createOrReplaceTempView("v_table")
            query = f"SELECT *, ST_AsBinary(ST_GeomFromWKT({key})) as new_sirenspark_geometry FROM v_table"
            df = spark.sql(query)
            df = df.drop(key)
            df = df.withColumnRenamed("new_sirenspark_geometry", key)

    return df
