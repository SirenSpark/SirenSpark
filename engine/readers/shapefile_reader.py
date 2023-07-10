""" 
ShapefileReader
Effectue une lecture sur un fichier Shape en utilisant gdal
"""
import logging
from osgeo import ogr
from pyspark.sql import SparkSession
from typing import Dict, Any, Optional
from model_base import BaseStep

class ShapefileReaderStep(BaseStep):
    type = "ShapefileReader"
    options: Dict[str, Any] = {
        "filepath": str
    }

class ShapefileReader:
    def __init__(self, filepath):
        self.filepath = filepath

    def run(self):
        spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

        # Open the shapefile
        shapefile = ogr.Open(self.filepath)

        # Get the first layer
        layer = shapefile.GetLayer(0)

        # Create a SparkSession
        spark = SparkSession.builder.appName("Shapefile to DataFrame").getOrCreate()

        # Create an empty list to store the data
        data = []

        # Default coordinate dimensions
        num_dims = 2

        # Loop through the features in the layer
        for feature in layer:
            # Get the feature's geometry
            geometry = feature.GetGeometryRef()

            # Get the number of dimensions of the geometry
            num_dims = geometry.GetCoordinateDimension()

            # Get the feature's attributes
            attributes = feature.items()

            # Add the data to the list
            if num_dims == 2:
                data.append((geometry.ExportToWkt(),) + tuple(attributes.values()))
            elif num_dims == 3:
                data.append((geometry.ExportToWkt(),) + tuple(attributes.values()))

        # Create a schema for the DataFrame
        fields = ["geometry"] + [field_defn.name for field_defn in layer.schema]
        schema = ",".join([f"`{field}` STRING" for field in fields])

        # Create the DataFrame
        df = spark.createDataFrame(data, schema)

        # Get the attribute types from the shapefile
        column_types = {}
        for field_defn in layer.schema:
            field_name = field_defn.name
            field_type = field_defn.GetTypeName()
            if field_type == "Integer":
                column_types[field_name] = {"data_type": "int"}
            elif field_type == "Real":
                column_types[field_name] = {"data_type": "float"}
            elif field_type == "String":
                column_types[field_name] = {"data_type": "string"}
            elif field_type == "Date":
                column_types[field_name] = {"data_type": "date"}
            elif field_type == "Time":
                column_types[field_name] = {"data_type": "time"}
            elif field_type == "DateTime":
                column_types[field_name] = {"data_type": "timestamp"}
            else:
                column_types[field_name] = {"data_type": "string"}

        if geometry:
            # Get the spatial reference system of the shapefile
            spatial_ref = layer.GetSpatialRef()

            # Get the projection as an SRID code
            srid = int(spatial_ref.GetAuthorityCode(None))
            proj = str(spatial_ref.GetAuthorityName(None))

            column_types['geometry'] = {"data_type": "geometry", "srid": srid, "coord_dimension": num_dims, "proj_name": proj}
            if geometry.GetGeometryType() == ogr.wkbPoint:
                column_types['geometry']['type'] = "POINT"
            elif geometry.GetGeometryType() == ogr.wkbLineString:
                column_types['geometry']['type'] = "LINESTRING"
            elif geometry.GetGeometryType() == ogr.wkbPolygon:
                column_types['geometry']['type'] = "POLYGON"
            elif geometry.GetGeometryType() == ogr.wkbMultiPoint:
                column_types['geometry']['type'] = "MULTIPOINT"
            elif geometry.GetGeometryType() == ogr.wkbMultiLineString:
                column_types['geometry']['type'] = "MULTILINESTRING"
            elif geometry.GetGeometryType() == ogr.wkbMultiPolygon:
                column_types['geometry']['type'] = "MULTIPOLYGON"
            else:
                column_types['geometry']['type'] = "GEOMETRY"

            # Transforms geometry format
            df = df.selectExpr("*", f"ST_AsEWKB(ST_SetSRID(ST_GeomFromWKT(geometry), {srid})) AS new_sirenspark_geometry")
            df = df.drop("geometry")
            df = df.withColumnRenamed("new_sirenspark_geometry", "geometry")

        return df, column_types, "success"