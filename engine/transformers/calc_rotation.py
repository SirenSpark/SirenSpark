"""
CalcRotation
Ajoute une colonne au DataFrame source avec la géométrie du point le plus proche du DataFrame cible
"""
from model_base import BaseStep
from typing import Dict, Any, Optional
from shapely.wkt import loads
from utils.pandas import toPandas, convertGeomsToText, convertGeomsToBinary
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, NumericType, IntegerType
from tqdm import tqdm
import math


class CalcRotationStep(BaseStep):
    type = "CalcRotation"
    options: Dict[str, Any] = {
        "geom_column_1": str,
        "geom_column_2": str,
        "new_column_name": str,
    }


class CalcRotation:
    def __init__(self, df, types, geom_column_1='geom', geom_column_2='geom', new_column_name='rotation', properties=None):
        self.df = df
        self.types = types
        self.geom_column_1 = geom_column_1
        self.geom_column_2 = geom_column_2
        self.new_column_name = new_column_name
        self.properties = properties

    def run(self):

        new_df = convertGeomsToText(self.df, self.types)
        pandas_df = new_df.fillna(0).toPandas()

        angles = []

        for index, row in tqdm(pandas_df.iterrows(), total=len(pandas_df), desc="Processing features", unit="row"):
            geom_1 = loads(row[self.geom_column_1])
            geom_2 = loads(row[self.geom_column_2])

            angles.append(self.calcRotation(geom_1, geom_2))

        pandas_df[self.new_column_name] = angles

        pandas_df = pandas_df[new_df.columns + [self.new_column_name]]

        # Transformation du df pandas en df spark
        spark = SparkSession.builder.appName("SirenSpark").getOrCreate()

        schema = StructType(
            new_df.schema.fields + [StructField(self.new_column_name, StringType(), True)])

        spark_df = spark.createDataFrame(pandas_df, schema)

        spark_df = convertGeomsToBinary(spark_df, self.types)

        types = {**self.types, **{self.new_column_name: {
            "data_type": "number",
            "pg_data_type": "number",
        }}}

        return spark_df, types, 'success'

    def calcRotation(self, geom_1, geom_2):
        point1 = [geom_1.x, geom_1.y, 0]
        point2 = [geom_2.x, geom_2.y, 0]

        angle_degres = self.angle_entre_deux_points(point1, point2)

        return angle_degres

    # Fonction pour calculer le vecteur entre deux points
    def calculer_vecteur(self, point1, point2):
        return [point1[0] - point2[0], point1[1] - point2[1], point1[2] - point2[2]]

    # Fonction pour calculer la norme d'un vecteur
    def norme_vecteur(self, vecteur):
        return math.sqrt(vecteur[0] * vecteur[0] + vecteur[1] * vecteur[1] + vecteur[2] * vecteur[2])

    # Fonction pour calculer l'angle en degrés entre trois points
    def angle_entre_deux_points(self, point1, point2):

        # Création d'un troisième point au-dessus du point 1
        point0 = [point1[0], point1[1] + 1, point1[2]]

        # Calcul des vecteurs entre les points
        vecteurA = self.calculer_vecteur(point0, point1)
        vecteurB = self.calculer_vecteur(point2, point1)

        # Calcul du produit scalaire
        dot_product = vecteurA[0] * vecteurB[0] + \
            vecteurA[1] * vecteurB[1] + vecteurA[2] * vecteurB[2]

        # Calcul des normes des vecteurs
        normeA = self.norme_vecteur(vecteurA)
        normeB = self.norme_vecteur(vecteurB)

        # Calcul de l'angle en radians
        angle_radians = math.acos(dot_product / (normeA * normeB))

        # Conversion en degrés
        angle_degres = angle_radians * (180 / math.pi)

        # Calcul de la direction
        if vecteurB[0] < 0:
            angle_degres = 360 - angle_degres

        return round(angle_degres)
