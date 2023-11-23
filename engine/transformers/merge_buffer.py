"""
MergeBuffer
Merge les features qui sont proches et les met dans un cluster json
deux colonnes sont ajoutées 
* clustered : contient le nombre d'éléments mergés
* cluster : contient la définition JSON de l'ensembe
"""
from model_base import BaseStep
from typing import Dict, Any, Optional
from utils.pandas import toPandas, toSpark
from pyspark.sql.types import StructType, StructField, StringType
from shapely.geometry import MultiPoint
from shapely import contains
from tqdm import tqdm
import shapely.wkt
import json


class MergeBufferStep(BaseStep):
    type = "MergeBuffer"
    options: Dict[str, Any] = {
        "geom_column": str,
        "distance_threshold": int,
    }


class MergeBuffer:
    def __init__(self, df, types, geom_column='geometry', distance_threshold=1, properties=None):
        self.df = df
        self.types = types
        self.properties = properties
        self.geom_column = geom_column
        self.distance_threshold = distance_threshold

    def run(self):

        # Transformation vers pandas
        pandas_df = toPandas(self.df, self.types)

        # Conversion dans un système métrique pour que le buffer puisse fonctionner
        pandas_df = pandas_df.to_crs(3857)

        # Calcul du buffer
        buffered = pandas_df[self.geom_column].buffer(self.distance_threshold)

        clustered = []
        neighbors = []
        cluster = []
        for index, row in tqdm(pandas_df.iterrows(), total=len(pandas_df), desc="Processing features", unit="row"):

            # Trouve les géométries intersectantes
            contained = contains(buffered, shapely.wkt.loads(
                str(row[self.geom_column])))

            # Voisins trouvés
            rowNeighbors = []
            for index2 in range(len(pandas_df)):
                if contained[index2] and index2 != index:
                    rowNeighbors.append(index2)

            clustered.append(len(rowNeighbors))
            neighbors.append(rowNeighbors)
            cluster.append('')

        pandas_df['clustered'] = clustered
        pandas_df['cluster'] = cluster

        # Traite les voisinages
        toDelete = {}
        for index in range(len(neighbors)):

            # S'il a des voisins et qu'il n'est pas marqué comme à supprimer
            if len(neighbors[index]) > 0 and not index in toDelete:

                # Renseigne ses voisins comme à supprimer
                for nIdx in neighbors[index]:
                    toDelete[nIdx] = True

                # Change la géométrie en affectant le centre
                points = [pandas_df[self.geom_column][index]]
                for nIdx in neighbors[index]:
                    points.append(pandas_df[self.geom_column][nIdx])
                multipoint = MultiPoint(points)
                pandas_df[self.geom_column][index] = multipoint.centroid

                # Colonne cluster
                clusterData = [
                    self.pop(pandas_df.loc[index].to_dict(), self.geom_column)]
                for nIdx in neighbors[index]:
                    clusterData.append(
                        self.pop(pandas_df.loc[nIdx].to_dict(), self.geom_column))

                pandas_df['cluster'][index] = json.dumps(clusterData)

        # Supprime les doublons
        pandas_df = pandas_df.drop(index=list(toDelete))

        # Conversion dans le système original
        pandas_df = pandas_df.to_crs(4326)

        # Transformation vers spark
        spark_df = toSpark(pandas_df, self.types, StructType(
            self.df.schema.fields + [
                StructField('clustered', StringType(), True),
                StructField('cluster', StringType(), True),
            ]))

        # Ajout des nouveaux types
        types = {**self.types, **{
            'cluster': {
                "data_type": "array_json",
            },
            'clustered': {
                "data_type": "string",
            }
        }}

        return spark_df, types, "success"

    def pop(self, arr, key):
        arr.pop(key)
        return arr
