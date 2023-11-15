"""
ClosestPoint
Ajoute une colonne au DataFrame source avec la géométrie du point le plus proche du DataFrame cible
"""
from model_base import BaseStep
from typing import Dict, Any, Optional
from utils.pandas import toPandas

import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import nearest_points

class ClosestPointStep(BaseStep):
    type = "ClosestPoint"
    options: Dict[str, Any] = {
        "left_geom_column": str,
        "right_geom_column": str,
        "new_column_name": str
    }

class ClosestPoint:
    def __init__(self, left_df, right_df, left_types, right_types, left_geom_column='geom', right_geom_column='geom', new_column_name='closest_point'):
        self.left_df = left_df
        self.left_types = left_types
        self.left_geom_column = left_geom_column
        self.right_df = right_df
        self.right_types = right_types
        self.right_geom_column = right_geom_column
        self.new_column_name = new_column_name

    def run(self):

        left_gdf = toPandas(self.left_df, self.left_types)
        right_gdf = toPandas(self.right_df, self.right_types)

        # Union des géométries de la right_gdf
        right_union = gpd.GeoSeries(right_gdf[self.right_geom_column]).unary_union
        # print('union : ' + str(right_union))

        def find_nearest_geometry(row, right_df):
            left_geom = gpd.GeoSeries(row[self.left_geom_column])
            # Utilise la fonction nearest_points de Shapely pour trouver les points les plus proches
            nearest = nearest_points(left_geom, right_union)
            print('nearest : ' + str(nearest))
            # Renvoie la géométrie correspondante du DataFrame
            return nearest[1]
            # return right_df.loc[right_df['geometry'] == nearest[1], 'geometry'].iloc[0]

        left_gdf['nearest_geom'] = left_gdf.apply(lambda row: find_nearest_geometry(row, right_gdf), axis=1)

        print('left_gdf.head(10) : ' + str(left_gdf.head(10)))
        # print('right_gdf.head(10) : ' + str(right_gdf.head(10)))


        # print('left_gdf[geom].unary_union : ' + str(left_gdf['geom'].unary_union))

        # gpd.GeoSeries()

        # left_gs = gpd.GeoSeries.from_wkt(left_gdf['geom'])
        # right_gs = gpd.GeoSeries.from_wkt(right_gdf['geometry'])



        # distance_serie = left_gs.distance(right_gs)
        # distance_serie = left_gdf.distance(right_gdf)


        # pandas_left = self.left_df.toPandas()
        # right_gdf = self.right_df.toPandas()


        # pandas_left['geom'] = gpd.GeoSeries.from_wkb(pandas_left['geom'])


        # left_gdf = gpd.GeoDataFrame(pandas_left, geometry='geom')



        # def find_nearest_geometry(row, right_df):
        #     left_geom = row['geom']
        #     # Utilise la fonction nearest_points de Shapely pour trouver les points les plus proches
        #     nearest = nearest_points(left_geom, right_df['geometry'].unary_union)
        #     # Renvoie la géométrie correspondante du DataFrame right
        #     return right_df.loc[right_df['geometry'] == nearest[1], 'geometry'].iloc[0]



        # left_gdf['nearest_geom'] = left_gdf.apply(lambda row: find_nearest_geometry(row, right_gdf), axis=1)


        # print('left_gdf.geom : ' + str(pandas_left['geom']))

        # print('left_gdf.head(10) : ' + str(pandas_left.head(10)))


        # # Convert GeoDataFrames to a GeoSeries of points
        # left_points = left_gdf[self.left_geom_column].apply(lambda geom: geom.representative_point())
        # right_points = right_gdf[self.right_geom_column].apply(lambda geom: geom.representative_point())

        # # Find the closest point for each left point
        # closest_points = left_points.apply(lambda point: right_points.distance(point).idxmin())
        # closest_geometry = self.right_gdf.loc[closest_points][self.right_geom_column]

        # # Add the new column to the left GeoDataFrame
        # left_gdf[self.new_column_name] = closest_geometry.values

        # print('left_gdf.head(10) : ' + str(left_gdf.head(10)))


        # # types = {**self.left_types, **{self.new_column_name: 'geometry'}}

        # # return self.left_gdf, types, 'success'




        closest_point_df = self.left_df.limit(100)

        types = {**self.left_types, **{self.new_column_name: 'string'}}

        return closest_point_df, types, 'success'
