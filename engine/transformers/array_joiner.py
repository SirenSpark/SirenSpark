from pyspark.sql import functions as F
from model_base import BaseStep
from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType


class ArrayJoinerStep(BaseStep):
    type = "ArrayJoiner"
    options: Dict[str, Any] = {
        'left_join_column': str,
        'right_join_column': str,
        'new_column_name': str
    }


class ArrayJoiner:
    def __init__(self, left_df, right_df, left_types, right_types, left_join_column, right_join_column, new_column_name):
        self.left_df = left_df
        self.left_types = left_types
        self.right_df = right_df
        self.right_types = right_types
        self.left_join_column = left_join_column
        self.right_join_column = right_join_column
        self.new_column_name = new_column_name

    def run(self):
        # Prefixe toutes les colonnes de right_df
        prefix = 'sirenspark_array_joiner_right_'
        for col_name in self.right_df.columns:
            self.right_df = self.right_df.withColumnRenamed(
                col_name, prefix + col_name)

        # Effectuer la jointure
        joined_df = self.left_df.alias("left").join(
            self.right_df.alias("right"),
            col("left." + self.left_join_column) == col(
                "right." + prefix + self.right_join_column),
            "left_outer"
        )

        # Collecter les résultats au format JSON
        json_col = F.to_json(F.struct([col(c) for c in self.right_df.columns]))
        joined_df = joined_df.groupBy(self.left_df.columns).agg(
            F.collect_list(json_col).alias(self.new_column_name))

        # Supprimer le préfixe du JSON dans la colonne nouvellement créée
        joined_df = joined_df.withColumn(
            self.new_column_name, F.expr(f"transform({self.new_column_name}, x -> regexp_replace(x, '{prefix}', ''))"))

        # Créer le DataFrame final avec les colonnes préfixées
        newdf = joined_df.select(
            *[F.col("left." + col_name).alias(col_name)
              for col_name in self.left_df.columns],
            F.col(self.new_column_name).alias(self.new_column_name)
        ).distinct()

        types = {**self.left_types, **{self.new_column_name: {
            "data_type": "array_json",
            "pg_data_type": "text",
        }}}

        return newdf, types, 'success'
