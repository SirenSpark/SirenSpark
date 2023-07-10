from pyspark.sql.functions import col
from typing import Dict, Any, Optional
from enum import Enum
from model_base import BaseStep


class JoinerStepType(str, Enum):
    inner = "inner"
    left_outer = "left_outer"
    right_outer = "right_outer"
    full_outer = "full_outer"
    left_semi = "left_semi"
    left_anti = "left_anti"


class JoinerStep(BaseStep):
    type = "Joiner"
    options: Dict[str, Any] = {
        "join_type": JoinerStepType,
        "join_keys": [str]
    }


class Joiner:
    def __init__(self, left_df, right_df, left_types, right_types, join_type='inner', join_keys=None):
        self.left_df = left_df
        self.right_df = right_df
        self.left_types = left_types
        self.right_types = right_types
        self.join_type = join_type
        self.join_keys = join_keys

    def run(self):

        # handle join keys
        if self.join_keys is None:
            join_keys = self.left_df.columns
        else:
            join_keys = self.join_keys

        # check for duplicate column names and rename columns of right DataFrame if necessary
        right_cols = set(self.right_df.columns)
        left_cols = set(self.left_df.columns)
        common_cols = right_cols.intersection(left_cols)
        for col in common_cols:
            if col not in join_keys:
                self.right_df = self.right_df.drop(col)
                del self.right_types[col]

        # perform join
        joined_df = self.left_df.join(
            self.right_df, on=join_keys, how=self.join_type)
        
        # merge types
        types = {**self.left_types, **self.right_types}

        return joined_df, types, 'success'
