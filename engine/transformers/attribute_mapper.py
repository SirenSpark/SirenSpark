""" 
Attribute mapper
Retourne un DF en évaluant le mapping en entrée
"""
from utils.evaluator import eval_mapping, cast_columns
from typing import Dict, Any, Optional
from model_base import BaseStep


class AttributeMapperStep(BaseStep):
    type = "AttributeMapper"
    options: Dict[str, Any] = {
        "mapping": Dict[str, str],
        "types": Dict[str, Dict[str, str]]
    }


class AttributeMapper:
    def __init__(self, mapping, df, types=None, properties=None):
        self.mapping = mapping
        self.df = df
        self.types = types
        self.properties = properties

    def run(self):
        self.df = eval_mapping(
            mapping=self.mapping, df=self.df, types=self.types, properties=self.properties)
        self.df = cast_columns(df=self.df, types=self.types)

        return self.df, self.types, 'success'
