""" 
Attribute creator
Crée un attribut dans le df en l'évaluant
"""
from utils.evaluator import eval_mapping, eval_column
from pyspark.sql.functions import monotonically_increasing_id, lit, row_number
from typing import Dict, Any, Optional
from model_base import BaseStep


class AttributeCreatorStep(BaseStep):
    type = "AttributeCreator"
    options: Dict[str, Any] = {
        "name": str,
        "expression": str,
        "type": Optional[str]
    }


class AttributeCreator:
    def __init__(self, name, expression, df, types, type='string', properties=None):
        self.attribute_name = name
        self.attribute_expression = expression
        self.attribute_type = type
        self.df = df
        self.types = types
        self.properties = properties

    def run(self):
        mapping = {}
        mapping[self.attribute_name] = self.attribute_expression

        self.df = eval_column(column_name=self.attribute_name,
                              expression=self.attribute_expression, df=self.df, type=self.attribute_type, properties=self.properties)
        self.types[self.attribute_name] = {
            "data_type": self.attribute_type
        }
        return self.df, self.types, 'success'
