from pydantic import BaseModel
from typing import Dict, List, Union, Any, Optional
from readers.postgis_reader import PostgisReaderStep
from readers.json_reader import JSONReaderStep
from readers.csv_reader import CSVReaderStep
from readers.xml_reader import XMLReaderStep
from writers.postgis_writer import PostgisWriterStep
from transformers.attribute_creator import AttributeCreatorStep
from transformers.attribute_mapper import AttributeMapperStep
from model_base import BaseStep


class Properties(BaseModel):
    name: Optional[str]
    description: Optional[str]
    parameters: Optional[Dict[str, Any]]


class Definition(BaseModel):
    properties: Properties
    trigger: BaseStep
    steps: List[Union[PostgisReaderStep, AttributeCreatorStep,
                      AttributeMapperStep, PostgisWriterStep, JSONReaderStep, CSVReaderStep, XMLReaderStep]]


class ColumnType(BaseModel):
    data_type: str
    pg_data_type: Optional[str]
    pg_character_maximum_length: Optional[int]


class ColumnTypeGeometry(ColumnType):
    type: Optional[str]
    srid: Optional[int]
    coord_dimension: Optional[int]
