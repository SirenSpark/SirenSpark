"""
Class Runner : runs the definition
"""
from models import Definition
from model_base import BaseStep
from datetime import datetime
from readers.postgis_reader import PostgisReader
from readers.json_reader import JSONReader
from readers.csv_reader import CSVReader
from readers.xml_reader import XMLReader
from readers.shapefile_reader import ShapefileReader
from readers.geojson_reader import GeoJSONReader
from writers.postgis_writer import PostgisWriter
from writers.json_file_writer import JSONFileWriter
from writers.geojson_file_writer import GeoJSONFileWriter
from writers.shapefile_writer import ShapefileWriter
from transformers.attribute_creator import AttributeCreator
from transformers.attribute_mapper import AttributeMapper
from transformers.joiner import Joiner
from transformers.array_joiner import ArrayJoiner
from transformers.closest_point import ClosestPoint
from transformers.calc_rotation import CalcRotation
from transformers.python_caller import PythonCaller
from pyspark.sql import SparkSession
import logging
import json


class Runner:
    def __init__(self, definition: Definition):
        self.spark = SparkSession.builder.appName("myApp").getOrCreate()
        self.definition = definition
        self.properties = {}
        self.steps_cache = {}

    def run(self):
        """
        Run the steps frop the definition
        """
        if self.definition.properties:
            self.properties = self.definition.properties
        if self.definition.trigger:
            first = self.definition.trigger
            self.recusrive_run_step(step=first)

        # Clear cached values
        for step_id in self.steps_cache:
            self.steps_cache[step_id]["values"].unpersist()

    def recusrive_run_step(self, step: BaseStep, curr_df=None, curr_types={}):
        """
        Recursive function that founds and runs the next step
        """
        curr_df, curr_types, output = self.run_step(
            step=step, curr_df=curr_df, curr_types=curr_types)
        if output in step.output:
            for id in step.output[output]:
                next_step = self.find_step_by_id(id)
                if next_step:
                    if next_step.id in self.steps_cache:
                        logging.error(
                            f"Loops are not allowed: {next_step.id} already launched")
                    elif next_step.input == None or self.are_inputs_loaded(next_step.input):
                        self.recusrive_run_step(
                            step=next_step, curr_df=curr_df, curr_types=curr_types)

    def run_step(self, step: BaseStep, curr_df=None, curr_types={}):
        """
        Run a step
        """
        logging.info(
            f"------------------------------ Run {step.id} ------------------------------")
        start_time = datetime.now()
        output = 'success'

        if step.type == 'creator':
            df = None
            types = {}
        elif step.type == 'PostgisReader':
            df, types, output = PostgisReader(**step.options).run()
        elif step.type == 'JSONReader':
            df, types, output = JSONReader(**step.options).run()
        elif step.type == 'CSVReader':
            df, types, output = CSVReader(**step.options).run()
        elif step.type == 'XMLReader':
            df, types, output = XMLReader(**step.options).run()
        elif step.type == 'ShapefileReader':
            df, types, output = ShapefileReader(**step.options).run()
        elif step.type == 'GeoJSONReader':
            df, types, output = GeoJSONReader(**step.options).run()
        elif step.type == 'AttributeCreator':
            df, types, output = AttributeCreator(
                df=curr_df, types=curr_types, properties=self.properties, **step.options).run()
        elif step.type == 'AttributeMapper':
            df, types, output = AttributeMapper(
                df=curr_df, properties=self.properties, **step.options).run()
        elif step.type == 'PostgisWriter':
            df, types, output = PostgisWriter(
                df=curr_df, types=curr_types, **step.options).run()
        elif step.type == 'JSONFileWriter':
            df, types, output = JSONFileWriter(
                df=curr_df, types=curr_types, **step.options).run()
        elif step.type == 'GeoJSONFileWriter':
            df, types, output = GeoJSONFileWriter(
                df=curr_df, types=curr_types, **step.options).run()
        elif step.type == 'ShapefileWriter':
            df, types, output = ShapefileWriter(
                df=curr_df, types=curr_types, **step.options).run()
        elif step.type == 'Joiner':
            if ("left" in step.input and
                step.input["left"] in self.steps_cache and
                "right" in step.input and
                    step.input["right"] in self.steps_cache):
                df, types, output = Joiner(
                    left_df=self.steps_cache[step.input["left"]]["values"],
                    right_df=self.steps_cache[step.input["right"]]["values"],
                    left_types=self.steps_cache[step.input["left"]]["types"],
                    right_types=self.steps_cache[step.input["right"]]["types"],
                    **step.options).run()
            else:
                logging.error(
                    f"Input left and right not defined or has no value: {step.input}")
                df = False
        elif step.type == 'ArrayJoiner':
            if ("left" in step.input and
                step.input["left"] in self.steps_cache and
                "right" in step.input and
                    step.input["right"] in self.steps_cache):
                df, types, output = ArrayJoiner(
                    left_df=self.steps_cache[step.input["left"]]["values"],
                    right_df=self.steps_cache[step.input["right"]]["values"],
                    left_types=self.steps_cache[step.input["left"]]["types"],
                    right_types=self.steps_cache[step.input["right"]]["types"],
                    **step.options).run()
            else:
                logging.error(
                    f"Input left and right not defined or has no value: {step.input}")
                df = False
        elif step.type == 'ClosestPoint':            
            if ("left" in step.input and
                step.input["left"] in self.steps_cache and
                "right" in step.input and
                    step.input["right"] in self.steps_cache):
                df, types, output = ClosestPoint(
                    left_df=self.steps_cache[step.input["left"]]["values"],
                    right_df=self.steps_cache[step.input["right"]]["values"],
                    left_types=self.steps_cache[step.input["left"]]["types"],
                    right_types=self.steps_cache[step.input["right"]]["types"],
                    **step.options).run()
            else:
                logging.error(
                    f"Input left and right not defined or has no value: {step.input}")
                df = False
        elif step.type == 'CalcRotation':
            df, types, output = CalcRotation(
                df=curr_df, types=curr_types, properties=self.properties, **step.options).run()
        elif step.type == 'PythonCaller':
            df, types, output = PythonCaller(
                df=curr_df, types=curr_types, properties=self.properties, **step.options).run()
        else:
            logging.error(f"Type {step.type} not recognized")
            df = False

        if df == False:
            logging.error(
                f"------------------------------ Error while running {step.id} ------------------------------")
            return curr_df, curr_types, 'error'

        if df:
            self.steps_cache[step.id] = {
                "values": df.cache(),
                "types": types
            }
            # print('types : ' + json.dumps(types))
            # print('df.show(10) : ' + str(df.show(10)))
            # print('df.count() : ' + str(df.count()))

        time_diff = (datetime.now() - start_time).total_seconds()
        logging.info(f"Total time taken by {step.type}: {time_diff} seconds")

        return df, types, output

    def are_inputs_loaded(self, inputs):
        """
        Check self.steps_cache and returns true if all the inputs have a value
        """
        loaded = True
        for key in inputs:
            input_step_id = inputs[key]
            if input_step_id not in self.steps_cache:
                loaded = False
        return loaded

    def find_step_by_id(self, step_id):
        # check if the JSON data has a "steps" key
        if self.definition.steps:
            # loop through the steps and check if the ID matches
            for step in self.definition.steps:
                if step.id == step_id:
                    return step
        # if the "steps" key is not present or the ID is not found, return None
        return None
