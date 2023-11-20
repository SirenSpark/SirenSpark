""" 
PythonCaller
Lance un fichier python
"""
from typing import Dict, Any, Optional
from model_base import BaseStep
import importlib.util


class PythonCallerStep(BaseStep):
    type = "PythonCaller"
    options: Dict[str, Any] = {
        "filepath": str,
    }


class PythonCaller:
    def __init__(self,df, types, filepath, properties=None):
        self.df = df
        self.types = types
        self.filepath = filepath
        self.properties = properties

    def run(self):

        # Charger le module à partir du chemin du fichier
        spec = importlib.util.spec_from_file_location("FeatureRunnerModule", self.filepath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Instancier et exécuter la classe FeatureRunner du module chargé
        feature_runner_instance = module.FeatureRunner(
            df=self.df, types=self.types, properties=self.properties
        )
        return feature_runner_instance.run()