"""
A Loader to load init values of sensitive variables.
"""

import os
import glob
import torch

from utils import var_ptn

class Variables:
    """
    This class is for recording existing variables and mapping them to unique ids
    variable symbol pattern: 'x7_421'
    """
    def __init__(self) -> None:
        self.name2val = {}
        self.count_used = 0

    def add_item(self, name: str, val: float) -> None:
        self.name2val[name] = val

    def contains(self, name: str) -> None:
        return name in self.name2val

class Loader:
    """
    Build variable table and load initial data of sensitive data from .tbl
    """
    def __init__(self, dbgen_dir: str = None) -> None:
        self.dbgen_dir = dbgen_dir or r"/../dbgen"

    def load_init_data(self, vars: Variables) -> None:
        for file_path in glob.glob(os.path.join(self.dbgen_dir, "*.tbl")):
            with open(file_path, "r") as file:
                for line in file:
                    items = line.split('|')
                    last_item = ""
                    for item in items:
                        match = var_ptn.match(item)
                        if match:
                            vars.add_item(item, float(last_item))
                        last_item = item
