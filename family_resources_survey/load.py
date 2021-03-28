import json
import pandas as pd
import numpy as np
import microdf as mdf
from typing import Union, List
from family_resources_survey.save import FRS_path, table_to_entity
from pathlib import Path


def load(
    year: int,
    table: str,
    return_mdf: bool = True,
    columns: List[str] = None,
) -> Union[pd.DataFrame, mdf.MicroDataFrame]:
    year = str(year)
    data_path = FRS_path / "data" / year / "raw"
    if data_path.exists():
        if table is not None:
            df = pd.read_csv(
                data_path / (table + ".csv"),
                usecols=columns,
                index_col=table_to_entity[table] + "_id",
                low_memory=False
            ).apply(pd.to_numeric, errors="coerce")
            if table in ("adult", "benunit", "househol") and return_mdf:
                df = mdf.MicroDataFrame(df, weights="GROSS4")
        return df
    else:
        raise FileNotFoundError("Could not find the data requested.")

class FRS:
    def __init__(self, year : int):
        self.year = year
        self.tables = {}
        self.data_path = FRS_path / "data" / str(year)
        codebook_path = self.data_path / "codebook.json"
        if codebook_path.exists():
            with open(codebook_path, "r") as f:
                self.description = json.load(f)
    
    def __getattr__(self, name: str) -> pd.DataFrame:
        if name == "description":
            return self.description
        if name not in self.tables:
            self.tables[name] = load(self.year, name)
        return self.tables[name]