import pandas as pd
import numpy as np
import microdf as mdf
from typing import Union, List
from family_resources_survey.save import FRS, table_to_entity


def load(
    year: int,
    table: str,
    return_mdf: bool = True,
    columns: List[str] = None,
) -> Union[pd.DataFrame, mdf.MicroDataFrame]:
    year = str(year)
    data_path = FRS / "data" / year / "raw"
    if data_path.exists():
        if table is not None:
            df = pd.read_csv(
                data_path / (table + ".csv"),
                usecols=columns,
                index_col=table_to_entity[table] + "_id",
            ).apply(pd.to_numeric, errors="coerce")
            if table in ("adult", "benunit", "household") and return_mdf:
                df = mdf.MicroDataFrame(df, weights="GROSS4")
        return df
    else:
        raise FileNotFoundError("Could not find the data requested.")
