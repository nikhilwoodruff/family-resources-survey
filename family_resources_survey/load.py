import pandas as pd
import microdf as mdf
from typing import Union, List
from family_resources_survey.save import FRS

def load(year: int, table : str = None, return_mdf : bool = True, columns : List[str] = None) -> Union[pd.DataFrame, mdf.MicroDataFrame]:
    year = str(year)
    data_path = FRS / "data" / year / "raw"
    if data_path.exists():
        if table is not None:
            df = pd.read_csv(data_path / table + ".tab", delimiter="\t", usecols=columns)
            if table in ("adult", "benunit", "household") and return_mdf:
                df = mdf.MicroDataFrame(df, weights="GROSS4")
        else:
            main_file = tuple((data_path).glob("frs*.tab"))
            if len(main_file) == 0:
                raise FileNotFoundError("Could not find the main table.")
            else:
                df = pd.read_csv(data_path / main_file[0], delimiter="\t", usecols=columns)
                if return_mdf:
                    df = mdf.MicroDataFrame(df, weights="GROSS4")
        return df
    else:
        raise FileNotFoundError("Could not find the data requested.")