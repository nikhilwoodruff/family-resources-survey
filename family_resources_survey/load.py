import json
import pandas as pd
from typing import Union, List
from family_resources_survey.save import FRS_path
import yaml
import warnings

def load(
    year: int,
    table: str,
    columns: List[str] = None,
    add_entity_ids: bool = True
) -> pd.DataFrame:
    year = str(year)
    data_path = FRS_path / "data" / year / "raw"
    if data_path.exists():
        if table is not None:
            df = pd.read_csv(
                data_path / (table + ".csv"), usecols=columns, low_memory=False
            )
            if add_entity_ids:
                if "PERSON" in df.columns:
                    df["person_id"] = df.sernum * 1e+2 + df.BENUNIT * 1e+1 + df.PERSON
                if "BENUNIT" in df.columns:
                    df["benunit_id"] = df.sernum * 1e+2 + df.BENUNIT * 1e+1
                if "sernum" in df.columns:
                    df["household_id"] = df.sernum * 1e+2
        return df
    else:
        raise FileNotFoundError("Could not find the data requested.")

class Uprating:
    affected_by = {
        "labour_income": ["INEARNS", "NINEARNS", "UGRSPAY", "SEINCAM2"]
    }

    def __init__(self, base_year: int = None, target_year: int = None):
        if base_year is not None and target_year is not None:
            self.empty = False
            self.multipliers = {}

            with open(FRS_path / "uprating.yaml") as f:
                parameters = yaml.safe_load(f)

            for variable in ("labour_income",):
                if variable not in parameters:
                    raise Exception(f"Uprating parameters do not contain {variable}")
                if base_year not in parameters[variable]:
                    raise Exception(f"Uprating parameters do not contain the rate for {base_year} for {variable}")
                if target_year not in parameters[variable]:
                    raise Exception(f"Uprating parameters do not contain the rate for {target_year} for {variable}")
                self.multipliers[variable] = parameters[variable][target_year] / parameters[variable][base_year]
        else:
            self.empty = True

    def __call__(self, table: pd.DataFrame) -> pd.DataFrame:
        table = table.copy(deep=True)
        if self.empty:
            return table
        for variable in self.multipliers:
            for affected_variable in self.affected_by[variable]:
                if affected_variable in table.columns:
                    table[affected_variable] *= self.multipliers[variable]
        return table

class FRS:
    def __init__(self, year: int, add_entity_ids=True):
        year = int(year)
        self.year = year
        self.tables = {}
        self.add_entity_ids = add_entity_ids
        self.data_path = FRS_path / "data" / str(year)
        codebook_path = self.data_path / "codebook.json"
        self.variables = {}
        self.uprater = Uprating()
        if not self.data_path.exists():
            available_years = list(map(lambda path: int(path.name), (FRS_path / "data").iterdir()))
            if len(available_years) == 0:
                raise FileNotFoundError(f"No FRS years stored.")
            try:
                base_year = available_years[-1]
                self.uprater = Uprating(base_year, year)
                self.year = base_year
            except Exception as e:
                raise Exception(f"No data for {year} stored, and uprating failed: {e}")
        if codebook_path.exists():
            with open(codebook_path, "r") as f:
                codebook = json.load(f)
                self.variables = {}
                for var in codebook:
                    self.variables[var] = FRSVariable()
                    if "description" in codebook[var]:
                        self.variables[var].description = codebook[var][
                            "description"
                        ]
                    if "codemap" in codebook[var]:
                        self.variables[var].codemap = codebook[var]["codemap"]

    def __getattr__(self, name: str) -> pd.DataFrame:
        if name == "description":
            return self.description
        if name not in self.tables:
            self.tables[name] = load(self.year, name, add_entity_ids=self.add_entity_ids)
        return self.uprater(self.tables[name])

    @property
    def table_names(self):
        return list(
            map(
                lambda p: p.name.split(".csv")[0],
                (self.data_path / "raw").iterdir(),
            )
        )


class FRSVariable:
    description = "No description provided"
    codemap = {}

    def __getitem__(self, encoded: Union[str, int]) -> Union[str, int, float]:
        if encoded in self.codemap:
            return self.codemap[encoded]
        else:
            return None

    def __repr__(self):
        short_desc = self.description[:60] + (
            "..." if len(self.description) > 60 else ""
        )
        return f'<FRS Variable, description = "{short_desc}" ({len(self.codemap)} categories)>'
