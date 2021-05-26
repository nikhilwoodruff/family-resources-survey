import json
import pandas as pd
from typing import Union, List
from family_resources_survey.save import FRS_path


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


class FRS:
    def __init__(self, year: int, add_entity_ids=True):
        self.year = year
        self.tables = {}
        self.add_entity_ids = add_entity_ids
        self.data_path = FRS_path / "data" / str(year)
        codebook_path = self.data_path / "codebook.json"
        self.variables = {}
        if not self.data_path.exists():
            raise FileNotFoundError(f"No data for the year {year}.")
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
        return self.tables[name]

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
