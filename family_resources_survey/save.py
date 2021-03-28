import pandas as pd
from pathlib import Path
import os
import shutil
from tqdm import tqdm

FRS = Path(__file__).parent


table_entities = dict(
    person=[
        "accounts",
        "adult",
        "assets",
        "benefits",
        "care",
        "child",
        "chldcare",
        "govpay",
        "job",
        "maint",
        "oddjob",
        "penprov",
        "pension"
    ],
    benunit=[
        "benunit",
    ],
    household=[
        "endowmnt",
        "extchild",
        "househol",
        "mortcont",
        "mortgage",
        "owner",
        "rentcont",
        "renter"
    ]
)

table_to_entity = {}

for entity, tables in table_entities.items():
    for table_name in tables:
        table_to_entity[table_name] = entity

ADMIN_COLUMNS = [
    'sernum',
    'BENUNIT',
    'PERSON',
    'ISSUE', 
    'MONTH'
]


def person_id(df: pd.DataFrame) -> pd.Series:
    if "PERSON" in df.columns:
        person_col = df.PERSON
    elif "NEEDPER" in df.columns:
        person_col = df.NEEDPER
    return df.sernum * 1e2 + df.BENUNIT + person_col


def benunit_id(df: pd.DataFrame) -> pd.Series:
    return df.sernum * 1e2 + df.BENUNIT


def household_id(df: pd.DataFrame) -> pd.Series:
    return df.sernum


def index_table(table, entity_name):
    if entity_name == "person":
        table["person_id"] = person_id(table)
        table["benunit_id"] = benunit_id(table)
        table["household_id"] = household_id(table)
    elif entity_name == "benunit":
        table["benunit_id"] = benunit_id(table)
        table["household_id"] = household_id(table)
    elif entity_name == "household":
        table["household_id"] = household_id(table)
    return table.set_index(f"{entity_name}_id").drop(ADMIN_COLUMNS, axis=1, errors="ignore")


def save(folder: str, year: int, zipped: bool = True) -> None:
    """Save the FRS microdata to the package internal storage.

    Args:
        folder (str): A path to the (zipped or unzipped) folder downloaded from the UK Data Archive.
        year (int): The year to store the microdata as.
        zipped (bool, optional): Whether the folder given is zipped. Defaults to True.

    Raises:
        FileNotFoundError: If an invalid path is given.
    """

    # Get the folder ready.

    folder = Path(folder)
    if not os.path.exists(folder):
        raise FileNotFoundError("Invalid path supplied.")
    if zipped:
        new_folder = FRS / "data" / "tmp"
        shutil.unpack_archive(folder, new_folder)
        folder = new_folder
    main_folder = next(folder.iterdir())
    year = str(year)
    target_folder = FRS / "data" / year / "raw"
    if os.path.exists(target_folder):
        # Overwrite
        shutil.rmtree(target_folder)
    os.makedirs(target_folder)

    # Index the data and save.

    if (main_folder / "tab").exists():
        data_folder = main_folder / "tab"
        data_files = list(data_folder.glob("*.tab"))
        task = tqdm(data_files, desc="Indexing data tables")
        for filepath in task:
            task.set_description(f"Indexing {filepath.name}")
            table_name = filepath.name.replace(".tab", "")
            if table_name in table_to_entity:
                entity_name = table_to_entity[table_name]
                df = pd.read_csv(filepath, delimiter="\t", low_memory=False)
                df = index_table(df, entity_name)
                df.to_csv(target_folder / (table_name + ".csv"))
    else:
        raise FileNotFoundError("Could not find the TAB files.")

    # Clean up tmp storage.

    if (FRS / "data" / "tmp").exists():
        shutil.rmtree(FRS / "data" / "tmp")
