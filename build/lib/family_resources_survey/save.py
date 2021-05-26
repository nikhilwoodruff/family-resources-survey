import pandas as pd
from pathlib import Path
import os
import shutil
from tqdm import tqdm
import warnings
import json
import re

FRS_path = Path(__file__).parent


def parse_codebook(main_folder: Path) -> dict:
    """Attempts to automatically parse an Excel FRS codebook.

    Args:
        main_folder (Path): The path to the folder containing 'mrdoc' and 'tab'.

    Raises:
        FileNotFoundError: If the codebook can't be found.
        Exception: If the codebook couldn't be parsed.

    Returns:
        dict: The dictionary of descriptions for variable names.
    """
    excel_folder = main_folder / "mrdoc" / "excel"
    if excel_folder.exists():
        matches = tuple(excel_folder.glob("*hierarchical_benv_income*.xlsx"))
        if len(matches) == 0:
            raise FileNotFoundError(
                "Found the excel folder, but could not find the codebook."
            )
        else:
            try:
                codebook = dict()
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    xls = pd.ExcelFile(matches[0], engine="openpyxl")
                    df = pd.read_excel(xls, "VARIABLE LISTING")
                    for _, row in df.iterrows():
                        if row.VARIABLE not in codebook:
                            codebook[row["VARIABLE"]] = dict(
                                description=row["DESCRIPTION (SAS LABEL)"]
                            )
                    df = df.set_index(df.VARIABLE.ffill())
                    for name, x in tqdm(
                        df.groupby(df.index),
                        desc="Reading decodes for variables",
                    ):
                        for _, row in x.iterrows():
                            if not row.VALUE != row.VALUE:
                                if name not in codebook:
                                    codebook[name] = dict(
                                        description="No description provided",
                                        codemap=dict(),
                                    )
                                if "codemap" not in codebook[name]:
                                    codebook[name]["codemap"] = dict()
                                codebook[name]["codemap"][
                                    row.VALUE
                                ] = row.DECODE
                return codebook
            except:
                raise Exception("Couldn't parse the codebook.")
    else:
        raise FileNotFoundError("Could not find the excel codebook folder.")


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
        new_folder = FRS_path / "data" / "tmp"
        shutil.unpack_archive(folder, new_folder)
        folder = new_folder
    main_folder = next(folder.iterdir())
    year = str(year)
    target_folder = FRS_path / "data" / year / "raw"
    if os.path.exists(target_folder):
        # Overwrite
        shutil.rmtree(target_folder)
    os.makedirs(target_folder)

    # Look for the codebook.

    try:
        codebook = parse_codebook(main_folder)
        with open(FRS_path / "data" / year / "codebook.json", "w+") as f:
            json.dump(codebook, f)
    except:
        print("Couldn't automatically parse the codebook.")

    # Save the data.

    if (main_folder / "tab").exists():
        data_folder = main_folder / "tab"
        criterion = re.compile("[a-z]+\.tab")
        data_files = [
            path
            for path in data_folder.iterdir()
            if criterion.match(path.name)
        ]
        task = tqdm(data_files, desc="Saving data tables")
        for i, filepath in enumerate(task):
            task.set_description(f"Saving {filepath.name}")
            table_name = filepath.name.replace(".tab", "")
            df = pd.read_csv(filepath, delimiter="\t", low_memory=False).apply(
                pd.to_numeric, errors="coerce"
            )
            df.to_csv(target_folder / (table_name + ".csv"), index=False)
            if i == len(task) - 1:
                task.set_description("Saved all tables")
    else:
        raise FileNotFoundError("Could not find the TAB files.")

    # Clean up tmp storage.

    if (FRS_path / "data" / "tmp").exists():
        shutil.rmtree(FRS_path / "data" / "tmp")
