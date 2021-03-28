import pandas as pd
from pathlib import Path
import os
import shutil

FRS = Path(__file__).parent


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

    # Find the data type and extract.

    if (main_folder / "sas").exists():
        data_folder = main_folder / "sas"
        data_files = data_folder.glob("*.sas7bdat")
    elif (main_folder / "tab").exists():
        data_folder = main_folder / "tab"
        data_files = data_folder.glob("*.tab")
    for filepath in data_files:
        shutil.copyfile(filepath, target_folder / filepath.name)

    # Clean up tmp storage.

    if (FRS / "data" / "tmp").exists():
        shutil.rmtree(FRS / "data" / "tmp")
