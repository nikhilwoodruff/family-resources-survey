from typing import Tuple
import pandas as pd
from functools import reduce

def consolidate(
    table: pd.DataFrame, max_count: int = 1, count_label: str = None
) -> pd.DataFrame:
    """Converts a table containing multiple entries per person into a table with numbered repetitions of each column for each entry, limited to a maximum, with excess entries summed into an 'other' column.

    Args:
        table (pd.DataFrame): The table containing multiple entries.
        max_count (int): The maximum number of entries (includes the 'other' column).
        count_label (str): A column to fill with the number of entries.

    Returns:
        pd.DataFrame: The converted table.
    """
    gb = table.groupby(table.index)
    max_number = gb.size().max()
    df = pd.DataFrame(index=table.index)
    for i in range(max_count - 1):
        df = df.join(gb.nth(i).add_suffix(f"_{i + 1}"))
    other_jobs = tuple(range(max_count - 1, max_number))
    remaining = gb.nth(other_jobs)
    suffix = ("_other", "")[max_count == 1]
    df = df.join(remaining.groupby(remaining.index).sum().add_suffix(suffix))
    if count_label is not None:
        df[count_label] = gb.size()
    return df.fillna(0)

def join_2_tables(df1 : pd.DataFrame, df2 : pd.DataFrame) -> pd.DataFrame:
    """Joins two tables.
    """
    return df1.join(df2[df2.columns.difference(df1.columns)])


def join(*tables : Tuple[pd.DataFrame]) -> pd.DataFrame:
    """Joins the tables given.
    """
    return reduce(join_2_tables, tables)