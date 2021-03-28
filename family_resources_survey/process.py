import pandas as pd


def person_id(df: pd.DataFrame) -> pd.Series:
    return df.sernum * 1e2 + df.BENUNIT + df.PERSON


def benunit_id(df: pd.DataFrame) -> pd.Series:
    return df.sernum * 1e2 + df.BENUNIT


def household_id(df: pd.DataFrame) -> pd.Series:
    return df.sernum


def expand_multiple_occurrences(
    table: pd.DataFrame, id_col: str, max_count: int, count_label: str
) -> pd.DataFrame:
    """Converts a table containing multiple entries per person into a table with numbered repetitions of each column for each entry, limited to a maximum, with excess entries summed into an 'other' column.

    Args:
        table (pd.DataFrame): The table containing multiple entries.
        id_col (str): The identity column to join on.
        max_count (int): The maximum number of entries (includes the 'other' column).
        count_label (str): A column to fill with the number of entries.

    Returns:
        pd.DataFrame: The converted table.
    """
    gb = table.groupby(id_col)
    max_number = gb.size().max()
    df = table[[id_col]].set_index(id_col)
    for i in range(max_count - 1):
        df = df.join(gb.nth(i), rsuffix=f"_{i + 1}")
    other_jobs = tuple(range(max_count - 1, max_number))
    df = df.join(gb.nth(other_jobs).groupby("id").sum(), rsuffix="_other")
    df[count_label] = gb.size()
    return df.fillna(0)
