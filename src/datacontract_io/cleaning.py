# datacontract_io/cleaning.py
from collections.abc import Iterable

import pandas as pd

EMPTY_LIKE = ("", " ", "\t")


def normalize_empties(df: pd.DataFrame) -> pd.DataFrame:
    # Replace common empties with NA (string empties across all columns)
    return df.replace(list(EMPTY_LIKE), pd.NA)


def drop_all_null_rows(df: pd.DataFrame, subset: Iterable[str] | None = None) -> pd.DataFrame:
    # If subset is None, check all columns
    return df.dropna(how="all", subset=subset)


def clean_df(df: pd.DataFrame, important_cols: list[str] | None = None) -> pd.DataFrame:
    df = normalize_empties(df)
    return drop_all_null_rows(df, subset=important_cols)
