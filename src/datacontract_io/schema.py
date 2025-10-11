from __future__ import annotations

import numpy as np
import pandas as pd

from .contracts import ColumnSpec, DataContract
from .errors import SchemaError


def _cast_decimal_series(s: pd.Series, comma_based: bool) -> pd.Series:
    if comma_based:
        s = s.str.replace(".", "", regex=False)  # тысячные-точки убрать, если вдруг
        s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _cast_bool_series(s: pd.Series) -> pd.Series:
    true_set = {"true", "t", "1", "y", "yes", "да"}
    false_set = {"false", "f", "0", "n", "no", "нет"}

    def _m(x: object) -> bool | pd.NAType:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return pd.NA
        xs = str(x).strip().lower()
        if xs in true_set:
            return True
        if xs in false_set:
            return False
        return pd.NA

    return s.map(_m)


def _needs_comma_decimal(col: ColumnSpec) -> bool:
    fmt = (col.format or "").lower()
    return "decimal with comma" in fmt or "comma" in fmt


def _cast_col(df: pd.DataFrame, col: ColumnSpec) -> pd.Series:
    s = df[col.name]

    if col.type in ("varchar", "string", "text"):
        result = s.astype("string")
    elif col.type in ("int", "bigint", "integer"):
        result = pd.to_numeric(s, errors="coerce").astype("Int64")
    elif col.type in ("float", "double", "decimal", "numeric"):
        if col.type in ("decimal", "numeric"):
            result = _cast_decimal_series(s.astype("string"), comma_based=_needs_comma_decimal(col))
        else:
            result = pd.to_numeric(s, errors="coerce")
    elif col.type in ("date", "timestamp", "datetime"):
        fmt = col.format
        dayfirst = ("-%m-" not in fmt and "%d" in fmt) if fmt else True
        result = pd.to_datetime(s, format=fmt, errors="coerce", dayfirst=dayfirst)
    elif col.type in ("bool", "boolean"):
        result = _cast_bool_series(s)
    else:
        # fallback
        result = s.astype("string")

    return result


def enforce_schema(df: pd.DataFrame, contract: DataContract) -> pd.DataFrame:
    expected = contract.expected_columns()
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise SchemaError(f"Отсутствуют столбцы: {missing}")

    # Оставляем только ожидаемые и упорядочиваем
    df = df[expected].copy()

    # Приведение типов по контракту
    for col in contract.columns:
        df[col.name] = _cast_col(df, col)

    # Проверка NOT NULL
    non_nullable = [c.name for c in contract.columns if not c.nullable]
    if non_nullable:
        bad_mask = df[non_nullable].isna().any(axis=1)
        if bad_mask.any():
            idx = df.index[bad_mask].tolist()[:10]
            raise SchemaError(f"Найдены NULL в not-null полях {non_nullable}. Примеры строк: {idx}")

    return df
