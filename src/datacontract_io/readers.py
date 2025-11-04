# datacontract_io/readers.py
from __future__ import annotations

from typing import Protocol

import pandas as pd

from .cleaning import clean_df
from .contracts import DataContract


class Reader(Protocol):
    def read(self, path: str) -> pd.DataFrame: ...


class BaseReader:
    def __init__(self, contract: DataContract):
        self.contract = contract

    # ---------- Core I/O ----------
    def _read_raw(self, path: str) -> pd.DataFrame:
        fmt = (self.contract.format or "").upper()
        if fmt == "CSV":
            read_fn = pd.read_csv
            read_kwargs = {
                "dtype": str,
                "encoding": self.contract.encoding,
                "delimiter": self.contract.csv_delimiter(),
            }
        elif fmt == "XLSX":
            read_fn = pd.read_excel
            # encoding/delimiter do not apply to Excel
            read_kwargs = {"dtype": str}
        else:
            raise ValueError(f"Unsupported format: {self.contract.format}")

        # If there are N non-data rows before the header, skip them,
        # then set header=0 so the next row becomes the header.
        if self.contract.skip_header_rows > 0:
            read_kwargs["skiprows"] = self.contract.skip_header_rows
        read_kwargs["header"] = 0

        return read_fn(path, **read_kwargs)

    # ---------- Column normalization & cleaning ----------
    def _normalize_columns_by_index(self, df: pd.DataFrame) -> pd.DataFrame:
        # Safer check: explicitly test for None (not truthiness)
        if any(c.source_column_index is not None for c in self.contract.columns):
            cols = [
                col.lstrip("\ufeff") if isinstance(col, str) else col  # strip BOM if present
                for col in df.columns
            ]
            for c in self.contract.columns:
                if c.source_column_index is None:
                    continue
                src_pos = int(c.source_column_index)  # contract is 0-based
                if not (0 <= src_pos < len(cols)):
                    raise ValueError(
                        f"source_column_index out of range for column {c.name} "
                        f"(got index {c.source_column_index}, have {len(cols)} columns)"
                    )
                # Rename by position (does not rely on possibly duplicated/dirty header text)
                cols[src_pos] = c.name
            df.columns = cols
        return df

    def _post_process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Post-process: normalize columns, clean data, then apply footer skip."""
        df = self._normalize_columns_by_index(df)
        important = [c.name for c in self.contract.columns]
        df = clean_df(df, important_cols=important)

        # Apply footer skip AFTER cleaning
        n_footer = int(getattr(self.contract, "skip_footer_rows", 0) or 0)
        if n_footer > 0:
            n = len(df)
            df = df.iloc[0:0] if n_footer > n else df.iloc[: n - n_footer]

        return df

    # ---------- Public API ----------
    def read(self, path: str) -> pd.DataFrame:
        return self._post_process(self._read_raw(path))


# Keep class names for compatibility; they inherit the unified logic.
class CSVReader(BaseReader):
    pass


class ExcelReader(BaseReader):
    pass


def make_reader(contract: DataContract) -> BaseReader:
    fmt = (contract.format or "").upper()
    if fmt == "CSV":
        return CSVReader(contract)
    if fmt == "XLSX":
        return ExcelReader(contract)
    raise ValueError(f"Unsupported format: {contract.format}")
