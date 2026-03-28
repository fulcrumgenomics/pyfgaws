"""Utility functions for pyfgaws."""

from typing import List


def column_it(rows: List[List[str]], delimiter: str = " ") -> str:
    """A simple version of Unix's ``column`` utility.  Assumes the table is NxM.

    Args:
        rows: the rows to adjust.  Each row must have the same number of fields.
        delimiter: the delimiter for each field in a row.
    """
    num_columns = len(rows[0])
    max_column_lengths: List[int] = [
        max(len(row[col_i]) for row in rows) for col_i in range(num_columns)
    ]
    return "\n".join(
        delimiter.join(
            (" " * (max_column_lengths[col_i] - len(row[col_i]))) + row[col_i]
            for col_i in range(num_columns)
        )
        for row in rows
    )
