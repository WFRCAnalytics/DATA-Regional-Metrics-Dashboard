from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd


@dataclass(slots=True)
class ComparisonSummary:
    row_count_left: int
    row_count_right: int
    shared_columns: list[str]
    numeric_delta_columns: list[str]


def validate_columns(frame: pd.DataFrame, required_columns: Iterable[str]) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def compare_frames(
    left: pd.DataFrame,
    right: pd.DataFrame,
    join_field: str = "geoname",
) -> ComparisonSummary:
    validate_columns(left, [join_field])
    validate_columns(right, [join_field])

    shared_columns = sorted(set(left.columns).intersection(right.columns))
    numeric_delta_columns = []
    for column in shared_columns:
        if column == join_field:
            continue
        if (
            pd.api.types.is_numeric_dtype(left[column])
            and pd.api.types.is_numeric_dtype(right[column])
        ):
            numeric_delta_columns.append(column)

    return ComparisonSummary(
        row_count_left=len(left.index),
        row_count_right=len(right.index),
        shared_columns=shared_columns,
        numeric_delta_columns=numeric_delta_columns,
    )
