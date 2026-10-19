from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

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


@dataclass(slots=True)
class ParityReport:
    """Result of comparing a candidate output to a baseline snapshot, keyed on ``key``."""

    key: str
    row_count_baseline: int
    row_count_candidate: int
    missing_columns: list[str]
    extra_columns: list[str]
    column_order_matches: bool
    missing_keys: list[str]
    extra_keys: list[str]
    column_diffs: pd.DataFrame = field(default_factory=pd.DataFrame)

    @property
    def failing_columns(self) -> pd.DataFrame:
        return self.column_diffs[self.column_diffs["n_over_tolerance"] > 0]

    @property
    def passed(self) -> bool:
        return (
            not self.missing_columns
            and not self.extra_columns
            and not self.missing_keys
            and not self.extra_keys
            and self.failing_columns.empty
        )


def _tolerance_for(column: str, tolerances: dict[str, float], default: float) -> float:
    for prefix, value in tolerances.items():
        if column.startswith(prefix):
            return value
    return default


def _text_key(series: pd.Series) -> pd.Series:
    # Legacy exports wrote 0 for missing text; treat 0, "0", "" and NaN as the same blank.
    return series.astype(object).where(series.notna(), "").astype(str).replace({"0": "", "0.0": ""})


def compare_to_baseline(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
    key: str,
    tolerances: dict[str, float] | None = None,
    default_tolerance: float = 1e-9,
) -> ParityReport:
    """Column-by-column parity check against a baseline snapshot.

    Numeric columns pass when every absolute difference is within the tolerance for that
    column (the first matching prefix in ``tolerances``, else ``default_tolerance``). Other
    columns must match exactly after blank normalization.
    """
    validate_columns(baseline, [key])
    validate_columns(candidate, [key])
    tolerances = tolerances or {}

    shared = [column for column in baseline.columns if column in candidate.columns]
    baseline_keys, candidate_keys = set(baseline[key]), set(candidate[key])
    common_keys = [value for value in baseline[key] if value in candidate_keys]

    left = baseline.set_index(key).loc[common_keys]
    right = candidate.set_index(key).loc[common_keys]

    rows = []
    for column in shared:
        if column == key:
            continue
        tolerance = _tolerance_for(column, tolerances, default_tolerance)
        numeric = pd.api.types.is_numeric_dtype
        if numeric(left[column]) and numeric(right[column]):
            delta = (left[column].astype(float) - right[column].astype(float)).abs()
            both_null = left[column].isna() & right[column].isna()
            delta = delta.mask(both_null, 0.0).fillna(float("inf"))
            rows.append((column, float(delta.max()), int((delta > tolerance).sum()), tolerance))
        else:
            mismatched = _text_key(left[column]) != _text_key(right[column])
            rows.append((column, float("nan"), int(mismatched.sum()), 0.0))

    return ParityReport(
        key=key,
        row_count_baseline=len(baseline.index),
        row_count_candidate=len(candidate.index),
        missing_columns=[c for c in baseline.columns if c not in candidate.columns],
        extra_columns=[c for c in candidate.columns if c not in baseline.columns],
        column_order_matches=shared == [c for c in candidate.columns if c in baseline.columns],
        missing_keys=sorted(baseline_keys - candidate_keys),
        extra_keys=sorted(candidate_keys - baseline_keys),
        column_diffs=pd.DataFrame(
            rows, columns=["column", "max_abs_diff", "n_over_tolerance", "tolerance"]
        ),
    )
