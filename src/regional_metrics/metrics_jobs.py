from __future__ import annotations

import re

import pandas as pd

from .models import MetricConfig


def metric_jobs_by(metric: MetricConfig, source_df: pd.DataFrame) -> pd.DataFrame:
    work_df = source_df.copy()
    df_fields = list(metric.geog_fields)

    for geog_area in metric.geog_areas:
        for query_field in geog_area.query_fields:
            if query_field not in df_fields:
                df_fields.append(query_field)

    key_fields = _matched_fields(work_df.columns, metric.key_field_pattern)
    weighted_fields = _matched_fields(work_df.columns, metric.weighted_field_pattern or "")

    df_fields.extend(field_name for field_name in key_fields if field_name not in df_fields)
    df_fields.extend(field_name for field_name in weighted_fields if field_name not in df_fields)
    work_df = work_df[df_fields]

    out_fields = ["geoname"]
    for index, _field_name in enumerate(key_fields):
        out_fields.append(f"{metric.out_field_pattern}{'CY' if index == 0 else f'FY{index}'}")

    frames: list[pd.DataFrame] = []
    for geog_field in metric.geog_fields:
        geog_frame = _calculate_weighted_frame(
            frame=work_df,
            geog_name_field=geog_field,
            key_fields=key_fields,
            weighted_fields=weighted_fields,
            weighted_field_prefix=metric.weighted_field_prefix or "",
            out_fields=out_fields,
        )
        frames.append(geog_frame)

    for geog_area in metric.geog_areas:
        area_df = work_df.query(geog_area.query).copy()
        weighted_totals = area_df[weighted_fields].sum(numeric_only=True)
        rows = []
        for _, row in area_df.iterrows():
            values = [geog_area.geog_name]
            for key_field in key_fields:
                year = key_field[-2:]
                numerator = row[f"{metric.weighted_field_prefix}{year}"]
                denominator = weighted_totals[f"{metric.weighted_field_prefix}{year}"]
                values.append(0 if denominator == 0 else row[key_field] * (numerator / denominator))
            rows.append(values)

        geog_df = pd.DataFrame(rows, columns=out_fields)
        geog_df["geoname"] = geog_df["geoname"].astype(str).str.title()
        frames.append(geog_df.groupby("geoname", as_index=False).sum(numeric_only=True))

    return pd.concat(frames, ignore_index=True)


def _calculate_weighted_frame(
    frame: pd.DataFrame,
    geog_name_field: str,
    key_fields: list[str],
    weighted_fields: list[str],
    weighted_field_prefix: str,
    out_fields: list[str],
) -> pd.DataFrame:
    weighted_df = (
        frame[[geog_name_field, *weighted_fields]]
        .groupby(geog_name_field)
        .sum(numeric_only=True)
    )
    merge_df = frame.merge(weighted_df, left_on=geog_name_field, right_index=True)

    rows = []
    for _, row in merge_df.iterrows():
        values = [row[geog_name_field]]
        for key_field in key_fields:
            year = key_field[-2:]
            numerator = row[f"{weighted_field_prefix}{year}_x"]
            denominator = row[f"{weighted_field_prefix}{year}_y"]
            values.append(0 if denominator == 0 else row[key_field] * (numerator / denominator))
        rows.append(values)

    geog_df = pd.DataFrame(rows, columns=out_fields)
    geog_df["geoname"] = geog_df["geoname"].astype(str).str.title()
    return geog_df.groupby("geoname", as_index=False).sum(numeric_only=True)


def _matched_fields(columns, pattern: str) -> list[str]:
    return sorted(
        (column for column in columns if re.match(pattern, str(column))),
        key=_field_sort_key,
    )


def _field_sort_key(field_name: str) -> tuple[int, str]:
    token = re.search(r"([0-9]{2})$", field_name)
    return (int(token.group(1)) if token else 0, field_name)
