from __future__ import annotations

import re

import pandas as pd

from .models import MetricConfig


def aggregate_metric_table(metric: MetricConfig, source_df: pd.DataFrame) -> pd.DataFrame:
    work_df = source_df.copy()
    df_fields = list(metric.geog_fields)

    for geog_area in metric.geog_areas:
        for query_field in geog_area.query_fields:
            if query_field not in df_fields:
                df_fields.append(query_field)

    key_fields = _matched_fields(work_df.columns, metric.key_field_pattern)
    for field_name in key_fields:
        if field_name not in df_fields:
            df_fields.append(field_name)

    work_df = work_df[df_fields]
    rename_dict = {
        field_name: f"{metric.out_field_pattern}{_year_token(field_name)}"
        for field_name in key_fields
    }
    frames: list[pd.DataFrame] = []

    for geog_field in metric.geog_fields:
        geog_df = work_df.copy()
        geog_df["geoname"] = geog_df[geog_field]
        grouped = _aggregate_frame(
            geog_df,
            group_field=geog_field,
            geoname_field="geoname",
            key_fields=key_fields,
            aggregation=metric.aggregation,
            rename_dict=rename_dict,
        )
        frames.append(grouped)

    for geog_area in metric.geog_areas:
        area_df = work_df.query(geog_area.query).copy()
        area_df["geoname"] = geog_area.geog_name
        grouped = _aggregate_frame(
            area_df,
            group_field="geoname",
            geoname_field="geoname",
            key_fields=key_fields,
            aggregation=metric.aggregation,
            rename_dict=rename_dict,
        )
        frames.append(grouped)

    if not frames:
        return pd.DataFrame(columns=["geoname"])

    return pd.concat(frames, ignore_index=True)


def _aggregate_frame(
    frame: pd.DataFrame,
    group_field: str,
    geoname_field: str,
    key_fields: list[str],
    aggregation: str,
    rename_dict: dict[str, str],
) -> pd.DataFrame:
    groupby_dict = {field_name: aggregation for field_name in key_fields}
    groupby_dict[geoname_field] = "first"

    grouped = frame.groupby(by=group_field, dropna=False).agg(groupby_dict)
    grouped.rename(columns=rename_dict, inplace=True)
    drop_columns = [
        column
        for column in grouped.columns
        if column not in rename_dict.values() and column != geoname_field
    ]
    grouped.drop(columns=drop_columns, inplace=True)

    geoname = grouped.pop(geoname_field)
    grouped.insert(0, "geoname", geoname.astype(str).str.title())
    return grouped.reset_index(drop=True)


def _matched_fields(columns, pattern: str) -> list[str]:
    return sorted(
        (column for column in columns if re.match(pattern, str(column))),
        key=_field_sort_key,
    )


def _field_sort_key(field_name: str) -> tuple[int, str]:
    token = re.search(r"([0-9]{2,4})$", field_name)
    return (int(token.group(1)) if token else 0, field_name)


def _year_token(field_name: str) -> str:
    token = re.search(r"([0-9]{4})$", field_name)
    if token:
        return token.group(1)
    token = re.search(r"([0-9]{2})$", field_name)
    if token:
        return token.group(1)
    return field_name
