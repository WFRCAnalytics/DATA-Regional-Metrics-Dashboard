"""
calc_metrics.py
Brooke Reams - breams@esri.com
Feb. 23, 2024

Modified by: jreynolds@wfrc.org

Modified by: pukar.bhandari@wfrc.utah.gov on 03/20/2026

Script written for WFRC to calculate "Big 5 Metrics" consumed by Dashboard

---

Note: This script must be run in an arcpy-enabled environment, otherwise field names will be truncated

The script obtains the names (e.g Clearfield, Utah County North,) from each geography field provided, sums the data values,
then performs an outer join with those names and the names from the boundary layer feature service. This means that
the field name does not matter as long as the actual geog name values match those in the boundary layer, otherwise
the geog name will not be assigned a geo type or have an associated geometry.


The GIS API object is accessed by a specified username and password from the keyring module


"inputs" dictionary keys:
- itemID: the item's id from ArcGIS Online
- index: the layer number. This is usually zero unless the item is a gdb
- query: (optional) initial subset of the data
- geogFields: Fields that the dataset will be summarized to. Note the boundary layer
    currently only contains city area, small area, county, and region
- geogAreas: new geographies that are summarized using the statement shown
- keyFieldPattern: the fields that currently contain the data
- outFieldPattern: the new fields that will be created and added to the train;
    each layer must have a unique pattern

To add data from a new table, the code must be updated in three places.

"""

import datetime
import logging
import os
import re
import sys

import arcgis
import keyring
import pandas as pd


def logIt(message):
    print(message)
    logging.info(message)


def getFeatureLayerFromItemId(gis, item_id):
    item = gis.content.get(item_id)
    fl = item.layers[0]

    return fl


def getFeatureLayerFromItemIdandIndex(gis, item_id, index):
    """
    Same as the original function but also ingests an index.
    For datasets that have multiple layers
    """
    item = gis.content.get(item_id)
    fl = item.layers[index]

    return fl


def mergeMetricDataframes(output_df, metric_df):
    # Merge metric dataframes
    if not output_df.empty:
        output_df = output_df.merge(metric_df, how="outer", on="geoname")
    else:
        output_df = metric_df

    return output_df


def metricJobsBy(gis, metric_name, input):
    logIt(f"Current metric: {metric_name}")
    # Get fs item from portal
    ato_fl = getFeatureLayerFromItemId(gis, input["itemId"])

    # Convert layer to pandas dataframe
    ato_df = ato_fl.query(where=input["query"]).sdf
    # Get geog fields from dataframe
    df_flds = input["geogFields"][:]

    # Get geog area query fields from dataframe
    for d in input["geogAreas"]:
        for fld in d["queryFields"]:
            if fld not in df_flds:
                df_flds.append(fld)
    # Get key fields from dataframe
    key_flds = [i for i in ato_df.columns if re.match(input["keyFieldPattern"], i)]
    df_flds.extend(key_flds)
    # Get weighted fields from dataframe
    weighted_flds = [i for i in ato_df.columns if re.match(input["weightedFieldPattern"], i)]
    df_flds.extend(weighted_flds)

    # Set only required fields in dataframe
    ato_df = ato_df[df_flds]

    # Get list of output table fields
    out_flds = ["geoname"]
    for i in range(len(key_flds)):
        fld = key_flds[i]
        if i > 0:
            out_flds.append(input["outFieldPattern"] + f"FY{i}")
        else:
            out_flds.append(input["outFieldPattern"] + "CY")

    # Array to store all geography dataframes
    all_df_list = []

    # Perform metric calc on each geography field
    for fld_geog in input["geogFields"]:
        logIt(f"Calculating metric for: {fld_geog}")
        # Create subset dataframe with geog and weighted fields
        geog_weighted_flds = [fld_geog]
        geog_weighted_flds.extend(weighted_flds)

        # Create df of weighted data
        weighted_df = ato_df[geog_weighted_flds].groupby(fld_geog).sum()

        # Merge weighted dataframe to main dataframe
        merge_df = ato_df.merge(weighted_df, left_on=fld_geog, right_on=fld_geog)

        # Caclulate metrics in merged dataframe
        geog_list = []
        for ind in merge_df.index:
            value_list = [merge_df[fld_geog][ind]]
            for key_fld in key_flds:
                # Get two-digit yeaer
                yr = key_fld[-2:]
                weighted_x_fld = input["weightedFieldPrefix"] + yr + "_x"
                weighted_y_fld = input["weightedFieldPrefix"] + yr + "_y"
                if merge_df[weighted_y_fld][ind] != 0:
                    weighted_value = merge_df[key_fld][ind] * (
                        merge_df[weighted_x_fld][ind] / merge_df[weighted_y_fld][ind]
                    )
                    value_list.append(weighted_value)
                else:
                    value_list.append(0)
            geog_list.append(value_list)

        # Create dataframe of geog and weighted values
        geog_df = pd.DataFrame(geog_list, columns=out_flds)
        geog_df["geoname"] = geog_df["geoname"].str.title()
        geog_df = geog_df.groupby("geoname").sum()
        # logIt(geog_df.head())
        all_df_list.append(geog_df)

    # Perform metric calc on each geog area (by query)
    for geog_area in input["geogAreas"]:
        geog_name = geog_area["geogName"]
        logIt(f"Calculating metric for: {geog_name}")
        query = geog_area["query"]
        area_df = ato_df.query(query)

        # Create weighted dataframe
        weighted_df = area_df[geog_weighted_flds].sum(numeric_only=True)

        geog_list = []
        for ind in area_df.index:
            value_list = [geog_name]
            for key_fld in key_flds:
                # Get two-digit year
                yr = key_fld[-2:]
                weighted_x_fld = input["weightedFieldPrefix"] + yr
                sum_weighted_fld = weighted_df[input["weightedFieldPrefix"] + yr]
                weighted_value = area_df[key_fld][ind] * (
                    area_df[weighted_x_fld][ind] / sum_weighted_fld
                )
                value_list.append(weighted_value)
            geog_list.append(value_list)

        # Create dataframe of geog and weighted values
        geog_df = pd.DataFrame(geog_list, columns=out_flds)
        geog_df["geoname"] = geog_df["geoname"].str.title()
        geog_df.set_index("geoname", inplace=True)
        geog_df = geog_df.groupby("geoname").sum()
        logIt(geog_df.head())
        all_df_list.append(geog_df)

    # Concatenate all weighted geog dataframes
    metric_df = pd.concat(all_df_list)

    return metric_df


def metricEstimatesProjections(gis, metric_name, input):
    logIt(f"Current metric: {metric_name}")
    # Get fs item from portal
    ato_fl = getFeatureLayerFromItemIdandIndex(gis, input["itemId"], input["index"])
    logIt(ato_fl)

    # Convert layer to pandas dataframe
    ato_df = ato_fl.query(where=input["query"]).sdf
    # Get geog fields from dataframe
    df_flds = input["geogFields"][:]

    # Get geog area query fields from dataframe
    for d in input["geogAreas"]:
        for fld in d["queryFields"]:
            if fld not in df_flds:
                df_flds.append(fld)

    # Get key fields from dataframe
    key_flds = [i for i in ato_df.columns if re.match(input["keyFieldPattern"], i)]
    df_flds.extend(key_flds)

    ato_df = ato_df[df_flds]

    # Get dictionary of out table fields
    rename_dict = {}
    for fld in key_flds:
        yr = fld[-4:]
        rename_dict[fld] = input["outFieldPattern"] + yr

    # Array to store all geography dataframes
    all_df_list = []

    # Perform metric calc on each geography
    for fld_geog in input["geogFields"]:
        logIt(f"Calculating metric for: {fld_geog}")
        ato_df["geoname"] = ato_df[fld_geog]

        groupby_dict = {}
        for fld in ato_df.columns:
            if re.match(input["keyFieldPattern"], fld):
                groupby_dict[fld] = input["aggregation"]
            elif fld == "geoname":
                groupby_dict[fld] = "first"

        sum_df = ato_df.groupby(by=fld_geog).agg(groupby_dict)
        sum_df.rename(columns=rename_dict, inplace=True)

        drop_list = []
        for column in sum_df.columns:
            if column not in rename_dict.values() and column != "geoname":
                drop_list.append(column)
        sum_df.drop(columns=drop_list, inplace=True)

        first_col = sum_df.pop("geoname")
        sum_df.insert(0, "geoname", first_col)

        sum_df["geoname"] = sum_df["geoname"].str.title()
        logIt(sum_df.head())
        all_df_list.append(sum_df)

    # Perform metric calc on each geog area
    if len(input["geogAreas"]) > 0:
        for geog_area in input["geogAreas"]:
            geog_name = geog_area["geogName"]
            logIt(f"Calculating metric for: {geog_name}")

            ato_df["geoname"] = geog_area["geogName"]
            query = geog_area["query"]
            area_df = ato_df.query(query)

            groupby_dict = {}
            drop_columns = []
            for fld in area_df.columns:
                if re.match(input["keyFieldPattern"], fld):
                    groupby_dict[fld] = input["aggregation"]
                elif fld == "geoname":
                    groupby_dict[fld] = "first"
                else:
                    drop_columns.append(fld)
            area_df = area_df.drop(columns=drop_columns)

            sum_df = area_df.groupby(by="geoname").agg(groupby_dict)
            sum_df.rename(columns=rename_dict, inplace=True)
            sum_df["geoname"] = sum_df["geoname"].str.title()
            logIt(sum_df.head())

            all_df_list.append(sum_df)

    # Concatenate all weighted geog dataframes
    metric_df = pd.concat(all_df_list)

    return metric_df


def metricWeightedMean(gis, metric_name, input):
    """
    Aggregates cost or ratio fields using a household-weighted mean.

    Used for HCOST{YYYY}, TCOST{YYYY}, and HPLUST{YYYY} — all three are per-household
    metrics and must be weighted by total households (TOTHH{YYYY}) when rolling up
    to county or region level.  A simple arithmetic mean is wrong because it treats a
    city of 500 households the same as one with 50,000.

        metric_county = Σ(metric_city × TOTHH_city) / Σ(TOTHH_city)

    Cities with no household data (TOTHH = 0 or NaN) are excluded from both the
    numerator and denominator, so they do not distort the weighted result.

    At the CITYAREA level each city is a single row, so the weighted mean equals the
    city value exactly — no change in behaviour for city-level geographies.

    Requires the input dict to include:
      "weightFieldPattern"  — regex matching TOTHH{YYYY} columns in the source layer
                              (e.g. "^TOTHH[0-9]{4}$")
    """
    logIt(f"Current metric: {metric_name}")

    # Fetch source feature layer
    ato_fl = getFeatureLayerFromItemIdandIndex(gis, input["itemId"], input["index"])
    logIt(ato_fl)
    ato_df = ato_fl.query(where=input["query"]).sdf

    # Identify value fields and weight fields (TOTHH{YYYY})
    key_flds = sorted([c for c in ato_df.columns if re.match(input["keyFieldPattern"], c)])
    weight_flds = [c for c in ato_df.columns if re.match(input["weightFieldPattern"], c)]

    # Map 4-digit year string → weight column: "2024" → "TOTHH2024"
    year_to_weight = {wf[-4:]: wf for wf in weight_flds}

    # Collect all fields needed in the working dataframe
    df_flds = input["geogFields"][:]
    for d in input["geogAreas"]:
        for fld in d["queryFields"]:
            if fld not in df_flds:
                df_flds.append(fld)
    df_flds.extend(key_flds)
    df_flds.extend(weight_flds)
    df_flds = list(dict.fromkeys(df_flds))  # deduplicate, preserve order
    ato_df = ato_df[df_flds]

    # Output column names: HPLUST2024 → HPLUST_2024
    rename_dict = {fld: input["outFieldPattern"] + fld[-4:] for fld in key_flds}

    def weighted_mean_for_group(group_df):
        """Return one output row dict with population-weighted HPLUST values."""
        row = {}
        for kf in key_flds:
            yr = kf[-4:]
            wf = year_to_weight.get(yr)
            if wf and wf in group_df.columns:
                weights = group_df[wf].fillna(0)
                total_w = weights.sum()
                if total_w > 0:
                    row[kf] = (group_df[kf] * weights).sum() / total_w
                else:
                    row[kf] = None  # no population data for this group-year
            else:
                # Safety fallback: simple mean if weight column not found
                row[kf] = group_df[kf].mean()
        return row

    all_df_list = []

    # ── Geography fields (CITYAREA, SUBAREA, CO_NAME) ──────────────────────
    for fld_geog in input["geogFields"]:
        logIt(f"Calculating metric for: {fld_geog}")
        rows = []
        for grp_val, grp in ato_df.groupby(fld_geog):
            row = {"geoname": grp_val}
            row.update(weighted_mean_for_group(grp))
            rows.append(row)

        geog_df = pd.DataFrame(rows)
        geog_df.rename(columns=rename_dict, inplace=True)
        geog_df["geoname"] = geog_df["geoname"].str.title()
        logIt(geog_df.head())
        all_df_list.append(geog_df)

    # ── Geography areas (county groups, regions) ────────────────────────────
    for geog_area in input["geogAreas"]:
        geog_name = geog_area["geogName"]
        logIt(f"Calculating metric for: {geog_name}")
        area_df = ato_df.query(geog_area["query"])

        row = {"geoname": geog_name}
        row.update(weighted_mean_for_group(area_df))

        area_result = pd.DataFrame([row])
        area_result.rename(columns=rename_dict, inplace=True)
        area_result["geoname"] = area_result["geoname"].str.title()
        logIt(area_result.head())
        all_df_list.append(area_result)

    metric_df = pd.concat(all_df_list, ignore_index=True)
    return metric_df


def main():

    # toggle whether to upload to AGOL or write to csv
    upload_data = False

    # Script inputs - each metric should be added as dictionary of key/value pairs in the inputs dictionary
    inputs = {
        "Jobs By Auto": {
            "itemId": "d485928e777740c7963a5b68a37db116",
            "index": 0,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITYAREA", "CO_NAME", "SMALLAREA"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS in [3, 57, 11, 35]",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS == 49",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS==CO_FIPS",
                },
            ],
            "keyFieldPattern": "^JOBAUTO_[0-9]{2}$",
            "weightedFieldPattern": "^HH_[0-9]{2}$",
            "weightedFieldPrefix": "HH_",
            "outFieldPattern": "weighted_ato_jobauto_",
        },
        "Jobs By Transit": {
            "itemId": "d485928e777740c7963a5b68a37db116",
            "index": 0,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITYAREA", "CO_NAME", "SMALLAREA"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS in [3, 57, 11, 35]",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS == 49",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS==CO_FIPS",
                },
            ],
            "keyFieldPattern": "^JOBTRANSIT_[0-9]{2}$",
            "weightedFieldPattern": "^HH_[0-9]{2}$",
            "weightedFieldPrefix": "HH_",
            "outFieldPattern": "weighted_ato_jobtransit_",
        },
        "Population Estimates": {
            "itemId": "db1ebf9044e347758468de2b6d5f744a",
            "index": 0,
            "aggregation": "sum",
            "query": "ModelArea = 'Wasatch Front Travel Demand Model'",
            "geogFields": ["CityArea", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS in [3, 57, 11, 35]",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS == 49",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS==CO_FIPS",
                },
            ],
            "keyFieldPattern": "^YEAR[0-9]{4}$",
            "outFieldPattern": "pop_proj_",
        },
        "Household Estimates": {
            "itemId": "920e71114c8e491cb0d1c01e3766d839",
            "index": 0,
            "aggregation": "sum",
            "query": "ModelArea = 'Wasatch Front Travel Demand Model'",
            "geogFields": ["CityArea", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS in [3, 57, 11, 35]",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS == 49",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS==CO_FIPS",
                },
            ],
            "keyFieldPattern": "^YEAR[0-9]{4}$",
            "outFieldPattern": "hh_proj_",
        },
        "Households with Access to Transit": {
            "itemId": "98a0bd9da71a47339f29fefc7b1cb46a",
            "index": 0,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITYAREA", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS in [3, 57, 11, 35]",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS == 49",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS==CO_FIPS",
                },
            ],
            "keyFieldPattern": "^HH_[0-9]{4}$",
            "outFieldPattern": "hh_20min_walk_transit_",
        },
        "Households with Access to Trails 5min": {
            "itemId": "ee833e7d6461440bbd23d1be0918b875",
            "index": 0,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITYAREA", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS in [3, 57, 11, 35]",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS == 49",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS==CO_FIPS",
                },
            ],
            "keyFieldPattern": "^HH_[0-9]{4}$",
            "outFieldPattern": "hh_5min_walk_trail_",
        },
        "Households with Access to Trails 10min": {
            "itemId": "ce0caa2c8f6c412ba8178e744ae52282",
            "index": 0,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITYAREA", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS in [3, 57, 11, 35]",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS == 49",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS==CO_FIPS",
                },
            ],
            "keyFieldPattern": "^HH_[0-9]{4}$",
            "outFieldPattern": "hh_10min_walk_trail_",
        },
        "Households with Access to Parks 5min": {
            "itemId": "371d341c3aa043e9bc0caf046bfaf403",
            "index": 0,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITYAREA", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS in [3, 57, 11, 35]",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS == 49",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS==CO_FIPS",
                },
            ],
            "keyFieldPattern": "^HH_[0-9]{4}$",
            "outFieldPattern": "hh_5min_walk_parks_",
        },
        "Households with Access to Parks 10min": {
            "itemId": "a2fd003749824e12a347cb561b2ad089",
            "index": 0,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITYAREA", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS in [3, 57, 11, 35]",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS == 49",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS==CO_FIPS",
                },
            ],
            "keyFieldPattern": "^HH_[0-9]{4}$",
            "outFieldPattern": "hh_10min_walk_parks_",
        },
        "Population within Centers": {
            "itemId": "f693c6c6e09a4a75b98169eb1dfbeee4",
            "index": 0,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITYAREA", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS in [3, 57, 11, 35]",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS == 49",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_FIPS"],
                    "query": "CO_FIPS==CO_FIPS",
                },
            ],
            "keyFieldPattern": "^POP_[0-9]{4}$",
            "outFieldPattern": "pop_within_centers_",
        },
        "Percentage of Households and Jobs within Centers": {
            "itemId": "0772a38efb4c4e6689ea0577854ad726",
            "index": 0,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["GEOG"],
            "geogAreas": [],
            "keyFieldPattern": "^hhjob_wcc_pct_[0-9]{4}$",
            "outFieldPattern": "hhjob_wcc_pct_",
        },
        "Housing Costs": {
            "itemId": "72fd762352bd4712989ae56f163a3386",
            "index": 0,
            # Household-weighted mean (see metricWeightedMean).
            # "aggregation": "mean" is NOT used — metricWeightedMean handles its own agg.
            "query": "1=1",
            "geogFields": ["CITYAREA", "SUBAREA", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_NAME"],
                    "query": "CO_NAME in ['BOX ELDER', 'WEBER', 'DAVIS', 'SALT LAKE']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_NAME"],
                    "query": "CO_NAME == 'UTAH'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_NAME"],
                    "query": "CO_NAME==CO_NAME",
                },
            ],
            "keyFieldPattern": "^HCOST[0-9]{4}$",
            "weightFieldPattern": "^TOTHH[0-9]{4}$",
            "outFieldPattern": "HCOST_",
        },
        "Transportation Costs": {
            "itemId": "72fd762352bd4712989ae56f163a3386",
            "index": 0,
            # Household-weighted mean (see metricWeightedMean).
            "query": "1=1",
            "geogFields": ["CITYAREA", "SUBAREA", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_NAME"],
                    "query": "CO_NAME in ['BOX ELDER', 'WEBER', 'DAVIS', 'SALT LAKE']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_NAME"],
                    "query": "CO_NAME == 'UTAH'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_NAME"],
                    "query": "CO_NAME==CO_NAME",
                },
            ],
            "keyFieldPattern": "^TCOST[0-9]{4}$",
            "weightFieldPattern": "^TOTHH[0-9]{4}$",
            "outFieldPattern": "TCOST_",
        },
        "Housing + Transportation Costs": {
            "itemId": "72fd762352bd4712989ae56f163a3386",
            "index": 0,
            "query": "1=1",
            "geogFields": ["CITYAREA", "SUBAREA", "CO_NAME"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["CO_NAME"],
                    "query": "CO_NAME in ['BOX ELDER', 'WEBER', 'DAVIS', 'SALT LAKE']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["CO_NAME"],
                    "query": "CO_NAME == 'UTAH'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["CO_NAME"],
                    "query": "CO_NAME==CO_NAME",
                },
            ],
            "keyFieldPattern": "^HPLUST[0-9]{4}$",
            # Changed from TOTPOP to TOTHH: HPLUST is a per-household ratio so
            # household-weighted aggregation is semantically correct.
            "weightFieldPattern": "^TOTHH[0-9]{4}$",
            "outFieldPattern": "HPLUST_",
        },
        "Commuters that Drive Alone": {
            "itemId": "9fae7da885ce461fad068dad14bcf67c",
            "index": 0,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITY_NAME", "SUBAREA", "COUNTY"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY in ['Box Elder', 'Weber', 'Davis', 'Salt Lake']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY == 'Utah'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY==COUNTY",
                },
            ],
            "keyFieldPattern": "^ACS5_[0-9]{4}$",
            "outFieldPattern": "commuters_drive_alone_",
        },
        "Commuters that use Public Transportation": {
            "itemId": "9fae7da885ce461fad068dad14bcf67c",
            "index": 2,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITY_NAME", "SUBAREA", "COUNTY"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY in ['Box Elder', 'Weber', 'Davis', 'Salt Lake']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY == 'Utah'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY==COUNTY",
                },
            ],
            "keyFieldPattern": "^ACS5_[0-9]{4}$",
            "outFieldPattern": "commuters_public_transport_",
        },
        "Commuters that Work From Home": {
            "itemId": "9fae7da885ce461fad068dad14bcf67c",
            "index": 1,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITY_NAME", "SUBAREA", "COUNTY"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY in ['Box Elder', 'Weber', 'Davis', 'Salt Lake']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY == 'Utah'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY==COUNTY",
                },
            ],
            "keyFieldPattern": "^ACS5_[0-9]{4}$",
            "outFieldPattern": "commuters_work_home_",
        },
        "Median Income": {
            "itemId": "9fae7da885ce461fad068dad14bcf67c",
            "index": 7,
            "aggregation": "mean",
            "query": "1=1",
            "geogFields": ["CITY_NAME", "SUBAREA", "COUNTY"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY in ['Box Elder', 'Weber', 'Davis', 'Salt Lake']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY == 'Utah'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY==COUNTY",
                },
            ],
            "keyFieldPattern": "^ACS5_[0-9]{4}$",
            "outFieldPattern": "median_income_",
        },
        "Persons below Poverty Level": {
            "itemId": "9fae7da885ce461fad068dad14bcf67c",
            "index": 11,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITY_NAME", "SUBAREA", "COUNTY"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY in ['Box Elder', 'Weber', 'Davis', 'Salt Lake']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY == 'Utah'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY==COUNTY",
                },
            ],
            "keyFieldPattern": "^ACS5_[0-9]{4}$",
            "outFieldPattern": "persons_below_poverty_",
        },
        "Aggregate Travel Time for Commuters": {
            "itemId": "9fae7da885ce461fad068dad14bcf67c",
            "index": 13,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITY_NAME", "SUBAREA", "COUNTY"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY in ['Box Elder', 'Weber', 'Davis', 'Salt Lake']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY == 'Utah'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY==COUNTY",
                },
            ],
            "keyFieldPattern": "^ACS5_[0-9]{4}$",
            "outFieldPattern": "commuter_travel_time_",
        },
        "Median Age": {
            "itemId": "9fae7da885ce461fad068dad14bcf67c",
            "index": 14,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITY_NAME", "SUBAREA", "COUNTY"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY in ['Box Elder', 'Weber', 'Davis', 'Salt Lake']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY == 'Utah'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY==COUNTY",
                },
            ],
            "keyFieldPattern": "^ACS5_[0-9]{4}$",
            "outFieldPattern": "median_age_",
        },
        "Persons with Education of Bachelors Degree": {
            "itemId": "9fae7da885ce461fad068dad14bcf67c",
            "index": 21,
            "aggregation": "sum",
            "query": "1=1",
            "geogFields": ["CITY_NAME", "SUBAREA", "COUNTY"],
            "geogAreas": [
                {
                    "geogName": "Wasatch Front Regional Council Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY in ['Box Elder', 'Weber', 'Davis', 'Salt Lake']",
                },
                {
                    "geogName": "Mountainland Association of Governments Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY == 'Utah'",
                },
                {
                    "geogName": "Wasatch Front Region",
                    "queryFields": ["COUNTY"],
                    "query": "COUNTY==COUNTY",
                },
            ],
            "keyFieldPattern": "^ACS5_[0-9]{4}$",
            "outFieldPattern": "persons_with_bachelors_",
        },
    }

    # Item id of the feature layer that the output table will be joined to
    boundaries_item_id = "98bfc2eb26d94adcb6ae9cab2f7d57a8"

    # Details of the output item
    output_item = {
        "newItemTitle": "WFRC_PerformanceMetrics",
        "newServiceName": "WFRC_PerformanceMetrics",
        "newItemTags": "wfrc, metrics",
    }

    # Logging details
    now = datetime.datetime.now()
    cwd = sys.path[0]
    log_fldr = os.path.join(cwd, "Logs")
    if not os.path.exists(log_fldr):
        os.mkdir(log_fldr)
    file_name = now.strftime(r"log_%Y%m%d%H%M%S.txt")
    log_file = os.path.join(log_fldr, file_name)
    logging.basicConfig(
        filename=log_file,
        filemode="w",
        format="%(asctime)s - %(message)s",
        datefmt="%d-%b-%y %H:%M:%S",
        level=logging.INFO,
    )

    # Metrics to run
    metrics = [
        "Jobs By Auto",
        "Jobs By Transit",
        "Population Estimates",
        "Household Estimates",
        "Households with Access to Transit",
        "Households with Access to Trails 5min",
        "Households with Access to Trails 10min",
        "Households with Access to Parks 5min",
        "Households with Access to Parks 10min",
        "Population within Centers",
        "Percentage of Households and Jobs within Centers",
        "Housing Costs",
        "Transportation Costs",
        "Housing + Transportation Costs",
        "Commuters that Drive Alone",
        "Commuters that use Public Transportation",
        "Commuters that Work From Home",
        "Median Income",
        "Persons below Poverty Level",
        "Aggregate Travel Time for Commuters",
        "Median Age",
        "Persons with Education of Bachelors Degree",
    ]

    if metrics:
        un = "analytics_wfrc"
        pw = keyring.get_password("Analytics AGOL", un)

        # Portal info
        portal_profile = "wfrc_profile"

        # Connect to WFRC's portal
        gis = arcgis.gis.GIS("https://wfrc.maps.arcgis.com", un, pw, profile=portal_profile)
        logIt(f"Connected to gis: {gis}")

        # Initialize final output dataframe
        output_df = pd.DataFrame()

        # Run chosen metrics
        if "Jobs By Auto" in metrics:
            input = inputs["Jobs By Auto"]
            # Get dataframe of metric data
            metric_df = metricJobsBy(gis, "Jobs By Auto", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Jobs By Transit" in metrics:
            input = inputs["Jobs By Transit"]
            # Get dataframe of metric data
            metric_df = metricJobsBy(gis, "Jobs By Transit", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Population Estimates" in metrics:
            input = inputs["Population Estimates"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(gis, "Population Estimates", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Household Estimates" in metrics:
            input = inputs["Household Estimates"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(gis, "Household Estimates", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Households with Access to Transit" in metrics:
            input = inputs["Households with Access to Transit"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(gis, "Households with Access to Transit", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Households with Access to Trails 5min" in metrics:
            input = inputs["Households with Access to Trails 5min"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(
                gis, "Households with Access to Trails 5min", input
            )
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Households with Access to Trails 10min" in metrics:
            input = inputs["Households with Access to Trails 10min"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(
                gis, "Households with Access to Trails 10min", input
            )
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Households with Access to Parks 5min" in metrics:
            input = inputs["Households with Access to Parks 5min"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(
                gis, "Households with Access to Parks 5min", input
            )
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Households with Access to Parks 10min" in metrics:
            input = inputs["Households with Access to Parks 10min"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(
                gis, "Households with Access to Parks 10min", input
            )
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Population within Centers" in metrics:
            input = inputs["Population within Centers"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(gis, "Population within Centers", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Percentage of Households and Jobs within Centers" in metrics:
            input = inputs["Percentage of Households and Jobs within Centers"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(
                gis, "Percentage of Households and Jobs within Centers", input
            )
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Housing Costs" in metrics:
            input = inputs["Housing Costs"]
            # Get dataframe of metric data — household-weighted mean (TOTHH{YYYY})
            metric_df = metricWeightedMean(gis, "Housing Costs", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Transportation Costs" in metrics:
            input = inputs["Transportation Costs"]
            # Get dataframe of metric data — household-weighted mean (TOTHH{YYYY})
            metric_df = metricWeightedMean(gis, "Transportation Costs", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Housing + Transportation Costs" in metrics:
            input = inputs["Housing + Transportation Costs"]
            # Get dataframe of metric data — household-weighted mean (TOTHH{YYYY}).
            # HPLUST is a per-household ratio; TOTHH weighting is semantically correct.
            # Previously used TOTPOP; changed 2026-03-20.
            metric_df = metricWeightedMean(gis, "Housing + Transportation Costs", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Commuters that Drive Alone" in metrics:
            input = inputs["Commuters that Drive Alone"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(gis, "Commuters that Drive Alone", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Commuters that use Public Transportation" in metrics:
            input = inputs["Commuters that use Public Transportation"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(
                gis, "Commuters that use Public Transportation", input
            )
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Commuters that Work From Home" in metrics:
            input = inputs["Commuters that Work From Home"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(gis, "Commuters that Work From Home", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Median Income" in metrics:
            input = inputs["Median Income"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(gis, "Median Income", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Persons below Poverty Level" in metrics:
            input = inputs["Persons below Poverty Level"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(gis, "Persons below Poverty Level", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Aggregate Travel Time for Commuters" in metrics:
            input = inputs["Aggregate Travel Time for Commuters"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(
                gis, "Aggregate Travel Time for Commuters", input
            )
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Median Age" in metrics:
            input = inputs["Median Age"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(gis, "Median Age", input)
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        if "Persons with Education of Bachelors Degree" in metrics:
            input = inputs["Persons with Education of Bachelors Degree"]
            # Get dataframe of metric data
            metric_df = metricEstimatesProjections(
                gis, "Persons with Education of Bachelors Degree", input
            )
            # Merge metric to existing output
            output_df = mergeMetricDataframes(output_df, metric_df)

        # Get feature layer used for joining data
        logIt("Retrieving Performance Metric Boundaries feature layer from Portal")
        boundaries_fl_item = gis.content.get(boundaries_item_id)
        boundaries_fl = boundaries_fl_item.layers[0]
        boundaries_sdf = boundaries_fl.query().sdf

        # Merge output dataframe to existing feature layer item
        logIt("Merging metrics to Boundaries layer")
        merged_df = pd.merge(
            left=boundaries_sdf, right=output_df, how="inner", left_on="GeoName", right_on="geoname"
        )

        # Drop duplicate columns"
        merged_df.drop(columns=["Shape__Area", "Shape__Length"], inplace=True)

        # update names of workshop areas, if present
        wa_lookup = {
            "Box Elder Wfrc": "Box Elder County - Southern",
            "Davis County North": "Davis County - Northern",
            "Davis County South": "Davis County - Southern",
            "Salt Lake County North": "Salt Lake County - Northern",
            "Salt Lake County Sw": "Salt Lake County - Southwest",
            "Salt Lake County Se": "Salt Lake County - Southeast",
            "Weber County North": "Weber County - Northern",
            "Weber County South": "Weber County - Southern",
            "Utah County Central": "Utah County - Central",
            "Utah County North": "Utah County - Northern",
            "Utah County South": "Utah County - Southern",
        }
        merged_df["GeoName"] = merged_df["GeoName"].replace(wa_lookup)

        if upload_data == True:
            # Check if item already exists
            logIt("Checking for existing File Geodatabase item.")
            gdb_found = [
                item
                for item in gis.content.search(
                    "title:{} AND type:File Geodatabase".format(output_item["newItemTitle"])
                )
                if item.title == output_item["newItemTitle"]
            ]
            if gdb_found:
                logIt("File Geodatabase found.  Deleting item.")
                item = gdb_found[0]
                item.delete()

            # Check for exact name.  This is needed as a workaround where the query in the content search is returning items with titles that don't match exactly.
            logIt("Checking for existing Feature Service item.")
            items_found = [
                item
                for item in gis.content.search(
                    "title:{} AND type:Feature Service".format(output_item["newItemTitle"])
                )
                if item.title == output_item["newItemTitle"]
            ]
            if items_found:
                item = items_found[0]
                logIt("Existing feature service found.  Overwriting item.")
                result = merged_df.spatial.to_featurelayer(
                    output_item["newItemTitle"],
                    gis=gis,
                    tags=output_item["newItemTags"],
                    overwrite=True,
                    service={"featureServiceId": item.id, "layer": 0},
                    sanitize_columns=False,
                )
                logIt(result)
            else:
                logIt("Feature service not found.  Adding item.")
                result = merged_df.spatial.to_featurelayer(
                    output_item["newItemTitle"], sanitize_columns=False
                )
                logIt(result)

        if upload_data == False:
            merged_df.to_csv(r".\Outputs\test_metrics.csv", index=False)


if __name__ == "__main__":
    main()
