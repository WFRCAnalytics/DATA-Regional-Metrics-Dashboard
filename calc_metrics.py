"""
calc_metrics.py
Brooke Reams - breams@esri.com
Feb. 23, 2024

Script written for WFRC to calculate "Big 5 Metrics" consumed by Dashboard
"""


import arcgis
import pandas as pd
import os
import re
import logging
import datetime
import sys


def logIt(message):
    print(message)
    logging.info(message)


def getFeatureLayerFromItemId(gis, item_id):
    item = gis.content.get(item_id)
    fl = item.layers[0]

    return fl


def mergeMetricDataframes(output_df, metric_df):
    # Merge metric dataframes
    if not output_df.empty:
        output_df = output_df.merge(metric_df, how="outer", on="geoname")
    else:
        output_df = metric_df

    return output_df



def metricJobsBy(gis, metric_name, input):
    logIt("Current metric: {}".format(metric_name))
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
    for i in range(0, len(key_flds)):
        fld = key_flds[i]
        if i > 0:
            out_flds.append(input["outFieldPattern"] + "FY{}".format(i))
        else:
            out_flds.append(input["outFieldPattern"] + "CY")


    # Array to store all geography dataframes
    all_df_list = []

    # Perform metric calc on each geography field
    for fld_geog in input["geogFields"]:
        logIt("Calculating metric for: {}".format(fld_geog))
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
                    weighted_value = (merge_df[key_fld][ind] * (merge_df[weighted_x_fld][ind] / merge_df[weighted_y_fld][ind]))
                    value_list.append(weighted_value)
                else:
                    value_list.append(0)
            geog_list.append(value_list)

        
        # Create dataframe of geog and weigted values
        geog_df = pd.DataFrame(geog_list, columns=out_flds)
        geog_df["geoname"] = geog_df["geoname"].str.title()
        geog_df = geog_df.groupby("geoname").sum()
        logIt(geog_df.head())
        all_df_list.append(geog_df)


    # Perform metric calc on each geog area (by query)
    for geog_area in input["geogAreas"]:
        geog_name = geog_area["geogName"]
        logIt("Calculating metric for: {}".format(geog_name))
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
                weighted_value = (area_df[key_fld][ind] * (area_df[weighted_x_fld][ind] / sum_weighted_fld))
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
    logIt("Current metric: {}".format(metric_name))
    # Get fs item from portal
    ato_fl = getFeatureLayerFromItemId(gis, input["itemId"])
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
        logIt("Calculating metric for: {}".format(fld_geog))
        ato_df["geoname"] = ato_df[fld_geog]

        groupby_dict = {}
        for fld in ato_df.columns:
            if re.match(input["keyFieldPattern"], fld):
                groupby_dict[fld] = "sum"
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
    for geog_area in input["geogAreas"]:
        geog_name = geog_area["geogName"]
        logIt("Calculating metric for: {}".format(geog_name))

        ato_df["geoname"] = geog_area["geogName"]
        query = geog_area["query"]
        area_df = ato_df.query(query)

        groupby_dict = {}
        drop_columns = []
        for fld in area_df.columns:
            if re.match(input["keyFieldPattern"], fld):
                groupby_dict[fld] = "sum"
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


def main():
    # Script inputs - each metric should be added as dictionary of key/value pairs in the inputs dictionary
    inputs = {"Jobs By Auto": {"itemId": "d485928e777740c7963a5b68a37db116",
            "query": "1=1",
            "geogFields": ["CITYAREA", "CO_NAME", "SMALLAREA"],
            "geogAreas": [{"geogName": "Wasatch Front Regional Council Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS in [3, 57, 11, 35]"},
                            {"geogName": "Mountainland Association of Governments Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS == 49"},
                            {"geogName": "Wasatch Front Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS==CO_FIPS"}],
            "keyFieldPattern": "^JOBAUTO_[0-9]{2}$",
            "weightedFieldPattern": "^HH_[0-9]{2}$",
            "weightedFieldPrefix": "HH_",
            "outFieldPattern": "weighted_ato_jobauto_"},
            "Jobs By Transit": {"itemId": "d485928e777740c7963a5b68a37db116",
            "query": "1=1",
            "geogFields": ["CITYAREA", "CO_NAME", "SMALLAREA"],
            "geogAreas": [{"geogName": "Wasatch Front Regional Council Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS in [3, 57, 11, 35]"},
                            {"geogName": "Mountainland Association of Governments Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS == 49"},
                            {"geogName": "Wasatch Front Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS==CO_FIPS"}],
            "keyFieldPattern": "^JOBTRANSIT_[0-9]{2}$",
            "weightedFieldPattern": "^HH_[0-9]{2}$",
            "weightedFieldPrefix": "HH_",
            "outFieldPattern": "weighted_ato_jobtransit_"},
            "Population Estimates": {"itemId": "db1ebf9044e347758468de2b6d5f744a",
                "query": "ModelArea = 'Wasatch Front Travel Demand Model'",
                "geogFields": ["CityArea", "CO_NAME"],
                "geogAreas": [{"geogName": "Wasatch Front Regional Council Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS in [3, 57, 11, 35]"},
                            {"geogName": "Mountainland Association of Governments Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS == 49"},
                            {"geogName": "Wasatch Front Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS==CO_FIPS"}],
                "keyFieldPattern": "^YEAR[0-9]{4}$",
                "outFieldPattern": "pop_proj_"},
            "Household Estimates": {"itemId": "920e71114c8e491cb0d1c01e3766d839",
                "query": "ModelArea = 'Wasatch Front Travel Demand Model'",
                "geogFields": ["CityArea", "CO_NAME"],
                "geogAreas": [{"geogName": "Wasatch Front Regional Council Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS in [3, 57, 11, 35]"},
                            {"geogName": "Mountainland Association of Governments Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS == 49"},
                            {"geogName": "Wasatch Front Region", "queryFields": ["CO_FIPS"], "query": "CO_FIPS==CO_FIPS"}],
                "keyFieldPattern": "^YEAR[0-9]{4}$",
                "outFieldPattern": "hh_proj_"}}

    # Item id of the feature layer that the output table will be joined to
    boundaries_item_id = "98bfc2eb26d94adcb6ae9cab2f7d57a8"

    # Details of the output item
    output_item = {"newItemTitle": "WFRC_PerformanceMetrics",
            "newServiceName": "WFRC_PerformanceMetrics",
            "newItemTags": "wfrc, metrics"}
    

    # Logging details
    now = datetime.datetime.now()
    cwd = sys.path[0]
    log_fldr = os.path.join(cwd, "Logs")
    if not os.path.exists(log_fldr):
        os.mkdir(log_fldr)
    file_name = now.strftime(r"log_%Y%m%d%H%M%S.txt")
    log_file = os.path.join(log_fldr, file_name)
    logging.basicConfig(filename=log_file, filemode="w",
                    format="%(asctime)s - %(message)s",
                    datefmt="%d-%b-%y %H:%M:%S",
                    level=logging.INFO)


    # Metrics to run
    metrics = ["Jobs By Auto", "Jobs By Transit", "Population Estimates", "Household Estimates"]


    if metrics:
        # Portal info
        portal_profile = "wfrc_profile"

        # Connect to WFRC's portal
        ##gis = arcgis.gis.GIS(profile=portal_profile)
        gis = arcgis.gis.GIS("https://wfrc.maps.arcgis.com", "wfrc_consult", "consulting4wfrc", profile=portal_profile)
        logIt("Connected to gis: {}".format(gis))

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


        # Get feature layer used for joining data
        logIt("Retrieving Performance Metric Boundaries feature layer from Portal")
        boundaries_fl_item = gis.content.get(boundaries_item_id)
        boundaries_fl = boundaries_fl_item.layers[0]
        boundaries_sdf = boundaries_fl.query().sdf

        # Merge output dataframe to existing feature layer item
        logIt("Merging metrics to Boundaries layer")
        merged_df = pd.merge(left=boundaries_sdf, right=output_df, how="inner", left_on="GeoName", right_on="geoname")

        # Drop duplicate columns"
        merged_df.drop(columns=["Shape__Area", "Shape__Length"], inplace=True)

        # Check if item already exists
        logIt("Checking for existing File Geodatabase item.")
        gdb_found = [item for item in gis.content.search('title:{} AND type:File Geodatabase'.format(output_item["newItemTitle"])) if item.title == output_item["newItemTitle"]]
        if gdb_found:
            logIt("File Geodatabase found.  Deleting item.")
            item = gdb_found[0]
            item.delete()

        # Check for exact name.  This is needed as a workaround where the query in the content search is returning items with titles that don't match exactly.
        logIt("Checking for existing Feature Service item.")
        items_found = [item for item in gis.content.search('title:{} AND type:Feature Service'.format(output_item["newItemTitle"])) if item.title == output_item["newItemTitle"]]
        if items_found:
            item = items_found[0]
            logIt("Existing feature service found.  Overwriting item.")
            result = merged_df.spatial.to_featurelayer(output_item["newItemTitle"], gis=gis, tags=output_item["newItemTags"],
                                                           overwrite=True, service={"featureServiceId": item.id, "layer": 0})
            logIt(result)
        else:
            logIt("Feature service not found.  Adding item.")
            result = merged_df.spatial.to_featurelayer(output_item["newItemTitle"])
            logIt(result)

        


if __name__ == "__main__":
    main()