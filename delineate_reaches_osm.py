# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 19:36:49 2019

@author: michaelek
"""
import os
import pandas as pd
import geopandas as gpd
from pdsql import mssql
from gistools import osm
from utils import json_filters, geojson_convert, process_limit_data, assign_notes, get_json_from_api

pd.options.display.max_columns = 10

########################################
### Parameters

pl_server = 'azwaterdatasql.internal.ecan.govt.nz'
pl_db = 'planlimits'

pl_username = 'planlimitsuser'
pl_password = '7x#H"A6Lj)xu(<'

su_table = 'spatialunit'
sg_table = 'spatialgroup'
mg_table = 'ManagementGroup'

gis_server = '172.23.92.244'
gis_db = 'gis_waterdata'

gis_username = 'ReportingServices'
gis_password = 'coffee2go'

pts_table = 'WD_NZTM_Surface_Water_Points'


id_col = 'SpatialUnitId'

base_path = r'P:\WaterDataProgramme\limit_points'

#pts_gpkg = 'sw_limit_points.gpkg'
#layer_name = 'sw_limit_points'
#crs1 = {'init': 'epsg:2193'}

nodes_shp = 'site_nodes_2020-02-07.shp'

osm.op_endpoint = 'http://10.8.1.5/api/interpreter'
#op_endpoint = 'http://10.8.1.5/api/interpreter'

delin_shp = 'reach_delin_limit_osm_2020-02-07.shp'

########################################
### Load data

run_time_start = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
print(run_time_start)

## Read in source data
print('--Reading in source data...')

json_lst = get_json_from_api()
json_lst1 = json_filters(json_lst, only_operative=True, only_reach_points=True)
gjson1, hydro_units, pts, sg1 = geojson_convert(json_lst1)

pts.rename(columns={'id': id_col}, inplace=True)

pts = pts.to_crs('epsg:2193')




#pts = gpd.read_file(os.path.join(base_path, pts_gpkg), layer=layer_name)
#
#pts = pts[['ManagementGroupID', 'geometry']].copy()

pts['geometry'] = pts.geometry.simplify(1)

#######################################
### Run query

pts1 = osm.get_nearest_waterways(pts, id_col, 100, 'all')

pts1.to_file(os.path.join(base_path, nodes_shp))

waterways, nodes = osm.get_waterways(pts1, 'all')
site_delin = osm.waterway_delineation(waterways, True)
osm_delin = osm.to_osm(site_delin, nodes)
gdf1 = osm.to_gdf(osm_delin)

gdf2 = gdf1.to_crs(pts.crs)

gdf3 = gdf2.merge(pts1.rename(columns={'id': 'start_node'})[['start_node', id_col]], on='start_node').dissolve([id_col, 'name']).reset_index().drop('way_id', axis=1)

gdf3.to_file(os.path.join(base_path, delin_shp))








