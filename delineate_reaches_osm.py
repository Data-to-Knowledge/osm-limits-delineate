# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 19:36:49 2019

@author: michaelek
"""
import os
import pandas as pd
import geopandas as gpd
from pdsql import mssql, util
from gistools import osm, vector
from utils import json_filters, geojson_convert, process_limit_data, assign_notes, get_json_from_api
from sqlalchemy import types

pd.options.display.max_columns = 10

today = pd.Timestamp.now()
today_str  = today.strftime('%Y-%m-%d %H:%M:%S')

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
pts_id = 'GeoPK'
id_col = 'SpatialUnitId'

allo_zones_table = 'WD_NZTM_GROUNDWATER_ALLOCATION_ZONES'
allo_zones_id = 'GeoPK'

reaches_table = 'WD_NZTM_RIVERS'

#zone_exceptions = ['CWAZ0004', 'CWAZ0005', 'CWAZ0006', 'CWAZ0007', 'CWAZ0008', 'CWAZ0009']
combined_zones_id = 371
no_limit_id = 368

cwms_gis = {'server': '172.23.92.198', 'database': 'GIS', 'table': 'CWMS_NZTM_ZONES', 'col_names': ['ZONE_NAME'], 'rename_cols': ['cwms'], 'geo_col': True}

base_path = r'P:\WaterDataProgramme\limit_points'

#pts_gpkg = 'sw_limit_points.gpkg'
#layer_name = 'sw_limit_points'
#crs1 = {'init': 'epsg:2193'}

nodes_shp = 'site_nodes_2020-02-07.shp'

osm.op_endpoint = 'http://10.8.1.5/api/interpreter'
#op_endpoint = 'http://10.8.1.5/api/interpreter'

delin_shp = 'reach_delin_limit_osm_2020-02-07.shp'

poly_shp = r'C:\ecan\git\gistools\gistools\datasets\shapefiles\catchment_pareora.shp'

add_geo_column = "alter table {table} add {sfield} geometry"
update_geo_stmt = "UPDATE {table} SET {sfield} = geometry::STGeomFromText({sfield}, 2193) where SpatialUnitId in ({names})"
drop_column = "alter table {table} drop {ofield}"

#mssql.del_table_rows(param['output']['server'], param['output']['database'], 'SpatialUnit', stmt=update_geo_stmt.format(names=str(s_gdf3['SpatialUnitID'].tolist())[1:-1]))

########################################
### Load data

run_time_start = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
print(run_time_start)

## Read in source data
print('--Reading in source data...')

json_lst = get_json_from_api()
json_lst1 = json_filters(json_lst, only_operative=True, only_reach_points=True)
gjson1, hydro_units, pts_alt, sg1 = geojson_convert(json_lst1)

combined_zones1 = [j for j in json_lst if j['id'] == combined_zones_id][0]
combined_zones2 = [s['id'] for s in combined_zones1['spatialUnit']]

no_limit1 = [j for j in json_lst if j['id'] == no_limit_id][0]
no_limit2 = [s['id'] for s in no_limit1['spatialUnit']][0]

pts = mssql.rd_sql(gis_server, gis_db, pts_table, [pts_id], geo_col=True, username=gis_username, password=gis_password)
pts.rename(columns={pts_id: id_col}, inplace=True)

cwms1 = mssql.rd_sql(**cwms_gis)

zones3 = mssql.rd_sql(gis_server, gis_db, allo_zones_table, [allo_zones_id], where_in={allo_zones_id: combined_zones2}, username=gis_username, password=gis_password, geo_col=True, rename_cols=[id_col])
zones4 = zones3.unary_union

#pts = pts.to_crs('epsg:2193')

#pts = gpd.read_file(os.path.join(base_path, pts_gpkg), layer=layer_name)
#
#pts = pts[['ManagementGroupID', 'geometry']].copy()

pts['geometry'] = pts.geometry.simplify(1)

#######################################
### Run query
print('--Pull out the waterways from OSM')
pts1 = osm.get_nearest_waterways(pts, id_col, 100, 'all')

#pts1.to_file(os.path.join(base_path, nodes_shp))

waterways, nodes = osm.get_waterways(pts1, 'all')

print('--Delineating Reaches from OSM')

site_delin = osm.waterway_delineation(waterways, True)
osm_delin = osm.to_osm(site_delin, nodes)
gdf1 = osm.to_gdf(osm_delin)

gdf2 = gdf1.to_crs(pts.crs)

#gdf3 = gdf2.merge(pts1.rename(columns={'id': 'start_node'})[['start_node', id_col]], on='start_node').dissolve([id_col, 'name']).reset_index().drop('way_id', axis=1)
gdf3 = gdf2.merge(pts1.rename(columns={'id': 'start_node'})[['start_node', id_col]], on='start_node')

#gdf3.to_file(os.path.join(base_path, delin_shp))

print('--Pulling out all of Canterbury...')
cant2 = osm.get_waterways_within_boundary(cwms1, buffer=0, waterway_type='all')

combined1, poly1 = vector.pts_poly_join(cant2, zones3, id_col, op='intersects')

all_others1 = cant2[~cant2.way_id.isin(combined1.way_id)]
all_others2 = all_others1[~all_others1.way_id.isin(gdf2.way_id.unique().tolist())].copy()
all_others2[id_col] = no_limit2

print('--Combine all reach data')

gdf4 = pd.concat([gdf3, combined1, all_others2]).reset_index(drop=True)
gdf4.rename(columns={'way_id': 'OSMWaterwayId', 'waterway': 'OSMWaterwayType', 'name': 'RiverName', 'start_node': 'StartNode'}, inplace=True)
gdf4['OSMWaterwayId'] = gdf4['OSMWaterwayId'].astype('int64')
#gdf4['StartNode'] = gdf4['StartNode'].astype('int64')

print('--Compare existing reaches in the database')
cols = gdf4.columns.drop('geometry').tolist()
cols.extend(['OBJECTID'])

old1 = mssql.rd_sql(gis_server, gis_db, reaches_table, cols, username=gis_username, password=gis_password, geo_col=True)

comp_dict = util.compare_dfs(old1.drop('OBJECTID', axis=1), gdf4, on=['SpatialUnitId', 'OSMWaterwayId'])
new1 = comp_dict['new'].copy()
diff1 = comp_dict['diff'].copy()
rem1 = comp_dict['remove'][['SpatialUnitId', 'OSMWaterwayId']].copy()

print('--Save to database')
sql_dtypes = {'StartNode': types.BIGINT(), 'OSMWaterwayId': types.BIGINT(), 'RiverName': types.NVARCHAR(200), 'OSMWaterwayType': types.NVARCHAR(30), 'SpatialUnitId': types.NVARCHAR(8), 'SHAPE_': types.VARCHAR(), 'OBJECTID': types.INT(), 'ModifiedDate': types.DATETIME()}

if not new1.empty:
    max_id = old1['OBJECTID'].max() + 1

    new1['ModifiedDate'] = today_str
    new1['OBJECTID'] = list(range(max_id, max_id + len(new1)))
    new1.rename(columns={'geometry': 'SHAPE'}, inplace=True)

    mssql.update_table_rows(new1, gis_server, gis_db, reaches_table, on=['SpatialUnitId', 'OSMWaterwayId'], index=False, append=True, username=None, password=None, geo_col='SHAPE', clear_table=False, dtype=sql_dtypes)

if not diff1.empty:
    diff2 = pd.merge(diff1, old1[['SpatialUnitId', 'OSMWaterwayId', 'OBJECTID']], on=['SpatialUnitId', 'OSMWaterwayId'])
    diff1['ModifiedDate'] = today_str
    diff1.rename(columns={'geometry': 'SHAPE'}, inplace=True)

    mssql.update_table_rows(diff1, gis_server, gis_db, reaches_table, on=['SpatialUnitId', 'OSMWaterwayId'], index=False, append=True, username=None, password=None, geo_col='SHAPE', clear_table=False, dtype=sql_dtypes)

if not rem1.empty:
    mssql.del_table_rows(gis_server, gis_db, reaches_table, pk_df=rem1, username=None, password=None)

########################################
### Testing

#poly = gpd.read_file(poly_shp)

#server = '172.23.92.244'
#database = 'gis_waterdata'
#table = 'WD_NZTM_RIVERS'
#on=['SpatialUnitId', 'OSMWaterwayId']
#index=False
#append=True
#username=None
#password=None
#geo_col='SHAPE'
#
#df = gdf4[:100].copy()
#df = gdf4.copy()
#
#h1 = old1.geometry.apply(lambda x: hash(x.wkt))
#h2 = gdf4.geometry.apply(lambda x: hash(x.wkt))







