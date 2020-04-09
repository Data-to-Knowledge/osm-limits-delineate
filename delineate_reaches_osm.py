# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 19:36:49 2019

@author: michaelek
"""
import pandas as pd
import geopandas as gpd
from pdsql import mssql, util
from gistools import osm, vector
from utils import json_filters, geojson_convert, get_json_from_api
from sqlalchemy import types

pd.options.display.max_columns = 10

today = pd.Timestamp.now()
today_str  = today.strftime('%Y-%m-%d %H:%M:%S')

########################################
### Parameters

add_geo_column = "alter table {table} add {sfield} geometry"
update_geo_stmt = "UPDATE {table} SET {sfield} = geometry::STGeomFromText({sfield}, 2193) where SpatialUnitId in ({names})"
drop_column = "alter table {table} drop {ofield}"

id_col = 'SpatialUnitId'


def osm_delineation(param):
    """

    """
    osm.op_endpoint = param['osm']['op_endpoint']

    ########################################
    ### Load data

    # run_time_start = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
    # print(run_time_start)

    ## Read in source data
    print('--Reading in source data...')

    json_lst = get_json_from_api(param['plan_limits']['api_url'], param['plan_limits']['api_headers'])
    json_lst1 = json_filters(json_lst, only_operative=True, only_reach_points=True)
    gjson1, hydro_units, pts_alt, sg1 = geojson_convert(json_lst1)

    combined_zones1 = [j for j in json_lst if j['id'] == param['other']['combined_zones_id']][0]
    combined_zones2 = [s['id'] for s in combined_zones1['spatialUnit']]

    no_limit1 = [j for j in json_lst if j['id'] == param['other']['no_limit_id']][0]
    no_limit2 = [s['id'] for s in no_limit1['spatialUnit']][0]

    # pts = mssql.rd_sql(param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['pts']['table'], [param['gis_waterdata']['pts']['id']], where_in={param['gis_waterdata']['pts']['id']: pts_alt.id.unique().tolist()}, geo_col=True, username=param['gis_waterdata']['username'], password=param['gis_waterdata']['password'], rename_cols=[id_col])
    pts = mssql.rd_sql(param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['pts']['table'], [param['gis_waterdata']['pts']['id']], where_in={param['gis_waterdata']['pts']['id']: pts_alt.id.unique().tolist()}, geo_col=True, rename_cols=[id_col])

    ## Point checks
    excluded_points = pts_alt[~pts_alt.id.isin(pts.SpatialUnitId)].copy()
    if not excluded_points.empty:
        print('These points have a GIS location, but are not in the Plan Limits db:')
        print(excluded_points)

    bad_geo = pts[pts.geom_type != 'Point']
    if not bad_geo.empty:
        print('These points do not have a "Point" geometry (likely "MultiPoint"):')
        print(bad_geo)
        pts = pts[~pts.SpatialUnitId.isin(bad_geo.SpatialUnitId)].copy()

    cwms1 = mssql.rd_sql(param['gis_prod']['server'], param['gis_prod']['database'], param['gis_prod']['cwms']['table'], param['gis_prod']['cwms']['col_names'], rename_cols=param['gis_prod']['cwms']['rename_cols'], geo_col=True, username=param['gis_prod']['username'], password=param['gis_prod']['password'])

    # zones3 = mssql.rd_sql(param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['allo_zones']['table'], [param['gis_waterdata']['allo_zones']['id']], where_in={param['gis_waterdata']['allo_zones']['id']: combined_zones2}, username=param['gis_waterdata']['username'], password=param['gis_waterdata']['password'], geo_col=True, rename_cols=[id_col])
    zones3 = mssql.rd_sql(param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['allo_zones']['table'], [param['gis_waterdata']['allo_zones']['id']], where_in={param['gis_waterdata']['allo_zones']['id']: combined_zones2}, geo_col=True, rename_cols=[id_col])

    pts['geometry'] = pts.geometry.simplify(1)

    #######################################
    ### Run query
    print('--Pull out the waterways from OSM')

    pts1, bad_points = osm.get_nearest_waterways(pts, id_col, param['other']['search_distance'], 'all')

    waterways, nodes = osm.get_waterways(pts1, 'all')

    print('--Delineating Reaches from OSM')

    site_delin = osm.waterway_delineation(waterways, True)
    osm_delin = osm.to_osm(site_delin, nodes)
    gdf1 = osm.to_gdf(osm_delin)

    gdf2 = gdf1.to_crs(pts.crs)

    gdf3 = gdf2.merge(pts1.rename(columns={'id': 'start_node'})[['start_node', id_col]], on='start_node')

    print('--Pulling out all of Canterbury...')

    cant2 = osm.get_waterways_within_boundary(cwms1, buffer=0, waterway_type='all')

    combined1, poly1 = vector.pts_poly_join(cant2, zones3, id_col, op='intersects')
    gdf3 = gdf3[~gdf3.way_id.isin(combined1.way_id.unique())].copy()

    all_others1 = cant2[~cant2.way_id.isin(combined1.way_id)]
    all_others2 = all_others1[~all_others1.way_id.isin(gdf3.way_id.unique().tolist())].copy()
    all_others2[id_col] = no_limit2

    print('--Combine all reach data')

    gdf4 = pd.concat([gdf3, combined1, all_others2]).reset_index(drop=True)

    gdf4.rename(columns={'way_id': 'OSMWaterwayId', 'waterway': 'OSMWaterwayType', 'name': 'RiverName', 'start_node': 'StartNode'}, inplace=True)
    gdf4['OSMWaterwayId'] = gdf4['OSMWaterwayId'].astype('int64')

    print('--Compare existing reaches in the database')

    cols = gdf4.columns.drop('geometry').tolist()
    cols.extend(['OBJECTID'])

    # old1 = mssql.rd_sql(param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['reaches']['table'], cols, username=param['gis_waterdata']['username'], password=param['gis_waterdata']['password'], geo_col=True)
    old1 = mssql.rd_sql(param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['reaches']['table'], cols, geo_col=True)

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

        # mssql.update_table_rows(new1, param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['reaches']['table'], on=['SpatialUnitId', 'OSMWaterwayId'], index=False, append=True, username=param['gis_waterdata']['username'], password=param['gis_waterdata']['password'], geo_col='SHAPE', clear_table=False, dtype=sql_dtypes)
        mssql.update_table_rows(new1, param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['reaches']['table'], on=['SpatialUnitId', 'OSMWaterwayId'], index=False, append=True, geo_col='SHAPE', clear_table=False, dtype=sql_dtypes)

    if not diff1.empty:
        diff2 = pd.merge(diff1, old1[['SpatialUnitId', 'OSMWaterwayId', 'OBJECTID']], on=['SpatialUnitId', 'OSMWaterwayId'])
        diff2['ModifiedDate'] = today_str
        diff2.rename(columns={'geometry': 'SHAPE'}, inplace=True)

        # mssql.update_table_rows(diff2, param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['reaches']['table'], on=['SpatialUnitId', 'OSMWaterwayId'], index=False, append=True, username=param['gis_waterdata']['username'], password=param['gis_waterdata']['password'], geo_col='SHAPE', clear_table=False, dtype=sql_dtypes)
        mssql.update_table_rows(diff2, param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['reaches']['table'], on=['SpatialUnitId', 'OSMWaterwayId'], index=False, append=True, geo_col='SHAPE', clear_table=False, dtype=sql_dtypes)

    if not rem1.empty:
        # mssql.del_table_rows(param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['reaches']['table'], pk_df=rem1, username=param['gis_waterdata']['username'], password=param['gis_waterdata']['password'])
        mssql.del_table_rows(param['gis_waterdata']['server'], param['gis_waterdata']['database'], param['gis_waterdata']['reaches']['table'], pk_df=rem1)

    return gdf4, excluded_points, bad_geo, bad_points

########################################
### Testing








