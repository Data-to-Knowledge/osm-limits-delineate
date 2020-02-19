# -*- coding: utf-8 -*-
"""
Created on Tue Jan 15 15:59:37 2019

@author: MichaelEK
"""
import pandas as pd
import numpy as np
#from pyproj import Proj, CRS, Transformer
import geopandas as gpd
#from gistools import vector
from shapely import wkt
#import json
import requests

pd.options.display.max_columns = 10

##########################################
### Parameters

today1 = pd.Timestamp.today()

month_map = {1: 'Jul', 2: 'Aug', 3: 'Sep', 4: 'Oct', 5: 'Nov', 6: 'Dec', 7: 'Jan', 8: 'Feb', 9: 'Mar', 10: 'Apr', 11: 'May', 12: 'Jun'}

##########################################
### Functions



def get_json_from_api(api_url, api_headers):
    """

    """
    r = requests.get(api_url, headers=api_headers)

    return r.json()


def json_filters(json_lst, only_operative=True, only_gw=False, only_reach_points=False):
    """

    """
    json_lst1 = []

    for j in json_lst.copy():
        if j['spatialUnit']:
            j['managementUnit'] = [m for m in j['managementUnit'] if (m['parameterType'] == 'Allocation Block')]
            json_lst1.append(j)

    if only_operative:
        json_lst1 = [j for j in json_lst1 if (j['status']['status'] == 'Operative') and (pd.Timestamp(j['status']['fromDate']) <= today1)]

    ## Select only GW limits and combined GW/SW limits
    if only_gw:
        json_lst1 = [j for j in json_lst1 if j['hydroUnit'] != ['Surface Water']]
    if only_reach_points:
        json_lst1 = [j for j in json_lst1 if 'SWPT' in j['spatialUnit'][0]['id']]

    return json_lst1


def geojson_convert(json_lst):
    """

    """
    gjson1 = []
    hydro_units = {'Groundwater': {'value': [], 'label': []}, 'Surface Water': {'value': [], 'label': []}}
    sg = []

    for j in json_lst.copy():
        if isinstance(j['spatialUnit'], list):
            for g in j['spatialUnit']:
                gjson1.append(g)
                for h in j['hydroUnit']:
                    sg.append([j['id'], g['id'], h])
                    hydro_units[h]['value'].extend([g['id']])
                    hydro_units[h]['label'].extend([g['name']])
        if isinstance(j['spatialUnit'], dict):
            gjson1.append(j['spatialUnit'])
            for h in j['hydroUnit']:
                sg.append([j['id'], g['id'], h])
                hydro_units[h]['value'].extend([g['id']])
                hydro_units[h]['label'].extend([g['name']])

#    for gj in gjson1:
#        if gj['id'] == 'GWAZ0037':
#            gj['color'] = 'rgb(204, 204, 204)'
#        else:
#            gj['color'] = plotly.colors.qualitative.Vivid[np.random.randint(0, 11)]

    gpd1 = pd.DataFrame(gjson1).dropna()
    gpd1['geometry'] = gpd1['wkt'].apply(wkt.loads)
    gpd2 = gpd.GeoDataFrame(gpd1, geometry='geometry', crs=2193).drop('wkt', axis=1).to_crs(4326).set_index('id')
    gpd2['geometry'] = gpd2.simplify(0.001)

    sg_df = pd.DataFrame(sg)
    sg_df.columns = ['id', 'spatialId', 'HydroGroup']
    sg_df = sg_df[sg_df.spatialId.isin(gpd1.id)].copy()

    gjson2 = gpd2.__geo_interface__

    return gjson2, hydro_units, gpd2.reset_index(), sg_df


def process_limit_data(json_lst):
    """

    """
    l_lst1 = []

    for j in json_lst.copy():
        for m in j['managementUnit']:
            for l in m['limit']:
                l['id'] = j['id']
                l['Allocation Block'] = m['parameterName']
                l['units'] = m['units']
                l_lst1.append(l)

    l_data = pd.DataFrame(l_lst1)

    units = l_data[['id', 'units']].drop_duplicates()

    index1 = ['id', 'units', 'Allocation Block', 'fromMonth']

    ldata0 = l_data.set_index(index1).limit.unstack(3)
    col1 = set(ldata0.columns)
    col2 = col1.copy()
    col2.update(range(1, 13))
    new_cols = list(col2.difference(col1))
    ldata0 = ldata0.reindex(columns=ldata0.columns.tolist() + new_cols)
    ldata0.sort_index(axis=1, inplace=True)

    l_data1 = ldata0.ffill(axis=1).stack()
    l_data1.name = 'Limit'
    l_data1 = l_data1.reset_index()
    l_data1.rename(columns={'fromMonth': 'Month'}, inplace=True)

    ### Summary table
    include_cols = ['id', 'name', 'planName', 'planSection', 'planTable']
    t_lst = []
    for d in json_lst.copy():
        dict1 = {key: val for key, val in d.items() if key in include_cols}
        t_lst.append(dict1)

    t_data = pd.DataFrame(t_lst)

    t_data1 = pd.merge(t_data, l_data, on='id')
#    t_data1.rename(columns={'SpatialUnitName': 'Allocation Zone'}, inplace=True)
    t_data1.replace({'fromMonth': month_map, 'toMonth': month_map}, inplace=True)

    ### Return
    return l_data1, t_data1, units


def assign_notes(sg_df):
    """

    """
    ### Label joint s units
    sp2c = sg_df.drop_duplicates(subset=['id', 'spatialId']).copy()
    sp2c['joint_units'] = sp2c.groupby('id').spatialId.transform(lambda x: ', '.join(x))
    sp2c['unit_count'] = sp2c.groupby('id').spatialId.transform('count')
    sp2c.loc[sp2c['unit_count'] == 1, 'joint_units'] = ''
    sp2d = sp2c.set_index(['id', 'spatialId', 'HydroGroup'])['joint_units']

    ### Label joint hydro groups
    sp2e = sg_df.drop_duplicates(subset=['HydroGroup', 'id']).copy()
    sp2e['hydro_count'] = sp2e.groupby('id').spatialId.transform('count')
    sp2f = sp2e.set_index(['id', 'spatialId', 'HydroGroup'])['hydro_count']
#    sp2c.loc[sp2c['hydro_count'] == 1, 'joint_hydro'] = ''

    ## Join joint_hydro to main table
    sp3 = sg_df.set_index(['id', 'spatialId', 'HydroGroup'])
    sp4 = pd.concat([sp3, sp2d, sp2f], axis=1).reset_index()
    sp4.loc[sp4.hydro_count.isnull(), 'hydro_count'] = 1
    sp4.loc[sp4.joint_units.isnull(), 'joint_units'] = ''

    ## Create notes
#    front_note = '**Notes:**  '
    joint_hydro_notes = 'Allocation limits for this allocation zone are combined between surface water and groundwater.'
    joint_units_notes = 'The allocation limits are shared jointly across these zones: '
#    note_template = """{begin}
#    {hydro}
#    {unit_notes}{units}
#    """
    note_template = """{hydro} {unit_notes}{units}"""
    sp4['notes'] = ''

    cond1 = sp4.hydro_count > 1
    sp4.loc[cond1, 'notes'] = note_template.format(hydro=joint_hydro_notes, unit_notes='', units='')

    cond2 = (sp4.joint_units != '')
    sp4.loc[cond2, 'notes'] = sp4.loc[cond2, 'joint_units'].apply(lambda x:     note_template.format(hydro='', unit_notes=joint_units_notes, units=x))

    cond3 = (sp4.hydro_count > 1) & (sp4.joint_units != '')
    sp4.loc[cond3, 'notes'] = sp4.loc[cond3, 'joint_units'].apply(lambda x:     note_template.format(hydro=joint_hydro_notes, unit_notes=joint_units_notes, units=x))

    return sp4.drop(['hydro_count', 'joint_units'], axis=1)














