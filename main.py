# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 13:56:21 2020

@author: MichaelEK
"""
import os
import argparse
import pandas as pd
import numpy as np
import yaml
from delineate_reaches_osm import osm_delineation

#########################################
### Get todays date-time

pd.options.display.max_columns = 10
#run_time_start = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
#print(run_time_start)

########################################
### Read in parameters
print('---Read in parameters')

base_dir = os.path.realpath(os.path.dirname(__file__))

with open(os.path.join(base_dir, 'parameters-dev.yml')) as param:
    param = yaml.safe_load(param)

#parser = argparse.ArgumentParser()
#parser.add_argument('yaml_path')
#args = parser.parse_args()
#
#with open(args.yaml_path) as param:
#    param = yaml.safe_load(param)


########################################
### Run the process

print('---OSM delineation')
gdf1 = osm_delineation(param)





