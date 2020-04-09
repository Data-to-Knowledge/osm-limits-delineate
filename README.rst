Assigning limits to OSM waterways
==================================

This repo contains the python code necessary to delineate the OSM waterways by specified points along the waterways.
The reaches get assigned the unique point ID's which is then linked through to the PlanLimits database.

Installation
-------------
Either install using pip and the requirements file:
  pip install -r requirements.txt

Or install using conda and the env.txt:
  conda create --name osm-delineate --file env.txt

If installed via conda, you'll then need to access the new python environment

The conda installation also includes Spyder so that the scripts can be changed and run via Spyder.

Running the script
------------------
Once in the installed python environment from above, the scripts can be run by:
  python main.py parameters-dev.yml

The parameters-dev.yml file can be changed with the parameters.yml file of choice.
Most of the code that's run is actually contained in the delineate_reaches_osm.py file, the main.py file simply marshals the script.

The conda installation also includes Spyder so that the scripts can be changed and run via Spyder if need be.
