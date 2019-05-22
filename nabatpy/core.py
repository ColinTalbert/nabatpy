# -*- coding: utf-8 -*-
"""
#############################################################################
    _   _____    ____        __
   / | / /   |  / __ )____ _/ /_____  __  __
  /  |/ / /| | / __  / __ `/ __/ __ \/ / / /
 / /|  / ___ |/ /_/ / /_/ / /_/ /_/ / /_/ /
/_/ |_/_/  |_/_____/\__,_/\__/ .___/\__, /
                            /_/    /____/

 Python Tools for accessing and manipulating North American Bat Monitoring data

 Github: https://github.com/talbertc-usgs/NABatpy
 Written by: Colin B Talbert
 Created: 2018-10-31
#############################################################################
"""

from owslib.wfs import WebFeatureService
from owslib.fes import *
from owslib.etree import etree

from osgeo import gdal
import geopandas as gpd
import json

from nabatpy.utils import normalize_grid_frame

HASH_DICT = {'Alaska': '5b7b54efe4b0f5d578846149',
             'Canada': '5b7b559de4b0f5d57884614d',
             'Conus': '5b7b563ae4b0f5d57884615b',
             'Hawaii': '5b7b5641e4b0f5d57884615d',
             'Mexico': '5b7b5658e4b0f5d57884615f',
             'PuertoRico': '5b7b5660e4b0f5d578846161'}

URL_TEMPLATE = "https://www.sciencebase.gov/catalogMaps/mapping/ows/{grid_hash}?service=wfs"

# GRTS cells <= these values are the high priority cells (top 5%) for each frame.
PRIORITY_CUTOFFS ={'Alaska': 17142,
                   'Canada': 16964,
                   'Conus': 6714,
                   'Hawaii': 605,
                   'Mexico': 3240,
                   'PuertoRico': 123}


def get_layer_name(response):
    """
    Return the layer name from an owslib response that's not the BB or footprint.

    Parameters
    ----------
    response : owslib response

    Returns
    str layer name
    -------

    """
    return [k for k in response.contents.keys() if k not in ['sb:boundingBox', 'sb:footprint']][0]


def get_grts_data(grid_frame, state='', high_priority=False):
    """
    Returns a geodataframe of the selected data.

    Parameters
    ----------
    grid_frame : str
                 Grid frame of layer to return: conus, canada, mexico , alaska, hawaii, puertorico
    filters : list of owslib fes queries

    Returns
    -------

    Geopandas geodataframe

    """
    filter = None
    grid_frame = normalize_grid_frame(grid_frame)
    hash = HASH_DICT[grid_frame]

    wfs = WebFeatureService(url=URL_TEMPLATE.format(grid_hash=hash), version='1.1.0')
    layer_name = get_layer_name(wfs)

    if state:
        filters = []
        for i in range(1, 5):
            filters.append(PropertyIsLike(propertyname=f'state_n_{i}', literal=f"*{state}", wildCard="*"))

        filter = Or(operations=filters)
    else:
        filter = None

    if high_priority:
        threshold = PRIORITY_CUTOFFS[grid_frame]
        priority_filter = PropertyIsLessThanOrEqualTo(propertyname='GRTS_ID', literal=str(threshold))

        if filter is not None:
            filter = And(operations=[priority_filter, filter])
        else:
            filter = priority_filter

    if filter is not None:
        # return filter
        xml_filter = etree.tostring(filter.toXML()).decode("utf-8")
        wfs_json = wfs.getfeature(typename=[layer_name], filter=xml_filter,
                                  propertyname=None, srsname='EPSG:4326', outputFormat='application/json').read()
    else:
        wfs_json = wfs.getfeature(typename=[layer_name], propertyname=None,
                                  srsname='EPSG:4326', outputFormat='application/json').read()

    gdf = gpd.GeoDataFrame.from_features(eval(wfs_json))
    gdf.crs = {'init': 'epsg:4326'}

    return gdf
