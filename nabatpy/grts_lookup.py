import pandas as pd
from pyproj import Proj, transform
from pathlib import Path

from nabatpy.utils import normalize_grid_frame

# These are the Static parameters used for each of the NABat sampling frames
FRAME_SPECS = {'Conus':{'bounds':[-2363000, 276000, 2267000, 3166000],
                      'crs_metrics':{'proj': 'aea', 'lat_1': 29.5,
                                     'lat_2': 45.5, 'lat_0': 23,
                                     'lon_0': -96, 'x_0': 0, 'y_0': 0,
                                     'datum': 'NAD83', 'units': 'm', 'no_defs': True},
                      'meters':10000},
             'Canada':{'bounds':[-4280000, -730000, 3370000, 3720000],
                    'crs_metrics':{'proj': 'aea', 'lat_1': 55,
                                   'lat_2': 65, 'lat_0': 50,
                                   'lon_0': -100, 'x_0': 0,
                                   'y_0': 0, 'datum': 'NAD83',
                                   'units': 'm', 'no_defs': True},
                    'meters':10000},
             'Alaska':{'bounds':[-4280000, -730000, 3370000, 3720000],
                   'crs_metrics':{'proj': 'aea', 'lat_1': 55,
                                  'lat_2': 65, 'lat_0': 50,
                                  'lon_0': -100, 'x_0': 0,
                                  'y_0': 0, 'datum': 'NAD83',
                                  'units': 'm', 'no_defs': True},
                   'meters':10000},
             'Hawaii':{'bounds':[-370000, 630000, 280000, 1080000],
                   'crs_metrics':{'proj': 'aea', 'lat_1': 8,
                                  'lat_2': 18, 'lat_0': 13,
                                  'lon_0': -157, 'x_0': 0,
                                  'y_0': 0, 'datum': 'NAD83',
                                  'units': 'm', 'no_defs': True},
                   'meters':5000},
             'Mexico':{'bounds':[-1650000, 300000, 1400000, 2400000],
                    'crs_metrics':{'proj': 'aea', 'lat_1': 17,
                                   'lat_2': 30, 'lat_0': 12,
                                   'lon_0': -100, 'x_0': 0,
                                   'y_0': 0, 'datum': 'NAD83',
                                   'units': 'm', 'no_defs': True},
                    'meters':10000},
             'PuertoRico':{'bounds':[-170000, -50000, 230000, 100000],
                   'crs_metrics':{'proj': 'aea', 'lat_1': 17,
                                  'lat_2': 19, 'lat_0': 18,
                                  'lon_0': -66.5, 'x_0': 0,
                                  'y_0': 0, 'datum': 'NAD83',
                                  'units': 'm', 'no_defs': True},
                   'meters':5000},
             }

WGS84 = Proj(init='epsg:4326')

# Augment the specs with some derivative metrics
for which, spec in FRAME_SPECS.items():
    spec['crs'] = Proj(spec['crs_metrics'])
    spec['cols'] = (spec['bounds'][2]-spec['bounds'][0])/spec['meters']
    spec['rows'] = (spec['bounds'][3]-spec['bounds'][1])/spec['meters']


def _load_lookup(which='Conus'):
    fname = Path(__file__).parent.joinpath('resources', 'grts_lookup', f'{which}.csv')
    df = pd.read_csv(fname)
    return df


def get_conus_coords(lat, long, frame_proj):
    return transform(WGS84, frame_proj, long, lat)


def get_grts(lat, long, which='conus'):
    """
    returns the GRTS ID of the cell in which a a coordinate pair lands in.

    Parameters
    ----------
    lat: float
        The latitude (wgs84) of the point
    long: float
        The longitude (wgs84) of the point
    which: str
        Sample frame to look for a match in. ['Alaska', 'Canada', 'Conus', 'Hawaii', 'Mexico', 'PuertoRico']

    Returns
    -------
    int
    """
    which = normalize_grid_frame(which)
    spec = FRAME_SPECS[which]

    if not 'df' in spec:
        spec['df'] = _load_lookup(which)

    x, y = get_conus_coords(lat, long, spec['crs'])
    col = int((x - spec['bounds'][0]) / spec['meters'])
    row = int((y - spec['bounds'][1]) / spec['meters'])
    frame_id = row * spec['cols'] + col + 1

    df = spec['df']

    matching_row = df[df.frame_id==frame_id]['GRTS_ID']
    if matching_row.shape[0] == 0:
        raise Exception(f'The provided coordinates ({lat}, {long}) do not have a match in the {which} frame.')

    grts_id = int(df[df.frame_id==frame_id]['GRTS_ID'])
    return grts_id