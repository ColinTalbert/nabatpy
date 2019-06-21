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
for sample_frame, spec in FRAME_SPECS.items():
    spec['crs'] = Proj(spec['crs_metrics'])
    spec['cols'] = (spec['bounds'][2]-spec['bounds'][0])/spec['meters']
    spec['rows'] = (spec['bounds'][3]-spec['bounds'][1])/spec['meters']


def _load_lookup(sample_frame='Conus'):
    fname = Path(__file__).parent.joinpath('resources', 'grts_lookup', f'{sample_frame}.csv')
    df = pd.read_csv(fname)
    return df


def transform_coords(x, y, in_proj=WGS84, out_proj=WGS84):
    return transform(in_proj, out_proj, x, y)


def get_grts(lat, long, sample_frame='conus'):
    """
    returns the GRTS ID of the cell in which a a coordinate pair lands in.

    Parameters
    ----------
    lat: float
        The latitude (wgs84) of the point
    long: float
        The longitude (wgs84) of the point
    sample_frame: str
        Sample frame to look for a match in. ['Alaska', 'Canada', 'Conus', 'Hawaii', 'Mexico', 'PuertoRico']

    Returns
    -------
    int
    """
    sample_frame = normalize_grid_frame(sample_frame)
    spec = FRAME_SPECS[sample_frame]

    if 'df' not in spec:
        spec['df'] = _load_lookup(sample_frame)

    x, y = transform_coords(long, lat, out_proj=spec['crs'])
    col = int((x - spec['bounds'][0]) / spec['meters'])
    row = int((y - spec['bounds'][1]) / spec['meters'])
    frame_id = row * spec['cols'] + col + 1

    df = spec['df']

    matching_row = df[df.frame_id==frame_id]['GRTS_ID']
    if matching_row.shape[0] == 0:
        raise Exception(f'The provided coordinates ({lat}, {long}) do not have a match in the {sampling_frame} frame.')

    grts_id = int(df[df.frame_id==frame_id]['GRTS_ID'])
    return grts_id


def get_grts_geometry(grts_id, return_proj='wgs84', return_type='poly', sample_frame='Conus'):
    """

    Parameters
    ----------
    grts_id: int
             The GRTS ID of the cell we want the geometry for
    return_proj: None, proj, str ['wgs84']
            The projection to use for the return geometry
            None = The geometry will be returned in the native frame projection
            A valid proj4 projection will be used for the transform
            If you pass the string 'wgs84' the geometry will be in wgs84
    return_type: str
            'geometry' a shapely geometry will be returned
            'bounds' a list in the format [minx, miny, maxx, maxy] will be returned.
    sample_frame: str
        Sample frame to look for a match in. ['Alaska', 'Canada', 'Conus', 'Hawaii', 'Mexico', 'PuertoRico']

    Returns
    -------

        List or shapely geometry
    """
    sample_frame = normalize_grid_frame(sample_frame)
    spec = FRAME_SPECS[sample_frame]

    if 'df' not in spec:
        spec['df'] = _load_lookup(sample_frame)
    df = spec['df']

    matching_row = df[df.GRTS_ID==grts_id]['frame_id']
    if matching_row.shape[0] == 0:
        raise Exception(f'The provided grts_ID ({grts_id}) does not have a match in the {sample_frame} frame.')
    frame_id = int(matching_row)

    row = int(frame_id / spec['cols'])
    col = int(frame_id % spec['cols']) -1


    min_x = spec['bounds'][0] + (col*spec['meters'])
    min_y = spec['bounds'][1] + (row*spec['meters'])

    max_x = min_x + spec['meters']
    max_y = min_y + spec['meters']

    if return_proj == 'wgs84':
        min_x, min_y = transform_coords(min_x, min_y, in_proj=spec['crs'])
        max_x, max_y = transform_coords(max_x, max_y, in_proj=spec['crs'])
    elif type(return_proj) == Proj:
        min_x, min_y = transform_coords(min_x, min_y, in_proj=spec['crs'], out_proj=return_proj)
        max_x, max_y = transform_coords(max_x, max_y, in_proj=spec['crs'], out_proj=return_proj)
    elif return_proj is not None:
        raise Exception(f'The provided return_proj({return_proj}) must be one of "wgs84" or None, or a valid pyproj.Proj"')

    if return_type == 'bounds':
        return [min_x, min_y, max_x, max_y]

    elif return_type == 'poly':
        from shapely import geometry
        pointlist = [(min_x, min_y), (min_x, max_y), (max_x, max_y), (max_x, min_y)]
        poly = geometry.Polygon(pointlist)
        return poly

    else:
        raise Exception(f'The provided return_type({return_type}) must be one of "bounds" or "poly"')



