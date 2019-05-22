"""
#############################################################
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

 utils.py contains utility functions for data management
#############################################################################
"""

from guano import GuanoFile
from astral import Astral

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

def normalize_grid_frame(grid_frame):
    """
    Given a range of acceptible abbreviations and spellings returns the exact frame name that we need.

    Parameters
    ----------
    grid_frame : str
                 The name of the grid frame that we're trying to match to.

    Returns
    -------
    str - normalized frame name if a match was found
    """
    if grid_frame.lower() in ['ak', 'alaska']:
        return 'Alaska'
    elif grid_frame.lower() in ['ca', 'can', 'canada']:
        return 'Canada'
    elif grid_frame.lower() in ['conus', 'us', 'usa', 'united states']:
        return 'Conus'
    elif grid_frame.lower() in ['hi', 'hawaii']:
        return 'Hawaii'
    elif grid_frame.lower() in ['mex', 'mx', 'mexico']:
        return 'Mexico'
    elif grid_frame.lower() in ['pr', 'puerto rico', 'puertorico']:
        return 'PuertoRico'
    else:
        raise Exception("The specified grid frame name {grid_frame} is not one of 'Alaska', 'Canada', 'Conus', 'Hawaii', 'Mexico', or 'PuertoRico")


def monitoring_night(dt):
    """
    Retrurns the survey/monitoring night of a given date time.
    This is the previous day for times between midnight and dawn.

    Parameters
    ----------
    dt : datetime

    Returns
    -------
    date of the corresponding monitoring night
    """
    if dt.hour < 12:
        monitoringnight = dt.date() - timedelta(days=1)
    else:
        monitoringnight = dt.date()

    return monitoringnight


def parse_nabat_fname(fname):
    """
    Attempts to cleanup (normalize) a NABat Wav file name and parse out the relevant
    bits of information contained in it (GRTS ID, Site name, and datetime of recording).

    Parameters
    ----------
    fname : str of pathlib.Path
            The name of a NABat wav file

    Returns
    -------

    dictionary:  {"GrtsId":str,
                  "SiteName":str,
                  "datetime":python date time object"}

    """

    if isinstance(fname, Path):
        fname = str(fname)

    ofname = fname

    fname = fname.replace('-', '_')
    fname = fname.replace('___', '_')
    fname = fname.replace('__', '_')
    fname = fname.replace('_0_', '_')
    fname = fname.replace('_1_', '_')
    fname = fname.replace('_0+1_', '_')


    f = Path(fname)

    name = f.stem

    if name.lower().startswith('nabat'):
        name = name[5:]
    if name.lower().startswith('naba'):
        name = name[4:]
    if name.startswith('Q'):
        name = name[1:]
    if name.startswith('_'):
        name = name[1:]

    hold_it = name
    if len(name.split('_')) == 2:
        name = f"{f.parent.name}_{name}"

    digit = name[0]
    grtsid = ''
    while digit.isnumeric():
        name = name [1:]
        grtsid += digit
        digit = name[0]

    if name.startswith('_'):
        name = name [1:]
    try:
        sitename, datestr, timestr = name.split('_')
    except:
        print(ofname, hold_it, "name.split failed")

    if len(timestr) > 6:
        print(f"problem time: {timestr}, {fname}")
        timestr = "000000"

    dt = datetime.strptime('T'.join([datestr, timestr]), "%Y%m%dT%H%M%S")
    parts = {"GrtsId":grtsid, "SiteName":sitename, "datetime":dt}
    parts['correct_fname'] =  parts_to_fname(parts)

    return parts


def parts_to_fname(parts):
    dt = parts['datetime']
    return f"{parts['GrtsId']}_{parts['SiteName']}_{parts['datetime'].year}{dt.month:02d}{dt.day:02d}_{dt.hour:02d}{dt.minute:02d}{dt.second:02d}.wav"


# get_site_coords
def get_latlon(fname):
    p = Path(fname)
    try:
        df = pd.read_csv([f for f in p.parent.parent.glob('*Summary.csv')][0])
    except:
        df = pd.read_csv([f for f in p.parent.parent.glob('*Summary.txt')][0], delimiter='\t')
        if df.shape[1] ==1:
            df = pd.read_csv([f for f in p.parent.parent.glob('*Summary.txt')][0], delimiter=',')

    row = df.iloc[0]
    return f"{row['LAT']},-{row['LON']}"


def time_to_timestr(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def get_auto_times(fname, geocoder='Denver'):
    parts = parse_nabat_fname(fname)

    a = Astral()
    a.solar_depression = 'civil'
    denver = a.geocoder[geocoder]

    montitoringnight = monitoring_night(parts['datetime'])
    monitoringnight_sun = denver.sun(date=montitoringnight, local=True)
    monitoringmorn_sun = denver.sun(date=montitoringnight + timedelta(days=1), local=True)

    sunset = monitoringnight_sun['sunset']
    dusk = monitoringnight_sun['dusk']

    dawn = monitoringmorn_sun['dawn']
    sunrise = monitoringmorn_sun['sunrise']

    return sunset + timedelta(minutes=15), sunrise - timedelta(minutes=15)


def update_single_md(fname, guano_md={}, project_md={}, site_md={}):
    g = GuanoFile(fname)

    parts = parse_nabat_fname(fname)
    # content from the NABat filename
    g['NABat', 'Grid Cell GRTS ID'] = parts['GrtsId']
    g['NABat', 'Site Name'] = parts['SiteName']


    #content from the site_md
    if guano_md is None:
        g['GUANO', 'time_to_time'] = str(parts['datetime'])
    else:
        for k, v in guano_md.items():
            g['GUANO', k] = v

    #content from the project_md
    if project_md is not None:
        for k, v in project_md.items():
            g['NABat', k] = v

    #content from the site_md
    if site_md is not None:
        for k, v in site_md.items():
            g['NABat', k] = v

    g.write(make_backup=False)


def bulkupload_to_df(bulk_upload_csv):
    """
    Converts an NABat bulk upload csv into a workable Pandas dataframe

    Parameters
    ----------
    bulk_upload_csv: str or pathlib.Path
           the location on the local file system of the input csv in NABat bulk upload format

    Returns
    -------

    pandas dataframe
    """

    if type(bulk_upload_csv) == str:
        bulk_upload_csv = Path(bulk_upload_csv)

    assert bulk_upload_csv.exists()

    lookup = pd.read_csv(Path(__file__).parent.joinpath('resources', "NABatColumnRosetta.csv"))

    df = pd.read_csv(bulk_upload_csv)

    if df.iloc[0,0] == 'Integer -Required':
        df.drop(df.index[0])

    df = df.rename(columns=dict(zip(lookup.bulk_upload_columns, lookup.df_columns)))
    df.survey_start_time = pd.to_datetime(df.survey_start_time)
    df.survey_end_time = pd.to_datetime(df.survey_end_time)

    return df


def df_to_bulkupload(df, bulk_upload_csv):
    """
    Converts an NABat bulk upload dataframe into a csv

    Parameters
    ----------
    df: pandas dataframe

    bulk_upload_csv: str or pathlib.Path
           the location on the local file system to write the output csv

    Returns
    -------

    None
    """

    if type(bulk_upload_csv) == str:
        bulk_upload_csv = Path(bulk_upload_csv)

    lookup = pd.read_csv(Path(__file__).parent.joinpath('resources', "NABatColumnRosetta.csv"))

    df = df.rename(columns=dict(zip(lookup.df_columns, lookup.bulk_upload_columns)))
    df.to_csv(bulk_upload_csv, index=False)

    return None