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

column_lookup = pd.read_csv(Path(__file__).parent.joinpath('resources', "NABatColumnRosetta.csv"))
row_lookup = column_lookup[['bulk_upload_columns', 'df_columns', 'guano_tag2']].dropna()
row_lookup_v2 = column_lookup[['df_columns', 'guano_tag2']].dropna()


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

    grtsid = grtsid.lstrip("0")


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
    parts['correct_fname'] = parts_to_fname(parts)

    return parts


def parts_to_fname(parts):
    dt = parts['datetime']
    return f"{parts['GrtsId']}_{parts['SiteName']}_{parts['datetime'].year}{dt.month:02d}{dt.day:02d}_{dt.hour:02d}{dt.minute:02d}{dt.second:02d}.wav"


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

    df = pd.read_csv(bulk_upload_csv, parse_dates=['Survey Start Time', 'Survey End Time'])

    if df.iloc[0,0] == 'Integer -Required':
        df.drop(df.index[0])

    upload_columns = column_lookup[['bulk_upload_columns', 'df_columns']].dropna()

    df = df.rename(columns=dict(zip(upload_columns.bulk_upload_columns,
                                    upload_columns.df_columns)))

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


def generate_bulkupload(source_dname, redo_all=False, recursive=True, verbose=1):

    d = Path(source_dname)
    if redo_all:
        if d.joinpath('_problems.csv').exists():
            d.joinpath('_problems.csv').unlink()
        if d.joinpath('_batchupload.csv').exists():
            d.joinpath('_batchupload.csv').unlink()

    df, problems = guano_to_df(source_dname, recursive=False, verbose=verbose)

    if recursive:
        for thing in d.glob('*'):
            if thing.is_dir():
                sub_df, sub_problems = generate_bulkupload(thing, recursive=False, verbose=verbose)
                if df is None and sub_df is not None:
                    df = sub_df
                elif sub_df is not None:
                    df.append(sub_df, sort=False)

                if problems is None and sub_problems is not None:
                    problems = sub_problems
                elif sub_problems is not None:
                    problems.append(sub_df, sort=False)

    if df is not None:
        df_to_bulkupload(df, d.joinpath('_batchupload.csv'))

    if problems is not None and problems.shape[0] != 0:
        df_to_bulkupload(problems, d.joinpath('_problems.csv'))

    return df, problems


def guano_to_df(source_dname, recursive=True, verbose=1, use_previous=True):
    """Create an NABat bulk upload csv from the MD contained in a folder of wav files
    """
    d = Path(source_dname)

    if use_previous and d.joinpath('_problems.csv').exists():
        problems_df = bulkupload_to_df(d.joinpath('_problems.csv'))
    else:
        problems_df = None

    if use_previous and d.joinpath('_batchupload.csv').exists():
        df = bulkupload_to_df(d.joinpath('_batchupload.csv'))
    else:
        df = None

    if not use_previous or df is None:
        wavs = list(d.glob('*.wav'))
        if len(wavs) > 0:
            if verbose >= 1:
                print(f'Starting on {source_dname}')
            if dask is not None:
                values = [delayed(get_row_from_guano)(wav) for wav in wavs]
                if verbose >= 1:
                    with ProgressBar():
                        results = compute(*values, scheduler='processes')
                else:
                    results = compute(*values, scheduler='processes')
            else:
                results = [get_row_from_guano(wav) for wav in wavs]

            df = pd.DataFrame.from_records(results)
            problems_df = df[df.detector=='Problem extracting row from Guano']
            df = df[df.detector!='Problem extracting row from Guano']

        if recursive:
            for thing in d.glob('*'):
                if thing.is_dir():
                    # print(thing)
                    sub_df, sub_problems = guano_to_df(thing)
                    if df is None:
                        df = sub_df
                    else:
                        # print('else')
                        df = df.append(sub_df, sort=False)

                    if problems_df is None and sub_problems is not None:
                        problems_df = sub_problems
                    elif sub_problems is not None:
                        problems_df = problems_df.append(sub_df, sort=False)

    return df, problems_df


def get_empty_row(version=1):
    row = OrderedDict()

    if version == 2:
        for i, keyvalue in row_lookup_v2.iterrows():
            row[keyvalue.df_columns] = ''
    else:
        for i, keyvalue in row_lookup.iterrows():
            row[keyvalue.df_columns] = ''

    return row


def get_row_from_guano(fname):

    row = get_empty_row()

    try:
        g = GuanoFile(fname)
        for i, keyvalue in row_lookup.iterrows():
            value = g.get(keyvalue.guano_tag2, '')
            if value.lower() == 'nan':
                value = ''
            row[keyvalue.df_columns] = value

        if g.get('NABat|Site coordinates'):
            #     print('.')
            lat, long = g.get('NABat|Site coordinates').split()
            row['latitude'] = lat
            row['longitude'] = long

        # parse the software type from vendor namespaces
        if 'SB' in g.get_namespaces():
            software = 'Sonobat '
            if g.get('SB|Version').startswith('4.2'):
                software += '4.2'
            elif g.get('SB|Version').startswith('4.'):
                software += '4.x'
            elif g.get('SB|Version').startswith('3.'):
                software += '3.x'
            row['software_type'] = software
        else:
            # TODO: add logic for Kaleidoscope
            pass

        # Make sure we're using a auto/manual ID if available
        if row['auto_id'] == '':
            row['auto_id'] = g.get('GUANO|Species Auto ID', '')

        if row['manual_id'] == '':
            row['manual_id'] = g.get('GUANO|Species Manual ID', '')

        # convert nan to empty string
        for which in ['auto', 'manual']:
            if row[f'{which}_id'].lower() == 'nan':
                row[f'{which}_id'] = ''
    except:
        # Something went dreadfully wrong. We'll populate with what we have
        parts = parse_nabat_fname(fname)
        row['grts_cell_id'] = parts['GrtsId']
        row['location_name'] = parts['SiteName']
        row['detector'] = "Problem extracting row from Guano"

    row['audio_recording_name'] = Path(fname).name

    return row


def add_monitoringnight(series):
    pass
