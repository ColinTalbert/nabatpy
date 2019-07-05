import pytest
from nabatpy import utils
from nabatpy import grts_lookup

def test_normalize_grid_frame():
    assert utils.normalize_grid_frame('ak') == "Alaska"
    assert utils.normalize_grid_frame('alaska') == "Alaska"
    assert utils.normalize_grid_frame('Alaska') == "Alaska"
    assert utils.normalize_grid_frame('ca') == "Canada"
    assert utils.normalize_grid_frame('can') == "Canada"
    assert utils.normalize_grid_frame('Canada') == "Canada"
    assert utils.normalize_grid_frame('CA') == "Canada"
    assert utils.normalize_grid_frame('hi') == "Hawaii"
    assert utils.normalize_grid_frame('HI') == "Hawaii"
    assert utils.normalize_grid_frame('conus') == "Conus"
    assert utils.normalize_grid_frame('USA') == "Conus"
    assert utils.normalize_grid_frame('us') == "Conus"
    assert utils.normalize_grid_frame('mex') == "Mexico"
    assert utils.normalize_grid_frame('Mexico') == "Mexico"


def test_get_grts():
    assert grts_lookup.get_grts(40.75384858, -113.8450646, sample_frame='conus') == 1005
