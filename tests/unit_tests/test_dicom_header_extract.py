import json
from pathlib import Path
import tempfile

import pydicom
from pydicom.data import get_testdata_files

from dicom_metadata import dicom_header_extract

DATA_ROOT = Path(__file__).parents[1] / 'data'


def test_dicom_header_extract_valid_dcm():
    header_json_path = DATA_ROOT / 'test_dicom_header1.json'
    with open(header_json_path) as f_data:
        exp_header = json.load(f_data)
    test_dicom_path = get_testdata_files('MR_small.dcm')[0]
    return_header = dicom_header_extract(test_dicom_path)
    assert exp_header == return_header


def test_dicom_header_extract_rtstruct():
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        exp_header = json.load(f_data)
    test_dicom_path = get_testdata_files('rtstruct.dcm')[0]
    return_header = dicom_header_extract(test_dicom_path)
    assert exp_header == return_header


def test_dicom_header_extract_empty_file():
    exp_header = dict()
    with tempfile.NamedTemporaryFile() as f:
        return_header = dicom_header_extract(f.name)
    assert exp_header == return_header


def test_dicom_header_extract_non_dicom():
    exp_header = dict()
    json_path = DATA_ROOT / 'test_dicom_header1.json'
    return_header = dicom_header_extract(json_path)
    assert exp_header == return_header