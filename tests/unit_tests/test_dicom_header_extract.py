import json
from pathlib import Path
import os
import tempfile
import zipfile

import pydicom
from pydicom.data import get_testdata_files

from dicom_metadata import dicom_header_extract, select_matching_file

DATA_ROOT = Path(__file__).parents[1] / 'data'


def test_dicom_header_extract_valid_dcm():
    header_json_path = DATA_ROOT / 'test_dicom_header1.json'
    with open(header_json_path) as f_data:
        exp_header = json.load(f_data)
    test_dicom_path = get_testdata_files('MR_small.dcm')[0]
    return_header = dicom_header_extract(test_dicom_path, dict())
    assert exp_header == return_header


def test_dicom_header_extract_rtstruct():
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        exp_header = json.load(f_data)
    test_dicom_path = get_testdata_files('rtstruct.dcm')[0]
    return_header = dicom_header_extract(test_dicom_path, dict())
    assert exp_header == return_header


def test_dicom_header_extract_empty_file():
    exp_header = dict()
    with tempfile.NamedTemporaryFile() as f:
        return_header = dicom_header_extract(f.name, dict())
    assert exp_header == return_header


def test_dicom_header_extract_non_dicom():
    exp_header = dict()
    json_path = DATA_ROOT / 'test_dicom_header1.json'
    return_header = dicom_header_extract(json_path, dict())
    assert exp_header == return_header


def test_select_matching_file_extract():
    header_json_path = DATA_ROOT / 'test_dicom_header1.json'
    dicom_zip_path = DATA_ROOT / 'test_dicom.zip'

    with open(header_json_path) as f_data:
        exp_header = json.load(f_data)
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(dicom_zip_path) as zip_obj:
            zip_obj.extractall(temp_dir)
            dcm_path_list = Path(temp_dir).rglob('*')
            # keep only files
            dcm_path_list = [str(path) for path in dcm_path_list if path.is_file()]
        exp_header['InstanceNumber'] = 7
        return_path = select_matching_file(dcm_path_list, exp_header)
        assert return_path == os.path.join(temp_dir, 'MR_small_7.dcm')
    return_header = dicom_header_extract(dicom_zip_path, exp_header)
    assert exp_header == return_header
