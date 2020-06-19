import json
import tempfile
from pathlib import Path

import pydicom
import pytest
from pydicom.data import get_testdata_file, get_testdata_files

from dicom_metadata import compare_dicom_headers, dicom_header_extract

DATA_ROOT = Path(__file__).parents[1] / 'data'


def test_known_match(caplog):
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)

    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path, dict())

    update_keys = compare_dicom_headers(local_dicom_header, flywheel_dicom_header)

    assert (update_keys, caplog.records) == ([], [])


def test_dicom_header_list_element_match(caplog):
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)
    # Alter elements to be lists instead of bare values
    flywheel_dicom_header['InstanceNumber'] = [flywheel_dicom_header['InstanceNumber']]
    flywheel_dicom_header['AccessionNumber'] = [flywheel_dicom_header['AccessionNumber']]

    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path, dict())

    # Expected return values
    exp_update_keys = []
    exp_messages = [
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ROIContourSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in RTROIObservationsSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ReferencedFrameOfReferenceSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in StructureSetROISequence is not accounted for.'
    ]

    update_keys = compare_dicom_headers(local_dicom_header, flywheel_dicom_header)
    assert update_keys == exp_update_keys
    for i in range(len(caplog.records)):
        assert caplog.records[i].msg == exp_messages[i]


def test_dicom_header_mismatch(caplog):
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)
    # Alter elements to be lists instead of bare values
    flywheel_dicom_header['InstanceNumber'] = "2"
    flywheel_dicom_header['AccessionNumber'] = 30

    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path, dict())

    # Expected return values
    exp_update_keys = ['AccessionNumber', 'InstanceNumber']
    exp_messages = [
        'Local DICOM header and Flywheel header do NOT match...',
        'MISMATCH in key: AccessionNumber',
        'DICOM    = 1',
        'Flywheel = 30',
        'MISMATCH in key: InstanceNumber',
        'DICOM    = 1',
        'Flywheel = 2',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ROIContourSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in RTROIObservationsSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ReferencedFrameOfReferenceSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in StructureSetROISequence is not accounted for.'
    ]

    update_keys = compare_dicom_headers(local_dicom_header, flywheel_dicom_header)
    
    assert update_keys == exp_update_keys
    for i in range(len(caplog.records)):
        assert caplog.records[i].msg == exp_messages[i]
    

def test_dicom_header_SOPInstanceUID_mismatch(caplog):
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)

    flywheel_dicom_header['SOPInstanceUID'] = flywheel_dicom_header['SOPInstanceUID'].replace(
        '2010020400001',
        '2010020400004'
    )
    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path, dict())

    # Expected return values
    exp_update_keys = ['SOPInstanceUID']
    exp_messages = [
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ROIContourSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in RTROIObservationsSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ReferencedFrameOfReferenceSequence is not accounted for.',
        'Local DICOM header and Flywheel header do NOT match...',
        'WARNING: SOPInstanceUID does not match across the headers of individual dicom files!!!',
        'MISMATCH in key: SOPInstanceUID',
        'DICOM    = 1.2.826.0.1.3680043.8.498.2010020400001',
        'Flywheel = 1.2.826.0.1.3680043.8.498.2010020400004',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in StructureSetROISequence is not accounted for.'
    ]

    update_keys = compare_dicom_headers(local_dicom_header, flywheel_dicom_header)

    assert update_keys == exp_update_keys
    for i in range(len(caplog.records)):
        assert caplog.records[i].msg == exp_messages[i]


def test_dicom_header_insert_invalid_tag(caplog):
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)

    flywheel_dicom_header['SOPInstanceUID_TYPO_F'] = flywheel_dicom_header['SOPInstanceUID'].replace(
        '2010020400001', '2010020400004')

    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path, dict())

    # Expected return values
    exp_update_keys = []
    exp_messages = [
        '%s Dicom data elements were not type fixed based on VM',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ROIContourSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in RTROIObservationsSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ReferencedFrameOfReferenceSequence is not accounted for.',
        'The proposed key, "SOPInstanceUID_TYPO_F", is not a valid DICOM tag. ' +
        'It will not be considered to update the DICOM file.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in StructureSetROISequence is not accounted for.'
    ]

    update_keys = compare_dicom_headers(local_dicom_header, flywheel_dicom_header)

    assert update_keys == exp_update_keys
    for i in range(len(caplog.records)):
        assert caplog.records[i].msg == exp_messages[i]


def test_dicom_header_insert_valid_tag(caplog):
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)

    # Insert a valid DICOM Tag
    flywheel_dicom_header['BitsAllocated'] = 16

    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path, dict())

    exp_update_keys = ['BitsAllocated']
    exp_messages = [
        'MISSING key: BitsAllocated not found in local_header. \n' +
        'INSERTING valid tag: BitsAllocated into local dicom file. ',
        'Local DICOM header and Flywheel header do NOT match...',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ROIContourSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in RTROIObservationsSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ReferencedFrameOfReferenceSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in StructureSetROISequence is not accounted for.'
    ]

    update_keys = compare_dicom_headers(local_dicom_header, flywheel_dicom_header)

    assert update_keys == exp_update_keys
    for i in range(len(caplog.records)):
        assert caplog.records[i].msg == exp_messages[i]


def test_dicom_header_compare_VM_backward_compatibility():
    header_json_path = DATA_ROOT / 'test_dicom_header1.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)
    # old headers represented VM 1-n as single values rather than lists of len 1
    flywheel_dicom_header['WindowWidth'] = 1600
    flywheel_dicom_header['WindowCenter'] = 600
    flywheel_dicom_header['StudyID'] = ['4M', 'R1']
    test_dicom_path = get_testdata_files('MR_small.dcm')[0]
    local_dicom_header = dicom_header_extract(test_dicom_path, dict())
    local_dicom_header['StudyID'] = '4M\\R1'
    assert local_dicom_header.get('WindowWidth') == [1600]
    assert local_dicom_header.get('WindowCenter') == [600]
    update_keys = compare_dicom_headers(local_dicom_header, flywheel_dicom_header)
    assert 'WindowWidth' not in update_keys
    assert 'WindowCenter' not in update_keys
    assert 'StudyID' not in update_keys
