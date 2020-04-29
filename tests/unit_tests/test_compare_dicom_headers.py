import json
import tempfile
from pathlib import Path

import pydicom
from pydicom.data import get_testdata_file

from dicom_metadata import compare_dicom_headers, dicom_header_extract

DATA_ROOT = Path(__file__).parents[1] / 'data'


def test_known_match():
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)

    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path)

    headers_match, update_keys, messages = compare_dicom_headers(
        local_dicom_header, flywheel_dicom_header, [])

    assert (headers_match, update_keys, messages) == (True, [], [])


def test_dicom_header_list_element_match():
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)
    # Alter elements to be lists instead of bare values
    flywheel_dicom_header['InstanceNumber'] = [flywheel_dicom_header['InstanceNumber']]
    flywheel_dicom_header['AccessionNumber'] = [flywheel_dicom_header['AccessionNumber']]

    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path)

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

    headers_match, update_keys, messages = compare_dicom_headers(
        local_dicom_header, flywheel_dicom_header, [])

    assert (headers_match, update_keys, messages) == (True, exp_update_keys, exp_messages)


def test_dicom_header_mismatch():
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)
    # Alter elements to be lists instead of bare values
    flywheel_dicom_header['InstanceNumber'] = "2"
    flywheel_dicom_header['AccessionNumber'] = 30

    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path)

    # Expected return values
    exp_update_keys = ['AccessionNumber', 'InstanceNumber']
    exp_messages = [
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

    headers_match, update_keys, messages = compare_dicom_headers(
        local_dicom_header, flywheel_dicom_header, [])

    assert (headers_match, update_keys, messages) == (False, exp_update_keys, exp_messages)


def test_dicom_header_SOPInstanceUID_mismatch():
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)

    flywheel_dicom_header['SOPInstanceUID'] = flywheel_dicom_header['SOPInstanceUID'].replace(
        '2010020400001',
        '2010020400004'
    )
    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path)

    # Expected return values
    exp_update_keys = ['SOPInstanceUID']
    exp_messages = [
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ROIContourSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in RTROIObservationsSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ReferencedFrameOfReferenceSequence is not accounted for.',
        'WARNING: SOPInstanceUID does not match across the headers of individual dicom files!!!',
        'MISMATCH in key: SOPInstanceUID',
        'DICOM    = 1.2.826.0.1.3680043.8.498.2010020400001',
        'Flywheel = 1.2.826.0.1.3680043.8.498.2010020400004',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in StructureSetROISequence is not accounted for.'
    ]

    headers_match, update_keys, messages = compare_dicom_headers(
        local_dicom_header, flywheel_dicom_header, [])

    assert (headers_match, update_keys, messages) == (False, exp_update_keys, exp_messages)


def test_dicom_header_insert_invalid_tag():
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)

    flywheel_dicom_header['SOPInstanceUID_TYPO_F'] = flywheel_dicom_header['SOPInstanceUID'].replace(
        '2010020400001', '2010020400004')

    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path)

    # Expected return values
    exp_update_keys = []
    exp_messages = [
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

    headers_match, update_keys, messages = compare_dicom_headers(
        local_dicom_header, flywheel_dicom_header, [])

    assert (headers_match, update_keys, messages) == (True, exp_update_keys, exp_messages)


def test_dicom_header_insert_valid_tag():
    header_json_path = DATA_ROOT / 'test_dicom_header_rt.json'
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)

    # Insert a valid DICOM Tag
    flywheel_dicom_header['BitsAllocated'] = 16

    test_dicom_path = get_testdata_file('rtstruct.dcm')
    local_dicom_header = dicom_header_extract(test_dicom_path)

    exp_update_keys = ['BitsAllocated']
    exp_messages = [
        'MISSING key: BitsAllocated not found in local_header. \n' +
        'INSERTING valid tag: BitsAllocated into local dicom file. ',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ROIContourSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in RTROIObservationsSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in ReferencedFrameOfReferenceSequence is not accounted for.',
        'Sequence (SQ) Tags are not compared for update. \n' +
        'Any difference in StructureSetROISequence is not accounted for.'
    ]

    headers_match, update_keys, messages = compare_dicom_headers(
        local_dicom_header, flywheel_dicom_header, [])

    assert (headers_match, update_keys, messages) == (False, exp_update_keys, exp_messages)


"""
 More Tests:
 Test Changes in a SQ tag?
"""
