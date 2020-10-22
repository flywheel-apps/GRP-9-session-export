import pydicom
from pydicom.data import get_testdata_files
from pydicom.dataelem import RawDataElement
from pydicom.tag import Tag

from dicom_edit import *


def test_write_dcm_to_tempfile():
    dcm_path = get_testdata_files("MR_small.dcm")[0]
    dcm = pydicom.dcmread(dcm_path)
    write_dcm_to_tempfile(dcm)


def test_character_set_callback():
    raw_elem = RawDataElement(
        Tag(0x00080005),
        "UN",
        14,
        b"iso8859",
        770,
        False,
        True,
    )
    raw_elem_fix = character_set_callback(raw_elem)
    assert raw_elem_fix.VR == "CS"


def test_can_update_dicom():
    dcm_path = get_testdata_files("MR_small.dcm")[0]
    fw_pydicom_kwargs = get_dicom_save_config_kwargs(dcm_path)
    assert can_update_dicom(dcm_path, {"PatientID": "Flywheel"}, fw_pydicom_kwargs)
    assert can_update_dicom(dcm_path, {"PatientID": 2}, fw_pydicom_kwargs) is False


def test_edit_dicom():
    dcm_path = get_testdata_files("MR_small.dcm")[0]
    dcm = pydicom.dcmread(dcm_path)
    with tempfile.NamedTemporaryFile(suffix='.dcm') as tempf:
        dcm.save_as(tempf.name)
        assert edit_dicom(tempf.name, {"PatientID": "Flywheel"})
        assert edit_dicom(tempf.name, {"PatientID": 2}) is None