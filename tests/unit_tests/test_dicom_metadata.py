import pydicom
from pydicom.data import get_testdata_files

from dicom_metadata import assign_type, get_compatible_fw_header


def test_assign_type():
    test_dicom_path = get_testdata_files("MR_small.dcm")[0]
    ds = pydicom.dcmread(test_dicom_path)
    for tag in ds.dir():
        tag_val = ds.get(tag)
        if type(tag_val) != str:
            assert assign_type(tag_val) is not None


def test_get_compatible_fw_header():
    test_header_dict = {"SeriesDescription": ["spam", "eggs"], "DoesNotExist": "NA"}
    exp_return = {"SeriesDescription": "spam\\eggs", "DoesNotExist": "NA"}
    assert get_compatible_fw_header(test_header_dict) == exp_return
