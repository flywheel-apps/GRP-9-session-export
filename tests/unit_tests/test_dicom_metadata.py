import pydicom
import pytest
from pydicom.data import get_testdata_files
import json

from pathlib import Path
from dicom_metadata import assign_type, get_compatible_fw_header, get_pydicom_header


def test_assign_type():
    test_dicom_path = get_testdata_files("MR_small.dcm")[0]
    ds = pydicom.dcmread(test_dicom_path)
    for tag in ds.dir():
        tag_val = ds.get(tag)
        if type(tag_val) != str:
            assert assign_type(tag_val) is not None


def test_get_compatible_fw_header():
    test_header_dict = {
        "SeriesDescription": ["spam", "eggs"],
        "DoesNotExist": "NA",
        "AnatomicRegionSequence": [{"CodeValue": ["spam", "eggs"]}],
    }
    exp_return = {
        "SeriesDescription": "spam\\eggs",
        "DoesNotExist": "NA",
        "AnatomicRegionSequence": [{"CodeValue": "spam\\eggs"}],
    }
    assert get_compatible_fw_header(test_header_dict) == exp_return


exclude_tags = [
    "[Unknown]",
    "PixelData",
    "Pixel Data",
    "[User defined data]",
    "[Protocol Data Block (compressed)]",
    "[Histogram tables]",
    "[Unique image iden]",
]


@pytest.mark.parametrize(
    "in_file,out_file",
    [
        ("MR_small.dcm", "dicom_out_known_good.json"),
        ("CT_small.dcm", "dicom_out_CT_small.json"),
    ],
)
def test_get_pydicom_header_all_tags(in_file, out_file):
    test_dicom_path = get_testdata_files(in_file)[0]
    dcm = pydicom.read_file(test_dicom_path)
    header = get_pydicom_header(dcm)

    headers_that_we_care_about = [
        head
        for head in dcm.dir()
        if (
            head not in exclude_tags and not isinstance(head, pydicom.sequence.Sequence)
        )
    ]
    with open(str(Path(__file__).parents[1] / f"data/{out_file}"), "r") as fp:
        known_good = json.load(fp)
        for key, val in known_good.items():
            if val:
                assert header[key] == val
