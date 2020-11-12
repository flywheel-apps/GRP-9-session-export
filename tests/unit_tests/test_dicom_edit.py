import filecmp
import shutil

import pydicom
from pydicom.data import get_testdata_files
from pydicom.dataelem import RawDataElement
from pydicom.tag import Tag

from dicom_edit import *
from dicom_metadata import get_pydicom_header


def test_write_dcm_to_tempfile():
    dcm_path = get_testdata_files("MR_small.dcm")[0]
    dcm = pydicom.dcmread(dcm_path)
    write_dcm_to_tempfile(dcm)


def test_character_set_callback():
    raw_elem = RawDataElement(Tag(0x00080005), "UN", 14, b"iso8859", 770, False, True,)
    raw_elem_fix = character_set_callback(raw_elem)
    assert raw_elem_fix.VR == "CS"


def test_can_update_dicom():
    dcm_path = get_testdata_files("MR_small.dcm")[0]
    fw_pydicom_kwargs = get_dicom_save_config_kwargs(dcm_path)
    assert can_update_dicom(dcm_path, {"PatientID": "Flywheel"}, fw_pydicom_kwargs)
    assert can_update_dicom(dcm_path, {"PatientID": 2}, fw_pydicom_kwargs) is False
    assert can_update_dicom(dcm_path, {"NotaTag": 2}, fw_pydicom_kwargs) is False
    # test unsaveable file returns None
    assert get_dicom_save_config_kwargs("does_not_exist.dcm") is None
    assert can_update_dicom(dcm_path, {"PatientID": "Flywheel"}, None) is False


def test_edit_dicom():
    dcm_path = get_testdata_files("MR_small.dcm")[0]
    dcm = pydicom.dcmread(dcm_path)
    with tempfile.NamedTemporaryFile(suffix=".dcm") as tempf:
        dcm.save_as(tempf.name)
        assert edit_dicom(tempf.name, {"PatientID": "Flywheel"})
        assert edit_dicom(tempf.name, {"PatientID": 2}) is None
        assert edit_dicom("does_not_exist.dcm", {"PatientID": "Flywheel"}) is None


def test_dicom_list_updater_valid(caplog):
    caplog.set_level(logging.DEBUG)
    # Test updater with no difference between fw and DICOMs
    dcm_orig_path = get_testdata_files("MR_small.dcm")[0]
    with tempfile.TemporaryDirectory() as tempdir:
        dcm_copy_path = os.path.join(tempdir, os.path.basename(dcm_orig_path))
        shutil.copyfile(dcm_orig_path, dcm_copy_path)
        dcm = pydicom.dcmread(dcm_copy_path)
        header = get_pydicom_header(dcm)
        dcm_updater = DicomUpdater(
            [dcm_copy_path], header, files_log=logging.getLogger("test")
        )
        # Only one file, everything is common
        assert header == dcm_updater.local_common_dicom_dict
        assert dcm_updater.safe_to_update
        assert not any(
            [
                dcm_updater.update_dict,
                dcm_updater.header_diff_dict,
                dcm_updater.non_dicom_paths,
            ]
        )
        assert dcm_updater.update_dicoms() == [dcm_copy_path]
        updated_path = DicomUpdater.update_fw_dicom(dcm_copy_path, header)
        assert updated_path == dcm_copy_path
        assert pydicom.dcmread(updated_path) == dcm
        assert filecmp.cmp(dcm_orig_path, dcm_copy_path)

        # test difference
        update_dict = {
            "PatientSex": "M",
            "PatientWeight": 100,
            "PatientID": "FLYWHEEL",
            "SeriesDescription": "FLYWHEEL",
            "SourceImageSequence": [],
        }
        header.update(update_dict)
        dcm_updater = DicomUpdater(
            [dcm_copy_path], header, files_log=logging.getLogger("test")
        )
        update_dict.pop("SourceImageSequence")
        assert dcm_updater.update_dict == update_dict

        # test archive
        temp_zip_path = os.path.join(tempdir, "test.dicom.zip")
        with zipfile.ZipFile(
            temp_zip_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True
        ) as zip_obj:
            zip_obj.write(dcm_copy_path, os.path.basename(dcm_copy_path))
        DicomUpdater.update_fw_dicom(temp_zip_path, header)
        with tempfile.TemporaryDirectory() as tempdir2:
            with zipfile.ZipFile(temp_zip_path) as zipf:
                extracted_files = [
                    os.path.join(tempdir2, rel_path.filename)
                    for rel_path in zipf.infolist()
                    if not rel_path.is_dir()
                ]
                zipf.extractall(tempdir2)
            dcm_updater = DicomUpdater(
                extracted_files, header, files_log=logging.getLogger("test")
            )
            assert not dcm_updater.update_dict


def test_dicom_list_updater_invalid(caplog):
    # test no common tags
    dcm_test_files_list = [x for x in get_testdata_files() if x.endswith(".dcm")]
    dcm_path = get_testdata_files("MR_small.dcm")[0]
    dcm = pydicom.dcmread(dcm_path)
    header = get_pydicom_header(dcm)
    dcm_updater = DicomUpdater(
        dcm_test_files_list, header, files_log=logging.getLogger("test")
    )
    assert dcm_updater.safe_to_update is False

    # test wrong header
    dcm_path = get_testdata_files("rtstruct.dcm")[0]
    dcm_updater = DicomUpdater([dcm_path], header, files_log=logging.getLogger("test"))
    assert not dcm_updater.safe_to_update

    # test update fails
    dcm_orig_path = get_testdata_files("MR_small.dcm")[0]
    with tempfile.TemporaryDirectory() as tempdir:
        update_dict = {"PatientSex": "M"}
        header.update(update_dict)
        dcm_copy_path = os.path.join(tempdir, os.path.basename(dcm_orig_path))
        shutil.copyfile(dcm_orig_path, dcm_copy_path)
        dcm_updater = DicomUpdater(
            [dcm_copy_path], header, files_log=logging.getLogger("test")
        )
        assert dcm_updater.update_dict
        dcm_updater.dicom_dict_list.append({"path": "does_not_exist.dcm"})
        assert len(dcm_updater.update_dicoms()) == 1
