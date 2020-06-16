import json
import os
from pathlib import Path
import shutil
import tempfile
import zipfile

from run import _modify_dicom_archive, _retrieve_path_list
from dicom_metadata import get_dicom_df

DATA_ROOT = Path(__file__).parents[1] / 'data'


def test_modify_valid_key():
    archive_path = DATA_ROOT / 'test_dicom.zip'
    header_json_path = DATA_ROOT / 'test_dicom_header1.json'
    dcm_file_list_tup = _retrieve_path_list(archive_path)
    update_keys = ['Modality', 'ImageType']
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)
    flywheel_dicom_header['Modality'] = 'CT'
    flywheel_dicom_header['ImageType'] = ['ORIGINAL', 'PRIMARY']
    with tempfile.TemporaryDirectory() as tmp_dir:
        arc_copy_path = os.path.join(tmp_dir, os.path.basename(archive_path))
        shutil.copyfile(archive_path, arc_copy_path)
        output_path = _modify_dicom_archive(
            arc_copy_path,
            update_keys,
            flywheel_dicom_header,
            dcm_file_list_tup,
            tmp_dir
        )
        with zipfile.ZipFile(output_path) as zip_obj:
            zip_obj.extractall(tmp_dir)

            output_file_list = Path(tmp_dir).rglob('*.dcm')
            output_file_list = [str(path) for path in output_file_list if path.is_file()]
            dcm_df = get_dicom_df(output_file_list, specific_tag_list=update_keys)
            assert dcm_df['Modality'].all() == 'CT'
            assert dcm_df['ImageType'].all() == ['ORIGINAL', 'PRIMARY']


def test_do_not_modify_varying_key():
    archive_path = DATA_ROOT / 'test_dicom.zip'
    header_json_path = DATA_ROOT / 'test_dicom_header1.json'
    dcm_file_list_tup = _retrieve_path_list(archive_path)
    update_keys = ['InstanceNumber']
    with open(header_json_path) as f_data:
        flywheel_dicom_header = json.load(f_data)
    with tempfile.TemporaryDirectory() as tmp_dir:
        arc_copy_path = os.path.join(tmp_dir, os.path.basename(archive_path))
        shutil.copyfile(archive_path, arc_copy_path)
        output_path = _modify_dicom_archive(
            arc_copy_path,
            update_keys,
            flywheel_dicom_header,
            dcm_file_list_tup,
            tmp_dir
        )
        with zipfile.ZipFile(output_path) as zip_obj:
            zip_obj.extractall(tmp_dir)

            output_file_list = Path(tmp_dir).rglob('*.dcm')
            output_file_list = [str(path) for path in output_file_list if path.is_file()]
            dcm_df = get_dicom_df(output_file_list, specific_tag_list=update_keys)
            assert all([x in dcm_df['InstanceNumber'] for x in range(9)])
