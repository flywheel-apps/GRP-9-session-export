import pytest
from pathlib import Path

from run import _retrieve_path_list


DATA_ROOT = Path(__file__).parents[1] / 'data'

test_zip_file = DATA_ROOT / "test_zip_file.zip"
files_in_zip = ['ziptest/subdir1/zipfile2',
                 'ziptest/subdir1/zipfile1',
                 'ziptest/zipfile1',
                 'ziptest/zipfile2',
                 'ziptest/zipfile3']

test_nonzip_file = DATA_ROOT / "non_zip_file"
files_in_nonzip = [test_nonzip_file.as_posix()]


def test_detect_zipfile():
    file_list, is_zip = _retrieve_path_list(test_zip_file)
    assert(is_zip)
    
    
def test_zipfile_list():
    file_list, is_zip = _retrieve_path_list(test_zip_file)
    assert(file_list == files_in_zip)


def test_detect_nonzipfile():
    file_list, is_zip = _retrieve_path_list(test_nonzip_file)
    assert(not is_zip)


def test_nonzipfile_list():
    file_list, is_zip = _retrieve_path_list(test_nonzip_file)
    assert(file_list == files_in_nonzip)



