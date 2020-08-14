""" """

from unittest.mock import patch, MagicMock
import pytest
import logging
from pathlib import Path
import zipfile

import flywheel

import run


def test_create_archive_duplicates_directory_structure():

    content_dir = "/tmp/grp-9-test-oh-boy"
    content_dir_path = Path(content_dir)
    if not content_dir_path.exists():
        content_dir_path.mkdir()
    arcname = content_dir_path.name
    afile_name = "hunky"
    with open(content_dir_path / afile_name, 'w') as afp:
        afp.write("I'm words inside hunky")
    anotherfile_name = "dory"
    with open(content_dir_path / anotherfile_name, 'w') as afp:
        afp.write("I'm words inside dory")
    file_list = [afile_name, anotherfile_name]

    return_path = run._create_archive(content_dir, arcname, file_list)

    test_dir = Path("/tmp/grp-9-test-result")
    if not test_dir.exists():
        test_dir.mkdir()
    zipfilepath = Path(content_dir + ".zip")
    with zipfile.ZipFile(zipfilepath, 'r', allowZip64=True) as zf:
        zf.extractall(test_dir)
    assert (test_dir / "hunky").exists()
    assert (test_dir / "dory").exists()
