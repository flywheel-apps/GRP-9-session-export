# import pytest
import sys
from pathlib import Path
import pytest
import queue
from random import getrandbits as rand
from unittest.mock import MagicMock
from contextlib import nullcontext as does_not_raise

import export_report
from util import hash_value
import flywheel


def test_setup(sdk_mock, analysis):

    # Setup mocks
    sdk_mock.get_analysis.return_value = analysis
    sdk_mock.get_session.return_value = flywheel.Session(id="test_copy")
    sdk_mock.lookup.return_value = flywheel.Project(id="test_proj")
    # Create mock project and mock finder for mock project
    project_mock = MagicMock(spec=[*dir(flywheel.Project), "sessions"])
    sdk_mock.get_project.return_value = project_mock()
    project_mock.return_value.sessions.find_first.return_value = flywheel.Session(
        id="test_copy"
    )

    source, dest = export_report.setup("test", sdk_mock)

    # mock call assertions
    sdk_mock.get_analysis.assert_called_once_with("test")
    sdk_mock.get_session.assert_called_once_with("test_session")
    sdk_mock.lookup.assert_called_once_with("test_proj")
    sdk_mock.get_project.assert_called_once_with("test_proj")
    project_mock.return_value.sessions.find_first.assert_called_once_with(
        f'info.export.origin_id={hash_value("test_copy")}'
    )


@pytest.fixture
def exp(sdk_mock):
    src = flywheel.Subject(label="test")
    dest = flywheel.Subject(label="test-2")
    return export_report.ExportComparison(src, dest, sdk_mock)


def test_init(sdk_mock):
    src = flywheel.Subject(label="test")
    dest = flywheel.Subject(label="test-2")

    export = export_report.ExportComparison(src, dest, sdk_mock)

    assert isinstance(export.diffs, export_report.DiffRecord)
    assert isinstance(export.queue, queue.Queue)
    assert export.s_cont == src
    assert export.d_cont == dest
    assert export.c_type == src.container_type


def test_init_containers_not_same_type(sdk_mock):
    src = flywheel.Subject(label="test")
    dest = flywheel.Session(label="test-2")
    with pytest.raises(SystemExit):
        export = export_report.ExportComparison(src, dest, sdk_mock)


@pytest.fixture
def generate_finder(mocker, mock_iter_finder):
    def finder(num=5, same=True):
        src = []
        dest = []
        for idx in range(num):
            rand1, rand2 = rand(30), rand(30)
            rand_hash = hash_value(str(rand1)) if same else hash_value(str(rand(30)))
            src_c = flywheel.Session(label="test", id=str(rand1))
            dest_c = flywheel.Session(
                label="test",
                id=str(rand2),
                info={"export": {"origin_id": str(rand_hash)}},
            )
            reload_mock_1 = mocker.patch.object(src_c, "reload")
            reload_mock_1.return_value = src_c
            reload_mock_2 = mocker.patch.object(dest_c, "reload")
            reload_mock_2.return_value = dest_c
            src.append(src_c)
            dest.append(dest_c)
        src_finder = mock_iter_finder(src)
        dest_finder = mock_iter_finder(dest)

        return src_finder, dest_finder

    return finder


class TestCompareChildrenContainers:
    @pytest.mark.parametrize("num", [0, 1, 5])
    @pytest.mark.parametrize("same", [True, False])
    def test_containers(self, exp, generate_finder, mocker, same, num):
        queue_mock = mocker.patch.object(exp, "queue_children")
        record_mock = mocker.patch.object(exp.diffs, "add_record")
        src, dest = generate_finder(num, same=same)

        exp.compare_children_containers(src, dest, "test-sub")
        # queue_mock.assert_called_with

        if not same:
            assert record_mock.call_count == num
            assert queue_mock.call_count == 0
        else:
            assert record_mock.call_count == 0
            assert queue_mock.call_count == num


@pytest.fixture
def gen_files():
    def _fn(num=5, same=True):
        src = []
        dest = []
        for idx in range(num):
            hash = hash_value(str(rand(30)))
            hash2 = hash if same else hash_value(str(rand(30)))

            file1 = flywheel.FileEntry(hash=hash)
            file2 = flywheel.FileEntry(hash=hash2)

            src.append(file1)
            dest.append(file2)
        return src, dest

    return _fn


class TestCompareChildrenFiles:
    @pytest.mark.parametrize("num", [0, 1, 5])
    @pytest.mark.parametrize("same", [True, False])
    def test_files(self, mocker, gen_files, exp, same, num):

        record_mock = mocker.patch.object(exp.diffs, "add_record")
        src, dest = gen_files(num, same)
        exp.compare_children_files(src, dest, "test-ses")
        if same:
            assert record_mock.call_count == 0
        else:
            assert record_mock.call_count == num

