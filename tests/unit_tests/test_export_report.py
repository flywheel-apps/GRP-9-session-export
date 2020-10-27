# import pytest
import sys
from pathlib import Path
from unittest.mock import MagicMock

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

