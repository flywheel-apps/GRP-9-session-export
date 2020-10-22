import pytest

import flywheel

from unittest.mock import Mock


@pytest.fixture(scope="function")
def get_sdk_mock(mocker):
    spec = dir(flywheel.Flywheel)
    spec.extend(dir(flywheel.Client))
    spec.extend(["api_client", "deid_log"])
    sdk_mock = Mock(spec=spec)
    get_sdk_mock = mocker.patch("flywheel.Client", return_value=sdk_mock)
    return get_sdk_mock


@pytest.fixture(scope="function")
def sdk_mock(get_sdk_mock):
    return get_sdk_mock.return_value
