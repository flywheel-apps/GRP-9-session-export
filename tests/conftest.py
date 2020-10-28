import pytest

import flywheel

from unittest.mock import Mock


@pytest.fixture(scope="function")
def analysis():
    return flywheel.models.analysis_output.AnalysisOutput(
        id="test",
        job=flywheel.models.Job(config={"config": {"export_project": "test_proj"}}),
        gear_info={
            "category": "analysis",
            "id": "test_gear",
            "name": "test",
            "version": "0.0.1",
        },
        parent=flywheel.models.container_reference.ContainerReference(
            id="test_session", type="session"
        ),
    )


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


class MockFinder:
    def __init__(self, arr):
        self.arr = arr

    def iter(self):
        for x in self.arr:
            yield x

    def iter_find(self):
        for x in self.arr:
            yield x

    def __len__(self):
        return len(self.arr)

    def __call__(self):
        return self.arr

@pytest.fixture
def mock_iter_finder():
    def _fn(arr):
        return MockFinder(arr)

    return _fn

@pytest.fixture
def mock_finder(
    container=None,
    finder_type="project",
    methods={"find": [], "find_one": None, "find_first": None},
):
    """[summary]

    Args:
        container (unittest.mock.MagicMock, optional): Existing container mock. Defaults to None
        finder_type (str, optional): Finder type, one of {project, subject, session.} Defaults to 'project'.
        methods (dict, optional):  Dictionary of finder methods and corresponding return values. Defaults to:
            methods = {
                'find': [],
                'find_one': None,
                'find_first': None
            }

    Returns:
        (flywheel.Project, flywheel.Subject, or flywheel.Session): Mocked container with finder methods mocked to the given return values

    """
    finders = {
        "Project": ["sessions", "subjects", "acquisitions"],
        "Subject": ["sessions", "acquisitions"],
        "Session": ["sessions"],
    }
    if not container:
        container_spec = getattr(flywheel, capitalize(finder_type))
        spec = dir(container_spec)
        spec.extend(finders[capitalize(finder_type)])
        container = MagicMock(spec=spec)

    for name, ret in methods.items():
        finder_method = getattr(getattr(container, finder_type), name)
        finder_method.return_value = ret

    return container
