from validate import (
    get_project,
    get_destination,
    validate_gear_rules,
    check_exported,
    validate_context,
)
import flywheel
import pytest
from unittest.mock import MagicMock


class TestGetProject:
    def test_project_exists(self, sdk_mock):
        test_proj = "test/test_proj"
        sdk_mock.lookup.return_value = flywheel.Project(label=test_proj)
        errors = []

        proj = get_project(sdk_mock, test_proj, errors)
        sdk_mock.lookup.assert_called_once_with(test_proj)

        assert isinstance(proj, flywheel.Project)
        assert proj.label == test_proj
        assert errors == []

    @pytest.mark.parametrize(
        "errors,length", [([], 1), (["test"], 2), (["test1", "test2"], 3)]
    )
    def test_project_does_not_exist(self, sdk_mock, errors, length):
        test_proj = "test/test_proj"
        sdk_mock.lookup.side_effect = flywheel.rest.ApiException()
        sdk_mock.lookup.return_value = None

        proj = get_project(sdk_mock, test_proj, errors)

        assert len(errors) == length
        assert proj == None


class TestGetDestination:
    @pytest.mark.parametrize(
        "parent,errlen",
        [
            (flywheel.Subject(label="test"), 0),
            (flywheel.Session(label="test"), 0),
            (flywheel.Group(label="test"), 1),
            (flywheel.Project(label="test"), 1),
            (flywheel.Acquisition(label="test"), 1),
        ],
    )
    def test_container(self, sdk_mock, parent, errlen):
        container = flywheel.models.analysis_output.AnalysisOutput(
            parent=parent, id="test"
        )
        sdk_mock.get_analysis.return_value = container

        errors = []

        dest = get_destination(sdk_mock, "test", errors)

        assert len(errors) == errlen
        sdk_mock.get_analysis.assert_called_once_with("test")
        # assert dest.__class__ == parent.__class__
        assert isinstance(dest, parent.__class__)

    @pytest.mark.parametrize(
        "errors,length", [([], 1), (["test"], 2), (["test1", "test2"], 3)]
    )
    def test_errors(self, sdk_mock, errors, length):
        container = flywheel.models.analysis_output.AnalysisOutput(
            parent=flywheel.Project(), id="test"
        )
        sdk_mock.get_analysis.return_value = container
        dest = get_destination(sdk_mock, "test", errors)

        assert len(errors) == length
        #        assert dest.__class__ == flywheel.Project().__class__
        assert isinstance(dest, flywheel.Project)

    def test_analysis_does_not_exist(self, sdk_mock):

        sdk_mock.get_analysis.side_effect = flywheel.rest.ApiException()
        sdk_mock.get_analysis.return_value = None
        errors = []

        dest = get_destination(sdk_mock, "test", errors)

        assert len(errors) == 1
        assert dest is None


class TestValidateGearRules:
    @pytest.mark.parametrize(
        "rules,errlen",
        [
            ([flywheel.Rule(disabled=True)], 0),
            ([], 0),
            # (None, 0), # Assume get_project_rules returns [] and not None
            ([flywheel.Rule(disabled=False)], 1),
        ],
    )
    def test_gear_rules(self, sdk_mock, rules, errlen):
        sdk_mock.get_project_rules.return_value = rules
        project = flywheel.Project(
            id="test", label="test", parents=flywheel.ContainerParents(group="test")
        )
        errors = []
        validate_gear_rules(sdk_mock, project, errors)

        sdk_mock.get_project_rules.assert_called_once_with(project.id)
        assert len(errors) == errlen

    @pytest.mark.parametrize(
        "errors,length", [([], 1), (["test"], 2), (["test1", "test2"], 3)]
    )
    def test_errors(self, sdk_mock, errors, length):
        project = flywheel.Project(
            id="test", label="test", parents=flywheel.ContainerParents(group="test")
        )
        sdk_mock.get_project_rules.return_value = [flywheel.Rule(disabled=False)]
        validate_gear_rules(sdk_mock, project, errors)

        assert len(errors) == length

class TestCheckExported:
    @pytest.mark.parametrize('tags, force, return',[
      ([], True, True)
      ([], True, True)
      (['exported'], True, True)
      (['exported'], False, False)
    ])
    def test_force_export(self, sdk_mock, tags, force, return):
        sdk_mock.
