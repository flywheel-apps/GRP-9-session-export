from unittest.mock import MagicMock
from contextlib import nullcontext as does_not_raise

import flywheel
import pytest

from validate import (
    container_needs_export,
    get_destination,
    get_project,
    validate_context,
    validate_gear_rules,
)


class TestGetProject:
    def test_project_exists(self, sdk_mock):
        test_proj = "test/test_proj"
        sdk_mock.lookup.return_value = flywheel.Project(label=test_proj)

        proj = get_project(sdk_mock, test_proj)
        sdk_mock.lookup.assert_called_once_with(test_proj)

        assert isinstance(proj, flywheel.Project)
        assert proj.label == test_proj

    # @pytest.mark.parametrize(
    #     "errors,length", [([], 1), (["test"], 2), (["test1", "test2"], 3)]
    # )
    # Can't test backoff easily, or figure out a way
    def test_project_does_not_exist(self, sdk_mock):
        test_proj = "test/test_proj"
        sdk_mock.lookup.side_effect = flywheel.rest.ApiException(status=403)
        sdk_mock.lookup.return_value = None

        with pytest.raises(flywheel.rest.ApiException) as e:
            proj = get_project(sdk_mock, test_proj)
            assert e.status == 403


class TestGetDestination:
    @pytest.mark.parametrize(
        "parent,raising",
        [
            (flywheel.Subject(label="test"), does_not_raise()),
            (flywheel.Session(label="test"), does_not_raise()),
            (flywheel.Group(label="test"), pytest.raises(ValueError)),
            (flywheel.Project(label="test"), pytest.raises(ValueError)),
            (flywheel.Acquisition(label="test"), pytest.raises(ValueError)),
        ],
    )
    def test_container(self, sdk_mock, parent, raising):
        container = flywheel.models.analysis_output.AnalysisOutput(
            parent=parent, id="test"
        )
        sdk_mock.get_analysis.return_value = container
        sdk_mock.get.return_value = parent

        with raising:
            dest = get_destination(sdk_mock, "test")

            sdk_mock.get_analysis.assert_called_once_with("test")
            # assert dest.__class__ == parent.__class__
            assert isinstance(dest, parent.__class__)

    def test_analysis_does_not_exist(self, sdk_mock):
        container = flywheel.models.analysis_output.AnalysisOutput(
            parent=flywheel.Project(), id="test"
        )
        sdk_mock.get.side_effect = flywheel.rest.ApiException(status=404)
        sdk_mock.get_analysis.return_value = container
        with pytest.raises(flywheel.rest.ApiException):
            dest = get_destination(sdk_mock, "test")
            assert isinstance(dest, flywheel.Project)


class TestValidateGearRules:
    @pytest.mark.parametrize(
        "rules,returns",
        [
            ([flywheel.Rule(disabled=True)], True),
            ([], True),
            # (None, 0), # Assume get_project_rules returns [] and not None
            ([flywheel.Rule(disabled=False)], False),
        ],
    )
    def test_gear_rules(self, sdk_mock, rules, returns):
        sdk_mock.get_project_rules.return_value = rules
        project = flywheel.Project(
            id="test", label="test", parents=flywheel.ContainerParents(group="test")
        )
        errors = []
        val = validate_gear_rules(sdk_mock, project)

        sdk_mock.get_project_rules.assert_called_once_with(project.id)
        assert val == returns

    def test_project_doesnt_exist(self, sdk_mock):
        sdk_mock.get_project_rules.side_effect = flywheel.rest.ApiException(status=403)
        project = flywheel.Project(
            id="test", label="test", parents=flywheel.ContainerParents(group="test")
        )
        with pytest.raises(flywheel.rest.ApiException):
            validate_gear_rules(sdk_mock, project)


class TestNeedsExport:
    @pytest.mark.parametrize(
        "dest, tags, force, result",
        [
            (flywheel.Subject(id="test"), [], True, True),
            (flywheel.Subject(id="test"), [], False, True),
            (flywheel.Subject(id="test"), ["EXPORTED"], True, True),
            (flywheel.Subject(id="test"), ["EXPORTED"], False, False),
            (flywheel.Session(id="test"), [], True, True),
            (flywheel.Session(id="test"), [], False, True),
            (flywheel.Session(id="test"), ["EXPORTED"], True, True),
            (flywheel.Session(id="test"), ["EXPORTED"], False, False),
        ],
    )
    def test_container_exists(self, dest, tags, force, result):
        dest.tags = tags

        out = container_needs_export(dest, {"force_export": force})

        assert out == result


@pytest.fixture(scope="function")
def gear_context():
    gc = MagicMock(spec=dir(flywheel.GearContext))
    gc.destination = {"id": "test"}
    return gc


class TestValidateContext:
    @pytest.mark.parametrize(
        "config, call_num",
        [
            (
                {
                    "export_project": "test1",
                    "force_export": True,
                    "check_gear_rules": True,
                },
                1,
            ),
            (
                {
                    "export_project": "test1",
                    "force_export": False,
                    "check_gear_rules": True,
                },
                1,
            ),
            (
                {
                    "export_project": "test1",
                    "archive_project": "test2",
                    "force_export": True,
                },
                2,
            ),
            (
                {
                    "export_project": "test1",
                    "archive_project": "test2",
                    "force_export": False,
                },
                2,
            ),
        ],
    )
    def test_validate_calls(self, mocker, gear_context, config, call_num):
        mock_proj = (
            flywheel.Project(
                label="test",
                parents=flywheel.models.container_parents.ContainerParents(
                    group="test"
                ),
            ),
        )
        gear_context.config = config
        get_proj_mock = mocker.patch("validate.get_project")
        get_proj_mock.return_value = mock_proj

        get_dest_mock = mocker.patch("validate.get_destination")
        get_dest_mock.return_value = flywheel.Subject(label="test")

        check_exported_mock = mocker.patch("validate.container_needs_export")
        check_exported_mock.return_value = True

        check_gear_rules_mock = mocker.patch("validate.validate_gear_rules")
        check_gear_rules_mock.return_value = False

        export, archive, dest = validate_context(gear_context)

        assert get_proj_mock.call_count == call_num
        get_dest_mock.assert_called_once_with(gear_context.client, "test")
        check_exported_mock.assert_called_once_with(
            flywheel.Subject(label="test"), config
        )
        if "check_gear_rules" in config:
            check_gear_rules_mock.assert_called_once_with(
                gear_context.client, mock_proj
            )
        else:
            check_gear_rules_mock.assert_not_called()

    @pytest.mark.parametrize(
        "proj", [None, "proj"],
    )
    def test_errors(self, mocker, sdk_mock, gear_context, caplog, proj):
        gear_context.config = {"export_project": proj}

        def get_proj_side_effect(fw, proj):
            return flywheel.Project(
                id="test",
                label="test",
                parents=flywheel.ContainerParents(group="test"),
            )

        if not proj is None:
            # Mock get_project to return an actual project
            get_proj_mock = mocker.patch("validate.get_project")
            get_proj_mock.side_effect = get_proj_side_effect

        with pytest.raises(SystemExit):
            export, archive, dest = validate_context(gear_context)
            assert all(
                [
                    rec.levelno == logging.ERROR
                    for rec in caplog.get_records(when="teardown")
                ]
            )
