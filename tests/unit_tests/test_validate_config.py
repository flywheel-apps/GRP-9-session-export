import logging
from contextlib import nullcontext as does_not_raise
from unittest.mock import MagicMock

import flywheel
import pytest
from flywheel.rest import ApiException

from validate import (
    container_needs_export,
    false_if_exc_is_not_found_or_forbidden,
    get_destination,
    get_project,
    validate_context,
    validate_gear_rules,
)


@pytest.mark.parametrize(
    "code,val",
    [
        [400, False],
        [401, True],
        [403, True],
        [404, True],
        [405, True],
        [422, True],
        [423, False],
    ],
)
def test_backoff_giveup_if_exc_not_found_or_forbidden(code, val):
    exc = flywheel.rest.ApiException(status=code)
    ret = false_if_exc_is_not_found_or_forbidden(exc)
    assert ret == val


@pytest.mark.parametrize(
    "raises,exists",
    [(does_not_raise(), True), (pytest.raises(flywheel.rest.ApiException), False),],
)
def test_get_project_exists(sdk_mock, raises, exists, caplog):
    caplog.set_level(logging.DEBUG)
    if exists:
        # SDK mock instance of unittest.mock.MagicMock
        # Mock return value of fw.lookup()
        sdk_mock.lookup.return_value = flywheel.Project(label="test_proj")
    else:
        sdk_mock.lookup.return_value = None
        sdk_mock.lookup.side_effect = flywheel.rest.ApiException(status=404)

    # test
    with raises:
        proj = get_project(sdk_mock, "test/test_proj")

        # assertions
        sdk_mock.lookup.assert_called_once_with("test/test_proj")
        if exists:
            assert isinstance(proj, flywheel.Project)
            assert len(caplog.record_tuples) == 1
            assert caplog.records[0].message == "Found Project test_proj, id None"
            assert proj.label == "test_proj"
        else:
            assert proj is None
            assert len(caplog.record_tuples) == 1
            assert caplog.records[0].message == "Project test/test_proj not found"


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
    def test_validate_calls(self, mocker, gear_context, config, call_num, caplog):
        caplog.set_level(logging.INFO)
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
        check_gear_rules_mock.return_value = True

        export, archive, dest = validate_context(gear_context)

        assert get_proj_mock.call_count == call_num
        get_dest_mock.assert_called_once_with(gear_context.client, "test")
        check_exported_mock.assert_called_once_with(
            flywheel.Subject(label="test"), config
        )
        msgs = [rec.message for rec in caplog.records]
        if "check_gear_rules" in config:
            check_gear_rules_mock.assert_called_once_with(
                gear_context.client, mock_proj
            )
            assert "No enabled rules were found. Moving on..." in msgs
        else:
            check_gear_rules_mock.assert_not_called()
            assert "No enabled rules were found. Moving on..." not in msgs

    @pytest.mark.parametrize(
        "proj",
        [
            {"export_project": flywheel.Project(label="export")},
            {"archive_project": flywheel.Project(label="archvie")},
        ],
    )
    def test_get_proj_errors(self, mocker, sdk_mock, gear_context, caplog, proj):
        gear_context.config = {"export_project": "test", "archive_project": "test"}
        gear_context.config.update(proj)

        def get_proj_side_effect(fw, project):
            if not hasattr(project, "label"):
                raise flywheel.rest.ApiException(status=600)

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

    @pytest.mark.parametrize(
        "to_mock,val,to_err,raises,log",
        [
            (
                ["get_project"],
                [None],
                dict(),
                True,
                "Export project needs to be specified",
            ),
            (
                ["get_project", "validate_gear_rules"],
                [flywheel.Project(label="test"), False],
                dict(),
                True,
                "Aborting Session Export: test has ENABLED GEAR RULES and 'check_gear_rules' == True. If you would like to force the export regardless of enabled gear rules re-run.py the gear with 'check_gear_rules' == False. Warning: Doing so may result in undesired behavior.",
            ),
            (
                ["get_project", "validate_gear_rules", "get_destination"],
                ["test", True, None],
                {"get_destination": ValueError("test")},
                True,
                "Could not find destination with id test",
            ),
            (
                ["get_project", "validate_gear_rules", "get_destination"],
                ["test", True, None],
                {"get_destination": ApiException(status=20)},
                True,
                "Could not find destination with id test",
            ),
            (
                [
                    "get_project",
                    "validate_gear_rules",
                    "get_destination",
                    "container_needs_export",
                ],
                ["test", True, flywheel.Session(label="test"), False],
                dict(),
                True,
                "session test has already been exported and <force_export> = False. Nothing to do!",
            ),
        ],
    )
    def test_errors(
        self, mocker, to_mock, val, to_err, raises, log, caplog, gear_context
    ):
        gear_context.config = {
            "destination": {"id": "test", "container_type": "session", "label": "test"},
            "export_project": "test",
            "archive_project": "test",
            "check_gear_rules": True,
        }
        mocks = {}
        for mock, val in zip(to_mock, val):
            mocks[mock] = mocker.patch(f"validate.{mock}")
            mocks[mock].return_value = val
        for mock, err in to_err.items():
            if mock in mocks:
                mocks[mock].side_effect = err
        if raises:
            my_raise = pytest.raises(SystemExit)
        else:
            my_raise = does_not_raise()
        with my_raise:
            validate_context(gear_context)
