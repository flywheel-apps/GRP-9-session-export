from unittest.mock import MagicMock

import flywheel
import pytest

from validate import (
    check_exported,
    get_destination,
    get_project,
    validate_context,
    validate_gear_rules,
)


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
    @pytest.mark.parametrize(
        "dest, tags, force, result",
        [
            (flywheel.Subject(id="test"), [], True, True),
            (flywheel.Subject(id="test"), [], False, True),
            (flywheel.Subject(id="test"), ["exported"], True, True),
            (flywheel.Subject(id="test"), ["exported"], False, False),
            (flywheel.Session(id="test"), [], True, True),
            (flywheel.Session(id="test"), [], False, True),
            (flywheel.Session(id="test"), ["exported"], True, True),
            (flywheel.Session(id="test"), ["exported"], False, False),
        ],
    )
    def test_container_exists(self, sdk_mock, dest, tags, force, result):
        dest.tags = tags
        sdk_fn = getattr(sdk_mock, f"get_{dest.__class__.__name__.lower()}")
        sdk_fn.return_value = dest

        errors = []
        out = check_exported(sdk_mock, dest, force, errors)

        sdk_fn.assert_called_once_with("test")
        assert out == result

    @pytest.mark.parametrize(
        "errors,errlen", [([], 1), (["test"], 2), (["test1", "test2"], 3)]
    )
    def test_container_does_not_exist(self, sdk_mock, errors, errlen):
        dest = flywheel.Subject()
        sdk_mock.get_subject.side_effect = flywheel.rest.ApiException()
        out = check_exported(sdk_mock, dest, True, errors)

        assert len(errors) == errlen


@pytest.fixture
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
        gear_context.config = config
        get_proj_mock = mocker.patch("validate.get_project")
        get_proj_mock.return_value = ""

        get_dest_mock = mocker.patch("validate.get_destination")
        get_dest_mock.return_value = ""

        check_exported_mock = mocker.patch("validate.check_exported")
        check_exported_mock.return_value = True

        check_gear_rules_mock = mocker.patch("validate.validate_gear_rules")

        export, archive, dest = validate_context(gear_context)

        assert get_proj_mock.call_count == call_num
        get_dest_mock.assert_called_once_with(gear_context.client, "test", [])
        check_exported_mock.assert_called_once_with(
            gear_context.client, "", config["force_export"], []
        )
        if "check_gear_rules" in config:
            check_gear_rules_mock.assert_called_once_with(gear_context.client, "", [])
        else:
            check_gear_rules_mock.assert_not_called()

    @pytest.mark.parametrize(
        "proj", [None, "proj"],
    )
    def test_errors(self, mocker, gear_context, caplog, proj):
        gear_context.config = {"export_project": proj}

        def get_proj_side_effect(fw, proj, errors):
            return flywheel.Project(
                id="test",
                label="test",
                parents=flywheel.ContainerParents(group="test"),
            )

        def get_dest_side_effect(fw, proj, errors):
            return flywheel.Subject(id="test")

        for api_method in [
            "lookup",
            "get_analysis",
            "get_project_rules",
            "get_session",
            "get_subject",
        ]:
            getattr(
                gear_context.client, api_method
            ).side_effect = flywheel.rest.ApiException
        if not proj is None:
            # Mock get_project to return an actual project
            get_proj_mock = mocker.patch("validate.get_project")
            get_proj_mock.side_effect = get_proj_side_effect

            # Mock get_destination to return an actual container
            get_dest_mock = mocker.patch("validate.get_destination")
            get_dest_mock.side_effect = get_dest_side_effect

        with pytest.raises(SystemExit):
            export, archive, dest = validate_context(gear_context)
            assert all([rec.levelno == logging.ERROR for rec in caplog.get_records()])
            assert len(caplog.record_tuples) == 4
