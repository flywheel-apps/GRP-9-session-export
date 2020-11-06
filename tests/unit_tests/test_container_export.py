import flywheel
import flywheel_gear_toolkit
from contextlib import nullcontext as does_not_raise
import pytest
from unittest.mock import MagicMock
from container_export import (
    ContainerExporter,
    ContainerHierarchy,
    CONTAINER_KWARGS_KEYS,
    EXCLUDE_TAGS,
)
from util import hash_value


@pytest.fixture
def gear_context(sdk_mock):
    spec = dir(flywheel_gear_toolkit.GearToolkitContext)
    context_mock = MagicMock(spec=spec)
    context_mock.client = sdk_mock
    return context_mock


@pytest.fixture
def container_export(sdk_mock, mocker):
    def gen(exp_proj, archive_proj, origin_cont, config={}, mock=False):
        mocks = dict()
        if mock:
            mocks["log"] = mocker.patch("container_export.ExportLog")
            mocks["hierarchy"] = mocker.patch(
                "container_export.ContainerExporter.get_hierarchy"
            )

        spec = dir(flywheel_gear_toolkit.GearToolkitContext)
        mocks["context"] = MagicMock(spec=spec)
        mocks["context"].client = sdk_mock
        mocks["context"].config = config
        return (
            ContainerExporter(exp_proj, archive_proj, origin_cont, mocks["context"]),
            mocks,
        )

    return gen


@pytest.fixture
def container_gen():
    def gen(c_type, **kwargs):
        constructor = getattr(flywheel, c_type)
        return constructor(**kwargs)

    return gen


class TestContainerExporter:
    @pytest.mark.parametrize(
        "origin,raises",
        [
            (flywheel.Session(label="origin"), does_not_raise()),
            ("origin", pytest.raises(AttributeError)),
        ],
    )
    def test_init(self, mocker, origin, raises):
        gear_context_mock = MagicMock(
            spec=dir(flywheel_gear_toolkit.GearToolkitContext)
        )
        hierarchy_patch = mocker.patch(
            "container_export.ContainerExporter.get_hierarchy"
        )
        log_patch = mocker.patch("container_export.ExportLog")

        exporter = None
        with raises:
            exporter = ContainerExporter("export", "archive", origin, gear_context_mock)

        hierarchy_patch.assert_called_once_with(origin)

        # Validate attributes if exporter is called
        if hasattr(origin, "container_type"):
            log_patch.assert_called_once_with("export", "archive")

            for attr in [
                "status",
                "_log",
            ]:
                assert getattr(exporter, attr) is None

            assert exporter.gear_context == gear_context_mock
            assert exporter.origin_container == origin
            assert exporter.container_type == origin.container_type

    @pytest.mark.parametrize(
        "origin", [flywheel.Subject(code="origin"), flywheel.Session(label="origin")]
    )
    def test_from_gear_context(self, mocker, origin):
        gear_context_mock = MagicMock(
            spec=dir(flywheel_gear_toolkit.GearToolkitContext)
        )
        log_patch = mocker.patch("container_export.ExportLog")
        hierarchy_patch = mocker.patch(
            "container_export.ContainerExporter.get_hierarchy"
        )

        validate_patch = mocker.patch("container_export.validate_context")
        export_proj = flywheel.Project(label="export")
        archive_proj = (flywheel.Project(label="archive"),)
        validate_patch.return_value = [
            export_proj,
            archive_proj,
            origin,
        ]

        exporter = ContainerExporter.from_gear_context(gear_context_mock)

        assert exporter.origin_container == origin
        log_patch.assert_called_once_with(export_proj, archive_proj)
        hierarchy_patch.assert_called_once_with(origin)

    def test_log(self, mocker, container_export):
        export, mocks = container_export("test", "test", flywheel.Session(), mock=True)
        log_mock = mocker.patch("container_export.logging.getLogger")

        log = export.log
        log_mock.assert_called_once_with("GRP-9 Session None Export")

    @pytest.mark.parametrize(
        "container,exp",
        [
            (flywheel.Session(), "test-None_export_log.csv"),
            (flywheel.Subject(), "test_export_log.csv"),
        ],
    )
    def test_csv_path(self, mocker, container_export, container, exp):
        export, mocks = container_export("test", "test", container, mock=True)
        mocks["hierarchy"].return_value.subject.label = "test"
        mocks["context"].output_dir = "/tmp/gear"

        path = export.csv_path

        assert path == f"/tmp/gear/{exp}"

    def test_get_hierarchy(self, mocker, container_export):
        hierarchy_mock = mocker.patch(
            "container_export.ContainerHierarchy.from_container"
        )
        log_mock = mocker.patch("container_export.ExportLog")
        export, mocks = container_export("test", "test", flywheel.Session())

        hierarchy_mock.assert_called_once_with(
            mocks["context"].client, flywheel.Session()
        )

    @pytest.mark.parametrize("info", [{"test": "test"}, {"test": None}, {}])
    @pytest.mark.parametrize(
        "ctype,other", [("Session", {"age": "10"}), ("Subject", {"sex": "F"})]
    )
    def test_get_create_container_kwargs(
        self, mocker, container_gen, info, ctype, other
    ):
        container = container_gen(ctype, id="test", info=info, **other)
        out = ContainerExporter.get_create_container_kwargs(container)

        assert all([key in out for key in other.keys()])
        assert out.get("info").get("export").get("origin_id") == hash_value("test")
        info.update({"export": {"origin_id": hash_value("test")}})
        assert info == out.get("info")

    @pytest.mark.parametrize(
        "container,label",
        [
            (
                flywheel.Session(id="test"),
                (f"info.export.origin_id={hash_value('test')}",),
            ),
            (flywheel.Subject(label="test", code="test"), ("label=test", "code=test"),),
            (flywheel.Subject(label="5", code="5"), ('label="5"', 'code="5"'),),
        ],
    )
    def test_get_container_find_queries(self, container, label):

        queries = ContainerExporter.get_container_find_queries(container)

        assert queries == label

    @pytest.mark.parametrize("same", [True, False])
    @pytest.mark.parametrize(
        "origin,export,parent,par_type",
        [
            (
                flywheel.Session(id="test"),
                flywheel.Session(
                    id="test2", info={"export": {"origin_id": hash_value("test")}}
                ),
                MagicMock(spec=dir(flywheel.Subject).extend("sessions")),
                "subject",
            ),
            (
                flywheel.Subject(label="test"),
                flywheel.Subject(label="test"),
                MagicMock(spec=dir(flywheel.Project).extend("subjects")),
                "project",
            ),
        ],
    )
    def test_find_container_copy(self, origin, export, parent, mocker, par_type, same):
        parent.container_type = par_type
        parent.id = "test_parent"
        export.parents = flywheel.ContainerParents(**{par_type: parent.id})
        origin.parents = flywheel.ContainerParents(
            **{par_type: parent.id if same else "test_parent2"}
        )

        finder_mock = getattr(parent, f"{export.container_type}s").find_first
        finder_mock.return_value = export

        out = ContainerExporter.find_container_copy(origin, parent)
        if not same:
            if par_type == "project":
                finder_mock.assert_called_once_with("label=test")
            else:
                finder_mock.assert_called_once_with(
                    f"info.export.origin_id={hash_value('test')}"
                )
        else:
            assert out == origin

    @pytest.mark.parametrize(
        "origin,parent",
        [(flywheel.Session(id="test"), MagicMock(spec=dir(flywheel.Subject)))],
    )
    def test_create_container_copy(self, origin, parent):
        ContainerExporter.create_container_copy(origin, parent)

