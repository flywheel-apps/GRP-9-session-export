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
def c():
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
    def test_get_create_container_kwargs(self, mocker, c, info, ctype, other):
        container = c(ctype, id="test", info=info, **other)
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
        [
            (flywheel.Session(id="test"), MagicMock(spec=dir(flywheel.Subject))),
            (
                flywheel.Session(id="test", tags=["test", "one"]),
                MagicMock(spec=dir(flywheel.Subject)),
            ),
        ],
    )
    def test_create_container_copy(self, origin, parent):
        add_mock = getattr(parent, f"add_{origin.container_type}")
        add_mock.return_value = origin
        out = ContainerExporter.create_container_copy(origin, parent)
        assert out.label == origin.label
        assert out.tags == origin.tags

    @pytest.mark.parametrize("found", [None, flywheel.Subject(label="test")])
    def test_find_or_create_container_copy(self, mocker, found):
        find_mock = mocker.patch(
            "container_export.ContainerExporter.find_container_copy"
        )
        find_mock.return_value = found
        create_mock = mocker.patch(
            "container_export.ContainerExporter.create_container_copy"
        )
        create_mock.return_value = flywheel.Subject(label="test2")
        container, created = ContainerExporter.find_or_create_container_copy(
            "test", "test"
        )

        assert created == (found is None)
        assert (
            container == flywheel.Subject(label="test")
            if not found is None
            else flywheel.Subject(label="test2")
        )

    @pytest.mark.parametrize("base", [flywheel.Subject, flywheel.Session])
    def test_export_container_files(self, sdk_mock, mocker, base):

        exporter_mock = mocker.patch("container_export.FileExporter")
        origin = base(files=[])
        side_effect = []
        for i in range(10):
            origin.files.append(flywheel.FileEntry(name=str(i)))
            side_effect.append(
                (str(i) if i % 2 == 0 else None, True if i % 3 == 0 else False)
            )
        exporter_mock.return_value.find_or_create_file_copy.side_effect = side_effect

        found, created, failed = ContainerExporter.export_container_files(
            sdk_mock, origin, "other", "test"
        )
        assert failed == ["1", "3", "5", "7", "9"]
        assert created == ["0", "6"]
        assert found == ["2", "4", "8"]

    def test_export_container(self, mocker, container_export, caplog):
        export, mocks = container_export(
            "test", "test", flywheel.Session(label="test"), mock=True
        )

        exported = export.export_container(flywheel.Session(label="test"))

    @pytest.mark.parametrize(
        "container",
        [
            flywheel.Subject(label="test"),
            flywheel.Session(label="test", subject=flywheel.Subject(label="test")),
        ],
    )
    def test_get_subject_export_params(self, mocker, container_export, container):
        if container.container_type == "subject":
            cont_mock = mocker.patch.object(container, "reload")
            cont_mock.return_value = "mocked"
        else:
            cont_mock = mocker.patch.object(container.subject, "reload")
            cont_mock.return_value = "mocked"

        export, mocks = container_export("test", "test", container, mock=True)

        orig, proj, att, hier = export.get_subject_export_params()

        assert orig == "mocked"
        assert proj == "test"
        if container.container_type == "subject":
            assert att == None
        else:
            assert att == False

        assert mocks["hierarchy"].call_count == 1


# def test_get_origin_sessions(self)

