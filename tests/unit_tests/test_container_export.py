import os
import tempfile
from contextlib import nullcontext as does_not_raise
from copy import deepcopy
from unittest.mock import MagicMock

import flywheel
import flywheel_gear_toolkit
import pytest

from container_export import (
    CONTAINER_KWARGS_KEYS,
    EXCLUDE_TAGS,
    ContainerExporter,
    ContainerHierarchy,
    FileExporter,
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
            flywheel.Subject(label="test"), "test"
        )

        assert created == (found is None)
        assert (
            container == flywheel.Subject(label="test")
            if not found is None
            else flywheel.Subject(label="test2")
        )

    @pytest.mark.parametrize("base", [flywheel.Subject, flywheel.Session])
    def test_export_container_files(self, sdk_mock, mocker, base):

        exporter_mock = mocker.patch("container_export.FileExporter.from_client")
        origin = base(files=[])
        side_effect = []
        for i in range(10):
            origin.files.append(flywheel.FileEntry(name=str(i)))
            side_effect.append(
                (str(i) if i % 2 == 0 else None, True if i % 3 == 0 else False)
            )
        exporter_mock.return_value.find_or_create_file_copy.side_effect = side_effect

        found, created, failed = ContainerExporter.export_container_files(
            sdk_mock, origin, "other", None
        )
        assert failed == ["1", "3", "5", "7", "9"]
        assert created == ["0", "6"]
        assert found == ["2", "4", "8"]

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

    @pytest.mark.parametrize(
        "origin,ctype",
        [
            (MagicMock(spec=dir(flywheel.Session)), "session"),
            (MagicMock(spec=(dir(flywheel.Subject) + ["sessions"])), "subject"),
        ],
    )
    def test_get_origin_sessions(self, container_export, origin, ctype):
        origin.container_type = ctype
        if ctype == "subject":
            origin.sessions.iter.return_value = ["1", "2", "3"]
        else:
            origin.reload.return_value = origin
        container_ex, mocks = container_export("test", "test", origin, mock=True)

        sess = container_ex.get_origin_sessions()

        if ctype == "subject":
            assert sess == ["1", "2", "3"]
        else:
            assert sess == [origin]


def test_container_hierarchy():
    hierarchy_dict = {
        "group": flywheel.Group(id="test_group", label="Test Group"),
        "project": flywheel.Project(label="test_project"),
        "subject": flywheel.Subject(label="test_subject", sex="other"),
        "session": flywheel.Session(age=31000000, label="test_session", weight=50,),
    }
    # test from_dict
    test_hierarchy = ContainerHierarchy.from_dict(hierarchy_dict)
    # test deepcopy
    assert deepcopy(test_hierarchy) != test_hierarchy
    # test path
    assert test_hierarchy.path == "test_group/test_project/test_subject/test_session"
    # test parent
    assert test_hierarchy.parent.label == "test_subject"
    # test from_container
    mock_client = MagicMock(spec=dir(flywheel.Client))
    parent_dict = dict()
    for item in ("group", "project", "subject"):
        value = hierarchy_dict.copy().get(item)
        parent_dict[item] = item
        setattr(mock_client, f"get_{item}", lambda x: value)
    session = flywheel.Session(age=31000000, label="test_session", weight=50)
    session.parents = parent_dict
    assert (
        ContainerHierarchy.from_container(mock_client, session).container_type
        == "session"
    )
    # test _get_container
    assert test_hierarchy._get_container(None, None, None) is None
    with pytest.raises(ValueError) as exc:
        test_hierarchy._get_container(None, "garbage", "garbage_id")
        assert str(exc) == "Cannot get a container of type garbage"
    mock_client = MagicMock(spec=dir(flywheel.Client))
    mock_client.get_session = lambda x: x
    assert (
        test_hierarchy._get_container(mock_client, "session", "session_id")
        == "session_id"
    )
    # test container_type
    assert test_hierarchy.container_type == "session"
    # test dicom_map
    exp_map = {
        "PatientWeight": 50,
        "PatientAge": "011M",
        "StudyID": "test_session",
        "PatientSex": "O",
        "PatientID": "test_subject",
    }
    assert exp_map == test_hierarchy.dicom_map
    # test get
    assert test_hierarchy.get("container_type") == "session"
    # test get_patient_sex_from_subject
    assert test_hierarchy.get_patientsex_from_subject(flywheel.Subject()) == ""
    # test get_patientage_from_session
    assert test_hierarchy.get_patientage_from_session(flywheel.Session()) is None

    # test get_child_hierarchy
    test_acquisition = flywheel.Acquisition(label="test_acquisition")
    acq_hierarchy = test_hierarchy.get_child_hierarchy(test_acquisition)
    assert acq_hierarchy.dicom_map["SeriesDescription"] == test_acquisition.label
    # test get_parent_hierarchy
    parent_hierarchy = test_hierarchy.get_parent_hierarchy()
    assert parent_hierarchy.container_type == "subject"


MR_CLASSIFICATION_SCHEMA = {
    "Features": [
        "Quantitative",
        "Multi-Shell",
        "Multi-Echo",
        "Multi-Flip",
        "Multi-Band",
        "Steady-State",
        "3D",
        "Compressed-Sensing",
        "Eddy-Current-Corrected",
        "Fieldmap-Corrected",
        "Gradient-Unwarped",
        "Motion-Corrected",
        "Physio-Corrected",
        "Derived",
        "In-Plane",
        "Phase",
        "Magnitude",
        "2D",
        "AAscout",
        "Spin-Echo",
        "Gradient-Echo",
        "EPI",
        "WASSR",
        "FAIR",
        "FAIREST",
        "PASL",
        "EPISTAR",
        "PICORE",
        "pCASL",
        "MPRAGE",
        "MP2RAGE",
        "FLAIR",
        "SWI",
        "QSM",
        "RMS",
        "DTI",
        "DSI",
        "DKI",
        "HARDI",
        "NODDI",
        "Water-Reference",
        "Transmit-Reference",
        "SBRef",
        "Uniform",
        "Singlerep",
        "QC",
        "TRACE",
        "FA",
        "MIP",
        "Navigator",
        "Contrast-Agent",
        "Phase-Contrast",
        "TOF",
        "VASO",
        "iVASO",
        "DSC",
        "DCE",
        "Task",
        "Resting-State",
        "PRESS",
        "STEAM",
        "M0",
        "Phase-Reversed",
        "Spiral",
        "SPGR",
        "Control",
        "Label",
    ],
    "Intent": [
        "Localizer",
        "Shim",
        "Calibration",
        "Fieldmap",
        "Structural",
        "Functional",
        "Screenshot",
        "Non-Image",
        "Spectroscopy",
    ],
    "Measurement": [
        "B0",
        "B1",
        "T1",
        "T2",
        "T2*",
        "PD",
        "MT",
        "Perfusion",
        "Diffusion",
        "Susceptibility",
        "Fingerprinting",
        "MRA",
        "CEST",
        "T1rho",
        "SVS",
        "CSI",
        "EPSI",
        "BOLD",
        "Phoenix",
    ],
}


def test_file_exporter():
    classification_dict = {
        "Custom": ["Spam", "Eggs"],
        "Measurement": "T1",
        "Intent": ["Invalid", "Structural"],
        "InvalidKey": "InvalidValue",
        "Features": [],
    }
    info_dict = {
        "header": {
            "dicom": {
                "InstanceNumber": 1,
                "PatientPosition": "HFS",
                "PatientSex": "O",
                "PatientID": "SPAM_Subject",
                "SeriesDescription": "SPAM_study",
            }
        }
    }
    dicom_map_dict = {
        "PatientID": "Flywheel",
    }
    file_entry = flywheel.FileEntry(
        modality="MR",
        classification=classification_dict,
        name='fi:l*e/p"a?t>h|.t<xt',
        id="test_id",
        info=info_dict,
    )

    def upload_func(container_id, file, metadata):
        return container_id, file, metadata

    file_exporter = FileExporter(
        file_entry, MR_CLASSIFICATION_SCHEMA, upload_func, dicom_map_dict
    )
    # test get_modaility
    assert FileExporter.get_modality(file_entry) == "MR"
    assert (
        FileExporter.get_modality(flywheel.FileEntry(name="test.mriqc.qa.html")) == "MR"
    )
    # test classification
    assert file_exporter.classification == {
        "Custom": ["Spam", "Eggs"],
        "Measurement": ["T1"],
        "Intent": ["Structural"],
    }
    exp_info = {
        "header": {
            "dicom": {
                "InstanceNumber": 1,
                "PatientPosition": "HFS",
                "PatientSex": "O",
                "PatientID": "Flywheel",
                "SeriesDescription": "SPAM_study",
            }
        },
        "export": {
            "origin_id": "0fb27832c685c35889ba3653994bae061237518c40ed57d3b41eae17bf923137"
        },
    }
    # test info
    assert file_exporter.info == exp_info
    # test fw_dicom_header
    assert file_exporter.fw_dicom_header == exp_info["header"]["dicom"]
    # test setter
    exp_info["header"]["dicom"].pop("SeriesDescription")
    file_exporter.fw_dicom_header = exp_info["header"]["dicom"]
    assert file_exporter.info == exp_info
    with pytest.raises(ValueError):
        file_exporter.fw_dicom_header = "not a dict"
    # test from_client
    mock_client = MagicMock(spec=dir(flywheel.Client))
    mock_client.get_modality = lambda x: {"classification": MR_CLASSIFICATION_SCHEMA}
    mock_client.upload_file_to_container = upload_func
    file_exporter = FileExporter.from_client(mock_client, file_entry, dicom_map_dict)
    # test get_classification_schema
    assert (
        file_exporter.get_classification_schema(mock_client, "MR")
        == MR_CLASSIFICATION_SCHEMA
    )

    # Raise with a status that is not 404 and will not get caught with backoff
    def raise_func(*args):
        raise flywheel.ApiException(status=400)

    mock_client.get_modality = raise_func
    with pytest.raises(flywheel.ApiException):
        file_exporter.get_classification_schema(mock_client, "MR")
    # test find_file_copy
    file_entry.info = file_exporter.info.copy()
    export_parent = flywheel.Acquisition(
        label="SPAM_study", files=[file_entry], id="test_id"
    )
    assert file_exporter.find_file_copy(export_parent)
    # test create_file_copy
    setattr(file_entry, "download", lambda x: None)
    assert file_exporter.create_file_copy(export_parent)
    # test find_or_create_file_copy
    fn, created = file_exporter.find_or_create_file_copy(export_parent)
    assert not created
    export_parent.files = list()
    fn, created = file_exporter.find_or_create_file_copy(export_parent)
    assert created
    # test update_dicom
    _, temp_path = tempfile.mkstemp()

    assert not file_exporter.update_dicom(temp_path)
    file_exporter.fw_dicom_header = dict()
    # returns local path if no header is defined
    assert file_exporter.update_dicom(temp_path)
    os.remove(temp_path)
