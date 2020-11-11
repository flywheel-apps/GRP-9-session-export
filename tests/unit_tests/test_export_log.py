import csv
from pathlib import PurePosixPath
import os
import tempfile
import flywheel
from export_log import ExportLog, ExportRecord


def test_export_record():
    container_type = "session"
    container_label = "test_session"
    origin_path = "test_group/test_project/test_subject/test_session"
    created = False
    found_files = ("found.dicom.zip", "found.nii.gz")
    created_files = ("created.dicom.zip",)
    failed_files = ("failed.dicom.zip",)
    export_record = ExportRecord(
        container_type,
        container_label,
        origin_path,
        created,
        found_files,
        created_files,
        failed_files,
    )
    assert export_record.get_file_tuple_str(tuple()) == ""

    exp_dict = {
        "Container": "session",
        "Name": "test_session",
        "Status": "used_existing_partial",
        "Origin Path": "test_group/test_project/test_subject/test_session",
        "Export Path": "export_group/export_project/test_subject/test_session",
        "Archive Path": "archive_group/archive_project/test_subject/test_session",
        "Found Files": "('found.dicom.zip', 'found.nii.gz')",
        "Created Files": "('created.dicom.zip',)",
        "Failed Files": "('failed.dicom.zip',)",
    }
    assert (
        export_record.get_csv_dict(
            "export_group/export_project",
            PurePosixPath("archive_group/archive_project"),
        )
        == exp_dict
    )


def test_export_log():
    export_project = flywheel.Project(group="export_group", label="export_project")
    archive_project = flywheel.Project(group="archive_group", label="archive_project")
    export_log = ExportLog(export_project, archive_project)
    assert export_log.archive_path == PurePosixPath("archive_group/archive_project")

    export_acq = flywheel.Acquisition(label="test_acquisition")
    origin_path = "test_group/test_project/test_subject/test_session/test_acquisition"
    created = True
    found_files = list()
    created_files = [
        "created.dicom.zip",
    ]
    failed_files = [
        "failed.dicom.zip",
    ]
    export_log.add_container_record(
        origin_path, export_acq, created, found_files, created_files, failed_files
    )
    record_dict = export_log.records[0].get_csv_dict(
        export_log.export_path, export_log.archive_path
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_csv_path = os.path.join(temp_dir, "test_acquisition_export_log.csv")
        export_log.write_csv(temp_csv_path, export_log.archive_path)
        with open(temp_csv_path, "r") as csvfile:
            csv_reader = csv.reader(csvfile)
            csv_rows = [row for row in csv_reader]

        exp_row_0 = list(record_dict.keys())
        exp_row_1 = list(record_dict.values())
        assert csv_rows[0] == exp_row_0
        assert csv_rows[1] == exp_row_1
