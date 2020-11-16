import csv
from dataclasses import dataclass
from pathlib import PurePosixPath

from flywheel.models.mixins import ContainerBase


class ExportLog:
    """Log to record containers exported"""

    def __init__(self, export_project=None, archive_project=None):
        """

        Args:
            export_project (flywheel.Project or None): the project to which a
                copy of origin_container will be exported
            archive_project (flywheel.Project or None): the project to which
                origin_container will be moved upon successful export
        """
        self.archive_project = archive_project
        self.export_project = export_project
        self.created_dict = {"subjects": [], "sessions": [], "acquisitions": []}
        self.records = list()
        self.export_path = PurePosixPath(
            f"{export_project.group}/{export_project.label}"
        )
        self._archive_path = None

    @property
    def archive_path(self):
        """
        resolver path for the archive project (None if no archive project defined)
        """
        if not self._archive_path and self.archive_project:
            self._archive_path = PurePosixPath(
                f"{self.archive_project.group}/{self.archive_project.label}"
            )
        return self._archive_path

    def add_container_record(
        self,
        origin_path,
        container_copy,
        created_copy,
        found_files=[],
        created_files=[],
        failed_files=[],
    ):
        """
        Add a record to the self.records list for an exported container_type
        Args:
            origin_path (str): resolver path of the origin container_type
            container_copy (ContainerBase): copy of the origin container_type
            created_copy (bool): True if container_copy was created, False if
                it was found
            found_files (list): list of files found on container_copy during export
            created_files (list): list of files created during export
            failed_files (list): list of files that failed to export

        """
        if created_copy:
            created_dict_key = container_copy.container_type + "s"
            self.created_dict[created_dict_key].append(container_copy.id)
        record = ExportRecord(
            container_copy.container_type,
            container_copy.label,
            origin_path,
            created_copy,
            tuple(found_files),
            tuple(created_files),
            tuple(failed_files),
        )
        self.records.append(record)

    def write_csv(self, path, archive_project_path=None):
        """
        Write a csv representation of self.records to path
        Args:
            path (str): path to which to write the csv
            archive_project_path (str or None): resolver path of the archive project
        """
        fieldnames = ["Container", "Name", "Status", "Origin Path", "Export Path"]
        if archive_project_path:
            fieldnames.append("Archive Path")
        fieldnames = fieldnames + ["Found Files", "Created Files", "Failed Files"]
        with open(path, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for record in self.records:
                writer.writerow(
                    record.get_csv_dict(
                        export_project_path=self.export_path,
                        archive_project_path=archive_project_path,
                    )
                )


@dataclass
class ExportRecord:
    """
    Class to represent export of a flywheel container_type and its files
    """

    container_type: str
    container_label: str
    origin_path: str
    created: bool
    _found_files: tuple = ()
    _created_files: tuple = ()
    _failed_files: tuple = ()

    @property
    def status(self):
        """str representing status of export of the container_type"""
        if self.created:
            status = "created"
        else:
            status = "used_existing"
        if len(self._failed_files) > 0:
            status = status + "_partial"
        return status

    @property
    def created_files(self):
        """str representing the list of files created during export"""
        return self.get_file_tuple_str(self._created_files)

    @property
    def found_files(self):
        """str representing the list of files found during export"""
        return self.get_file_tuple_str(self._found_files)

    @property
    def failed_files(self):
        """str representing the list of files that failed to export"""
        return self.get_file_tuple_str(self._failed_files)

    @staticmethod
    def get_file_tuple_str(file_tuple):
        """
        convert a tuple into a string if the tuple is not empty, otherwise
            return empty str
        """
        if file_tuple:
            return str(file_tuple)
        else:
            return ""

    @staticmethod
    def replace_origin_path_project(origin_path, project_path):
        """Replace the project path in origin_path with project_path"""

        def ensure_Path(input_path):
            if not isinstance(input_path, PurePosixPath):
                return PurePosixPath(input_path)
            else:
                return input_path

        origin_path = ensure_Path(origin_path)
        project_path = ensure_Path(project_path)

        return str(project_path / "/".join(origin_path.parts[2:]))

    def get_csv_dict(self, export_project_path, archive_project_path=None):
        """
        Get a dictionary to be appended to a csv
        Args:
            export_project_path (str or PurePosixPath): resolver path for the
                export project
            archive_project_path(str or PurePosixPath or None):  resolver path
                for the archive project

        Returns:
            dict: representation of the record to be written to a csv
        """
        csv_dict = {
            "Container": self.container_type,
            "Name": self.container_label,
            "Status": self.status,
            "Origin Path": self.origin_path,
            "Export Path": self.replace_origin_path_project(
                self.origin_path, export_project_path
            ),
        }
        if archive_project_path:
            csv_dict["Archive Path"] = self.replace_origin_path_project(
                self.origin_path, archive_project_path
            )
        for item in ("found_files", "created_files", "failed_files"):
            key = " ".join([x.capitalize() for x in item.split("_")])
            csv_dict[key] = getattr(self, item)
        return csv_dict
