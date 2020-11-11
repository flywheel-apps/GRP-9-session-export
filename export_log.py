import csv
from dataclasses import dataclass
from pathlib import PurePosixPath

from container_export import ContainerBase


class ExportLog:
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
    container: str
    name: str
    origin_path: str
    created: bool
    _found_files: tuple = ()
    _created_files: tuple = ()
    _failed_files: tuple = ()

    @property
    def status(self):
        if self.created:
            status = "created"
        else:
            status = "used_existing"
        if len(self._failed_files) > 0:
            status = status + "_partial"
        return status

    @property
    def created_files(self):
        return self.get_file_tuple_str(self._created_files)

    @property
    def found_files(self):
        return self.get_file_tuple_str(self._found_files)

    @property
    def failed_files(self):
        return self.get_file_tuple_str(self._failed_files)

    @staticmethod
    def get_file_tuple_str(file_tuple):
        if file_tuple:
            return str(file_tuple)
        else:
            return ""

    @staticmethod
    def replace_origin_path_project(origin_path, project_path):
        def ensure_Path(input_path):
            if not isinstance(input_path, PurePosixPath):
                return PurePosixPath(input_path)
            else:
                return input_path

        origin_path = ensure_Path(origin_path)
        project_path = ensure_Path(project_path)

        return str(project_path / "/".join(origin_path.parts[2:]))

    def get_csv_dict(self, export_project_path, archive_project_path=None):
        csv_dict = {
            "Container": self.container,
            "Name": self.name,
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
