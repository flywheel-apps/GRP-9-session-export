import json
import logging
import os
import tempfile
import zipfile

from copy import deepcopy
from pprint import pformat


import backoff
import flywheel
from flywheel.models.mixins import ContainerBase

from util import (
    hash_value,
    quote_numeric_string,
    get_sanitized_filename,
    false_if_exc_is_timeout,
    false_if_exc_timeout_or_sub_exists,
    get_dict_list_common_dict,
)

from dicom_metadata import get_compatible_fw_header, get_header_dict_list
from dicom_edit import DicomUpdater
from validate import validate_context
from export_log import ExportLog

CONTAINER_KWARGS_KEYS = {
    "acquisition": ("label", "timestamp", "timezone", "uid"),
    "file": ("classification", "info", "modality", "type"),
    "session": ("age", "label", "operator", "timestamp", "timezone", "uid", "weight"),
    "subject": (
        "code",
        "cohort",
        "ethnicity",
        "firstname",
        "label",
        "lastname",
        "race",
        "sex",
        "species",
        "strain",
    ),
    "project": ("description",),
}

EXCLUDE_TAGS = ["EXPORTED"]


class ContainerExporter:
    """
    Attributes:
        origin_container (ContainerBase): the container to export
        gear_context (flywheel.GearContext)
        export_project (flywheel.Project): the project to which to export
        archive_project (flywheel.Project): the project to which to move the origin_container
            upon successful export


    """

    def __init__(self, export_project, archive_project, origin_container, gear_context):
        self.gear_context = gear_context
        self.fw_client = gear_context.client
        self.config = gear_context.config
        self.origin_container = origin_container
        self.origin_hierarchy = self.get_hierarchy(origin_container)
        self.export_project = export_project
        self.archive_project = archive_project
        # for abbreviated notation
        self.container_type = origin_container.container_type
        self.export_log = ExportLog(export_project, archive_project)
        self.status = None
        self._create_container_kwargs = None
        self._container_copy_find_queries = None
        self._origin_parent_id = None
        self._log = None

    @classmethod
    def from_gear_context(cls, gear_context):
        """
        Instantiate an Exporter class instance from gear_context
        Args:
            gear_context (flywheel.GearContext):

        Returns:
            GRP9Exporter
        """
        return cls(*validate_context(gear_context), gear_context)

    @property
    def origin_parent_id(self):
        """The id of self.origin_container's parent"""
        for parent_type, parent_id in self.origin_container.parents.items():
            if self.origin_container.get(parent_type) == parent_id:
                self._origin_parent_id = parent_id
                break

        return self._origin_parent_id

    @property
    @property
    def log(self):
        if not self._log:
            log_msg = f"GRP-9 {self.container_type.capitalize()} {self.origin_container.label} Export"
            self._log = logging.getLogger(log_msg)
        return self._log

    @property
    def csv_path(self):
        subject_label = (
            self.origin_hierarchy.subject.label or self.origin_hierarchy.subject.code
        )
        if self.container_type == "session":
            csv_name = "{}-{}_export_log.csv".format(
                subject_label, self.origin_container.label
            )
        else:
            csv_name = f"{subject_label}_export_log.csv"
        csv_name = get_sanitized_filename(csv_name)
        directory = self.gear_context.output_dir
        csv_path = os.path.join(directory, csv_name)
        return csv_path

    def get_hierarchy(self, container):
        """
        Get a ContainerHierarchy instance for container
        Args:
            container (ContainerBase): the container for which to retrieve hierarchy

        Returns:
            ExportHierarchy: an object with attributes from container.parents
                but with the container objects instead of the id string
        """
        return ExportHierarchy.from_container(self.fw_client, container)

    @staticmethod
    def get_create_container_kwargs(origin_container):
        """
        dictionary to be provided as kwargs to a destination_parent's
            add_<self.origin_container.container_type> method
        """
        container_kwargs_keys = CONTAINER_KWARGS_KEYS.get(
            origin_container.container_type
        )
        create_container_kwargs = dict()
        for key in container_kwargs_keys:
            origin_container_key_value = origin_container.get(key)
            if origin_container_key_value not in [None, {}, []]:
                create_container_kwargs[key] = origin_container_key_value
        create_container_kwargs["info"] = deepcopy(origin_container.info)
        create_container_kwargs["info"]["export"] = {
            "origin_id": hash_value(origin_container.id)
        }

        return create_container_kwargs

    @staticmethod
    def get_container_find_queries(origin_container):
        """tuple of query strings used by the find_container_copy method"""
        # Subject label/code has enforced uniqueness
        if origin_container.container_type == "subject":
            container_copy_find_queries = (
                # Search by label first
                f"label={quote_numeric_string(origin_container.label)}",
                # label is unreliable so also search code
                f"code={quote_numeric_string(origin_container.code)}",
            )
        else:
            origin_id = hash_value(origin_container.id)
            container_copy_find_queries = (f"info.export.origin_id={origin_id}",)

        return container_copy_find_queries

    @staticmethod
    def find_container_copy(origin_container, export_parent):
        """
        Returns an existing copy of origin_container if it exists on export_parent,
            otherwise returns None

        Args:
            origin_container (ContainerBase): the container for which to find a copy
            export_parent (ContainerBase): the parent container of the
                container copy

        Returns:
            ContainerBase or None: the found destination container or None
        """
        container_copy = None
        if (
            origin_container.parents.get(export_parent.container_type)
            == export_parent.id
        ):
            container_copy = origin_container
        else:
            find_first_func = getattr(
                getattr(export_parent, f"{origin_container.container_type}s"),
                "find_first",
            )
            for query in ContainerExporter.get_container_find_queries(origin_container):
                result = find_first_func(query)
                # Fully populate container metadata if a container is returned
                if isinstance(result, flywheel.models.mixins.ContainerBase):
                    container_copy = result.reload()
                    break

        return container_copy

    @staticmethod
    def create_container_copy(origin_container, export_parent):
        """
        Creates and returns a copy of self.origin_container on self.export_parent

        Args:
            origin_container (ContainerBase): the container for which to create a copy
            export_parent (ContainerBase): the parent container of the
                container copy

        Returns:
            ContainerBase: the created container
        """

        # For example, project.add_subject, subject.add_session, session.add_acquisition
        create_container_func = getattr(
            export_parent, f"add_{origin_container.container_type}"
        )
        create_kwargs = ContainerExporter.get_create_container_kwargs(origin_container)
        created_container = create_container_func(**create_kwargs)
        # tags must be added using the add_tag method
        for tag in origin_container.tags:
            if tag not in EXCLUDE_TAGS:
                created_container.add_tag(tag)

        return created_container

    @staticmethod
    @backoff.on_exception(
        backoff.expo,
        flywheel.rest.ApiException,
        max_time=300,
        giveup=false_if_exc_timeout_or_sub_exists,
        jitter=backoff.full_jitter,
    )
    def find_or_create_container_copy(origin_container, export_parent):
        """
        First tries to find a copy of or the original self.origin_id on destination_parent and
            creates a copy if one is not found

        Args:
            origin_container (ContainerBase): the container for which to find or create a copy
            export_parent (ContainerBase): the parent container of the
                container copy

        Returns:
            tuple(ContainerBase, bool): the found/created container and whether
                it was created

        """
        created = False
        found_container = ContainerExporter.find_container_copy(
            origin_container, export_parent
        )
        if found_container is None:
            created = True
            return_container = ContainerExporter.create_container_copy(
                origin_container, export_parent
            )
        else:
            return_container = found_container
        return return_container, created

    @staticmethod
    def export_container_files(
        fw_client, origin_container, export_container, dicom_map
    ):
        found = list()
        created = list()
        failed = list()
        for ifile in origin_container.files:
            file_exporter = FileExportRecord(fw_client, ifile, dicom_map)
            exported_name, file_created = file_exporter.find_or_create_file_copy(
                export_container
            )
            if exported_name:
                if file_created:
                    created.append(exported_name)
                else:
                    found.append(exported_name)
            else:
                failed.append(ifile.name)

        return found, created, failed

    def export_container(
        self,
        origin_container,
        export_parent,
        export_attachments=False,
        export_hierarchy=None,
    ):
        if not export_hierarchy:
            export_hierarchy = ExportHierarchy.from_container(
                self.fw_client, origin_container
            )
        container_label = origin_container.get("label", origin_container.get("code"))
        log_msg_str = (
            f"{export_hierarchy.container_type} {container_label} "
            f"({origin_container.id}):"
        )
        c_log = logging.getLogger(log_msg_str)
        debug_str = f"Attempting to find or create copy of {export_hierarchy.path}"
        c_log.debug(debug_str)
        c_copy, created = self.find_or_create_container_copy(
            origin_container, export_parent
        )
        prefix_str = "Created" if created else "Found"
        log_str = f"{prefix_str} copy of {export_hierarchy.path}"
        c_log.info(log_str)
        if origin_container.container_type == "acquisition" or export_attachments:
            c_log.info("Exporting files...")
            if self.config.get("map_flywheel_to_dicom"):
                dicom_map = export_hierarchy.dicom_map
            else:
                dicom_map = None
            found, created, failed = self.export_container_files(
                self.fw_client, origin_container, c_copy, dicom_map
            )
            if found:
                c_log.info("Found files: %s", str(found))
            if created:
                c_log.info("Created files: %s", str(created))
            if failed:
                c_log.info("Failed to export files: %s", str(failed))
            self.export_log.add_container_record(
                export_hierarchy.path, c_copy, created, found, created, failed
            )
        else:
            self.export_log.add_container_record(export_hierarchy.path, c_copy, created)
        return c_copy, created

    def export(self):
        export_attachments = self.config.get("export_attachments")
        (
            origin_subject,
            subject_export_hierarchy,
            sessions,
        ) = self.get_subject_export_params()
        subject_copy, created = self.export_container(
            origin_subject,
            self.export_project,
            export_attachments=export_attachments,
            export_hierarchy=subject_export_hierarchy,
        )
        export_success = True
        for session in sessions:
            session = session.reload()
            session_hierarchy = subject_export_hierarchy.get_child_hierarchy(session)
            try:
                session_copy, session_created = self.export_container(
                    session,
                    subject_copy,
                    export_attachments=export_attachments,
                    export_hierarchy=session_hierarchy,
                )
                for acquisition in session.acquisitions():
                    acquisition = acquisition.reload()
                    acq_hierarchy = session_hierarchy.get_child_hierarchy(acquisition)
                    try:
                        acq_copy, acq_created = self.export_container(
                            acquisition.reload(),
                            session_copy,
                            export_attachments=export_attachments,
                            export_hierarchy=acq_hierarchy,
                        )
                    except:
                        self.log.error(
                            "Failed to export acquisition %s",
                            acquisition.label,
                            exc_info=True,
                        )
                        export_success = False

            except:
                self.log.error(
                    "Failed to export session %s", session.label, exc_info=True
                )
                export_success = False
                continue
        if any(x.failed_files for x in self.export_log.records):
            export_success = False

        if export_success and self.archive_project:
            self.archive()
            self.export_log.write_csv(self.csv_path, self.export_log.archive_path)
        else:
            self.export_log.write_csv(self.csv_path)
        if export_success:
            return 0
        else:
            return 1

    def get_subject_export_params(self):
        if self.container_type == "subject":
            origin_subject = self.origin_container.reload()
            subject_export_hierarchy = self.origin_hierarchy
            sessions = self.origin_container.sessions.iter()
        else:
            origin_subject = self.origin_container.subject.reload()
            subject_export_hierarchy = self.origin_hierarchy.get_parent_hierarchy()
            sessions = [self.origin_container.reload()]
        return origin_subject, subject_export_hierarchy, sessions

    def archive(self):
        def move_sessions(archive_subject, sessions):
            for session in sessions:
                session.update({"subject": {"_id": archive_subject.id}})

        if self.archive_project:

            origin_subject, _, sessions = self.get_subject_export_params()

            found_subject = self.find_container_copy(
                origin_subject, self.archive_project
            )
            if self.container_type == "subject":
                if not found_subject:
                    origin_subject.update(project=self.archive_project.id)
                else:
                    move_sessions(found_subject, sessions)
            else:
                archive_subject, created = self.find_or_create_container_copy(
                    origin_subject, sessions
                )
                move_sessions(archive_subject, sessions)


class FileExportRecord:
    def __init__(self, fw_client, file_entry, dicom_map=None):
        """

        Args:
            fw_client (flywheel.Client): the flywheel client
            file_entry (flywheel.FileEntry):
        """
        self.sanitized_name = get_sanitized_filename(file_entry.name)
        self.origin_file = file_entry
        self.origin_id = hash_value(self.origin_file.id)
        self.type = file_entry.type
        self._modality = file_entry.modality
        self._classification = file_entry.classification
        self._info = file_entry.info
        self._upload_function = fw_client.upload_file_to_container
        self._log = None
        self.classification_schema = self.get_classification_schema(
            fw_client, self.modality
        )
        self.dicom_map = dicom_map

    @property
    def classification(self):
        """Classification that can be set on destination FileEntry"""
        if not isinstance(self._classification, dict):
            self._classification = dict()
        valid_classification = self.get_valid_classification(
            self._classification, self.classification_schema
        )
        if valid_classification != self._classification:
            warn_str = (
                f"classification {pformat(self._classification)} is not valid for "
                f"modality {self.modality}. Using classification {pformat(valid_classification)}"
            )
            self.log.warning(warn_str)
            self._classification = valid_classification
        else:
            self.log.debug("file classification is %s", pformat(self._classification))
        return self._classification

    @property
    def info(self):
        """
        The info dictionary from the original FileEntry with export.origin_id
            defined
        """
        self._info["export"] = {"origin_id": self.origin_id}
        self.log.debug("file info is %s", pformat(self._info))
        return self._info

    @property
    def log(self):
        if self._log is None:
            self._log = logging.getLogger(f"{self.sanitized_name}")
        return self._log

    @property
    def modality(self):
        """The file modality"""
        # Special case - mriqc output files do not have modality set, so
        # we must set the modality prior to the classification to avoid errors.
        if not self._modality and self.sanitized_name.endswith("mriqc.qa.html"):
            self._modality = "MR"
        self.log.debug("file modality is %s", self._modality)
        return self._modality

    @property
    def fw_dicom_header(self):
        """DICOM header information from file.info.header.dicom"""
        dicom_header_dict = self._info.get("header", {}).get("dicom")

        if dicom_header_dict:
            # Backwards compatibility
            dicom_header_dict = get_compatible_fw_header(dicom_header_dict)
            if self.dicom_map:
                info_str = f"Flywheel DICOM map: {pformat(self.dicom_map)}"
                self.log.info(info_str)
                dicom_header_dict.update(self.dicom_map)

        return dicom_header_dict

    @fw_dicom_header.setter
    def fw_dicom_header(self, value):
        """setter for the fw_dicom_header property"""
        if not isinstance(value, dict):
            raise ValueError(
                f"info.header.dicom must be a dict, {type(value)} provided"
            )
        else:
            self._info["header"] = {"dicom": value}

    @staticmethod
    @backoff.on_exception(
        backoff.expo,
        flywheel.rest.ApiException,
        max_time=300,
        giveup=false_if_exc_timeout_or_sub_exists,
        jitter=backoff.full_jitter,
        on_giveup=lambda x: dict(),
    )
    def get_classification_schema(fw_client, modality):
        """
        Retrieves the classification schema for modality, returns
            empty dictionary if it cannot be retrieved
        Args:
            fw_client (flywheel.Client): the flywheel client
            modality (str): the modality for which to get the schema
        Returns:
            dict: the classification schema dictionary
        """
        schema = dict()
        if modality:
            try:
                schema = fw_client.get_modality(modality).get("classification")
                return schema
            except flywheel.rest.ApiException as exc:
                # Modality does not have a schema
                if not exc.status == 404:
                    raise exc
        return schema

    def find_file_copy(self, export_parent):
        for file_entry in export_parent.files:
            file_origin_id = file_entry.info.get("export", {}).get("origin_id")
            if self.origin_id == file_origin_id and file_entry.name in [
                self.sanitized_name,
                self.origin_file.name,
            ]:
                return file_entry

    @backoff.on_exception(
        backoff.expo,
        flywheel.rest.ApiException,
        max_time=300,
        giveup=false_if_exc_is_timeout,
        jitter=backoff.full_jitter,
    )
    def create_file_copy(self, export_parent):
        with tempfile.TemporaryDirectory() as tempdir:
            local_filepath = self.download(tempdir)
            if self.type == "dicom":
                result = self.update_dicom(local_filepath)
                if not result:
                    return None

            self.upload(export_parent, local_filepath)
            return self.sanitized_name

    def find_or_create_file_copy(self, export_parent):
        file_copy = self.find_file_copy(export_parent)
        file_name = None
        created = False
        if file_copy is None:
            try:
                file_name = self.create_file_copy(export_parent)
                if file_name:
                    created = True
            except Exception:
                self.log.error("Failed to create file copy!", exc_info=True)
        else:
            file_name = file_copy.name

        return file_name, created

    def update_dicom(self, local_filepath):
        if not self.fw_dicom_header:
            warn_str = (
                "Flywheel DICOM does not have a header at info.header.dicom to "
                " map to DICOM!"
            )
            self.log.warning(warn_str)
            if self.dicom_map:
                warn_str = (
                    "map_flywheel_to_dicom is True, but mapping will not be "
                    "performed since info.header.dicom is not defined. "
                    "Please run GRP-3 (medatadata extraction) on DICOMs to which "
                    "you wish to map flywheel metadata"
                )

                self.log.warning(warn_str)
            return local_filepath
        return DicomUpdater.update_fw_dicom(
            local_filepath, self.fw_dicom_header
        )

    def download(self, download_dirpath):
        """
        Download the file to download_dirpath as self.sanitized_name
        Args:
            download_dirpath (str): the path of the directory to which to
                download the file
        """
        if self.origin_file.name != self.sanitized_name:
            warn_str = (
                f"{self.origin_file.name} is not a valid file name. Using "
                f"{self.sanitized_name}"
            )
            self.log.warning(warn_str)
        download_path = os.path.join(download_dirpath, self.sanitized_name)
        self.origin_file.download(download_path)
        return download_path

    def upload(self, destination_container, local_filepath):
        """
        Upload the file at local_filepath to destination_container with the
            pertinent metadata

        Args:
            destination_container (ContainerBase): the container to which to upload
                the file
            local_filepath (str): path to a local copy of the file

        """
        return self._upload_function(
            container_id=destination_container.id,
            file=local_filepath,
            metadata=self.get_file_upload_metadata_str(),
        )

    def get_file_upload_metadata_str(self):
        """
        Get a JSON string representation of the file metadata to be provided
            as the metadata parameter for upload_file_to_container

        Returns:
            str: file metadata json string
        """
        metadata_dict = dict()
        for attr in ("type", "info", "modality", "classification"):
            attr_value = getattr(self, attr)
            if attr_value:
                metadata_dict[attr] = attr_value
        self.log.debug("upload metadata string is %s", pformat(metadata_dict))
        return json.dumps(metadata_dict)

    @staticmethod
    def get_valid_classification(classification, classification_schema):
        """
        Modify classification to be consistent with classification schema

        Args:
            classification (dict): classification dictionary from FileEntry.classification
            classification_schema (dict): classification schema for the modality
                containing valid keys with list values that contain items that
                are valid

        Returns:
            dict: a valid classification dictionary
        """
        # copy so we can pop keys while iterating
        classification_copy = classification.copy()
        for key, value in classification.items():
            # values should all be lists
            if not isinstance(value, list) and value is not None:
                value = [value]
                classification_copy[key] = value
            # Remove keys that are not in the schema (except for Custom)
            if key not in classification_schema.keys() and key != "Custom":
                classification_copy.pop(key)
            else:
                if key != "Custom":
                    # Remove values that are not in the schema list
                    value = [
                        item
                        for item in value
                        if item in classification_schema.get(key, [])
                    ]
                if value:
                    classification_copy[key] = value
                # Remove key if value is empty list or None
                else:
                    classification_copy.pop(key)

        return classification_copy


class GRP9Exporter:
    def __init__(self, export_project, archive_project, origin_container, gear_context):
        self.export_project = export_project
        self.archive_project = archive_project
        self.origin_container = origin_container
        self.context = gear_context

    @classmethod
    def from_gear_context(cls, gear_context):
        """
        Instantiate an Exporter class instance from gear_context
        Args:
            gear_context (flywheel.GearContext):

        Returns:
            GRP9Exporter
        """
        return cls(*validate_context(gear_context), gear_context)


class ExportHierarchy:
    order_tuple = ("acquisition", "session", "subject", "project", "group")

    def __init__(self, **kwargs):
        self.group = kwargs.get("group")
        self.project = kwargs.get("project")
        self.subject = kwargs.get("subject")
        self.session = kwargs.get("session")
        self.acquisition = kwargs.get("acquisition")
        self._container_type = None
        self._dicom_map = dict()
        self._path = None

    def __deepcopy__(self, memodict={}):
        return ExportHierarchy(**self.to_dict())

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __delitem__(self, key):
        if hasattr(self, key):
            delattr(self, key)

    @classmethod
    def from_container(cls, fw_client, container):
        init_kwargs = dict()
        for parent_type, parent_id in container.parents.items():
            if parent_id is not None:
                init_kwargs[parent_type] = cls._get_container(
                    fw_client, parent_type, parent_id
                )
        init_kwargs[container.container_type] = container
        return cls(**init_kwargs)

    @staticmethod
    @backoff.on_exception(
        backoff.expo,
        flywheel.rest.ApiException,
        max_time=300,
        giveup=false_if_exc_is_timeout,
        jitter=backoff.full_jitter,
    )
    def _get_container(fw_client, container_type, container_id):
        if container_id is None:
            return None
        # get_session, for example
        get_container_func = getattr(fw_client, f"get_{container_type}", None)
        if get_container_func is None:
            raise ValueError(f"Cannot get a container of type {container_type}")
        else:
            return get_container_func(container_id)

    @property
    def container_type(self):
        """The type of the lowest container in the hierarchy"""
        for container_type in self.order_tuple:
            container = self.get(container_type)
            if container:
                self._container_type = container_type
                break

        return self._container_type

    @property
    def parent(self):
        if self.container_type not in ["group", None]:
            parent_container_type_idx = self.order_tuple.index(self.container_type) + 1
            parent_container_type = self.order_tuple[parent_container_type_idx]
            return getattr(self, parent_container_type)

    @property
    def dicom_map(self):
        """
        The dictionary to use for map_flywheel_to_dicom
        """
        self._dicom_map = self.get_dicom_map_dict(
            self.acquisition, self.session, self.subject
        )
        return self._dicom_map

    @property
    def path(self):
        """The flywheel resolver path"""
        if self._path is None:
            containers = (
                self.group,
                self.project,
                self.subject,
                self.session,
                self.acquisition,
            )
            labels = [(c.label or c.get("code")) for c in containers if c is not None]
            if labels:
                self._path = "/".join(labels)
        return self._path

    @staticmethod
    def get_dicom_map_dict(acquisition, session, subject):
        """
        Get a dictionary to use for map_flywheel_to_dicom
        Args:
            acquisition (flywheel.Acquisition or None): acquisition parent (if any) of the files
            session (flywheel.Session or None): session parent (if any) of the files
            subject (flywheel.Subject or None): subject parent (if any) of the files

        Returns:
            dict: dictionary of DICOM tags and values with which to update
                DICOM files
        """
        dicom_map_dict = dict()
        if acquisition:
            dicom_map_dict["SeriesDescription"] = acquisition.label
        if session:
            dicom_map_dict["PatientWeight"] = session.get("weight", "")
            dicom_map_dict["PatientAge"] = ExportHierarchy.get_patientage_from_session(
                session
            )
            dicom_map_dict["StudyID"] = session.label
        if subject:
            dicom_map_dict["PatientSex"] = ExportHierarchy.get_patientsex_from_subject(
                subject
            )
            dicom_map_dict["PatientID"] = subject.label or subject.code
        return dicom_map_dict

    @staticmethod
    def get_patientsex_from_subject(subject):
        """
        transforms flywheel subject.sex into a string valid for the PatientSex
            DICOM header tag

        Args:
            subject (flywheel.Subject): a flywheel subject container object

        Returns:
            str: empty string (''), 'M', 'F', or 'O'

        """
        if subject.sex in ["male", "female", "other"]:
            patientsex = subject.sex[0].upper()
            return patientsex
        else:
            return ""

    @staticmethod
    def get_patientage_from_session(session):
        """
        Retrieves a DICOM Age String appropriate for the age
            (i.e. 001Y, 011M, 003W, 006D). Returns None if age is not defined

        Args:
            session (flywheel.Session): the session from which to retrieve the age

        Returns:
            None or str
        """
        if session.age is None:
            return None

        for age_field in ("age_years", "age_months", "age_weeks", "age_days"):
            age_field_value = getattr(session, age_field, 0)
            if age_field_value >= 1:
                break
        age_letter = age_field.lstrip("age_").upper()[0]
        age_str = str(int(age_field_value)).zfill(3) + age_letter
        return age_str

    def get(self, key):
        """Exposes a dictionary-like get method"""
        return getattr(self, key, None)

    def get_child_hierarchy(self, child_container):
        hierarchy_dict = self.to_dict()
        hierarchy_dict[child_container.container_type] = child_container
        return ExportHierarchy.from_dict(hierarchy_dict)

    def get_parent_hierarchy(self):
        hierarchy_dict = self.to_dict()
        hierarchy_dict.pop(self.container_type)
        return ExportHierarchy.from_dict(hierarchy_dict)

    def to_dict(self):
        """
        Returns:
            dict: a dictionary representation of the instance
        """
        return vars(self)

    @classmethod
    def from_dict(cls, hierarchy_dict):
        kwargs = {k: v for k, v in hierarchy_dict.items() if k in cls.order_tuple}
        return cls(**kwargs)
