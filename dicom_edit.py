import logging
import os
import tempfile
import zipfile
from collections import namedtuple
from pprint import pformat

import pydicom
from flywheel_metadata.file.dicom.fixer import fw_pydicom_config
from pydicom.datadict import dictionary_VR, keyword_dict

from dicom_metadata import get_compatible_fw_header, get_header_dict_list
from util import get_dict_list_common_dict


log = logging.getLogger(__name__)


def write_dcm_to_tempfile(dicom_ds):
    with tempfile.NamedTemporaryFile(suffix=".dcm") as tempf:
        dicom_ds.save_as(tempf.name)


def write_dcm_at_path_to_temp_with_config(path, **fw_config_kwargs):
    with fw_pydicom_config(**fw_config_kwargs):
        dcm = pydicom.dcmread(path, force=True)
        write_dcm_to_tempfile(dcm)


def character_set_callback(raw_elem, **kwargs):
    """A RawDataElement callback for ensuring Specific Character Set VR is correct"""
    if raw_elem.tag == 0x00080005 and raw_elem.VR == "UN":
        # Handle special case when 0x00080005 (Specific Character Set) is UN which
        # prohibits decoding text VR.
        raw_elem = raw_elem._replace(VR="CS")
    return raw_elem


def get_dicom_save_config_kwargs(dicom_path):
    log.debug("Getting save configuration for %s", dicom_path)
    default_pydicom_config = {"use_fw_callback": False}
    fw_fixer_config = {"callback": character_set_callback, "fix_vm1_strings": False}
    fw_fixer_un_vr_config = {
        "callback": character_set_callback,
        "fix_vm1_strings": False,
        "replace_un_with_known_vr": False,
    }
    kwarg_dict_list = [default_pydicom_config, fw_fixer_config, fw_fixer_un_vr_config]
    for i, conf_kwargs in enumerate(kwarg_dict_list):
        try:
            write_dcm_at_path_to_temp_with_config(dicom_path, **conf_kwargs)
            return conf_kwargs
        except:
            if i == 2:
                log.error("Cannot save %s - traceback:\n", dicom_path, exc_info=True)
            pass


def can_update_dicom_tag(dcm_path, tag_keyword, tag_value, **fw_config_kwargs):
    can_update_tag = False
    if not pydicom.datadict.tag_for_keyword(tag_keyword):
        log.error("Unknown DICOM keyword: %s. Tag will not be added.", tag_keyword)
        return can_update_tag
    with fw_pydicom_config(**fw_config_kwargs):
        dcm = pydicom.dcmread(dcm_path, force=True)
    if tag_keyword in dcm:
        # We could have decode problems with the current tag/value
        try:
            dcm_tag_value = dcm.get(tag_keyword)
            log.debug(
                "%s is present in DICOM header with value %s",
                tag_keyword,
                str(dcm_tag_value),
            )
        except:
            log.warning("Could not read current %s value for %s", tag_keyword, dcm_path)
    else:
        log.debug("%s is not currently present in DICOM header")

    log.debug(
        "Testing whether %s can be set as %s and saved for %s",
        tag_keyword,
        str(tag_value),
        dcm_path,
    )
    try:
        setattr(dcm, tag_keyword, tag_value)
        with fw_pydicom_config(**fw_config_kwargs):
            write_dcm_to_tempfile(dcm)
        can_update_tag = True
    except:
        log.error(
            "Exception raised when attempting to set %s as %s for %s",
            tag_keyword,
            str(tag_value),
            dcm_path,
            exc_info=True,
        )
    return can_update_tag


def can_set_dicom_tags(update_dict, dicom_path, **fw_config_kwargs):
    error_tags = list()
    for tag, value in update_dict.items():
        if can_update_dicom_tag(dicom_path, tag, value, **fw_config_kwargs) is False:
            error_tags.append(tag)

    if error_tags:
        log.error("The following tags cannot be updated: %s", str(error_tags))
        return False
    else:
        return True


def can_update_dicom(dicom_path, update_dict, fw_config_kwargs):
    # Cannot save if a dictionary wasn't returned
    if fw_config_kwargs is None:
        return False
    return can_set_dicom_tags(update_dict, dicom_path, **fw_config_kwargs)


def edit_dicom(dicom_path, update_dict):
    log.debug("Checking that %s is saveable...", dicom_path)
    fw_config_kwargs = get_dicom_save_config_kwargs(dicom_path)
    # Cannot save if a dictionary wasn't returned
    if fw_config_kwargs is None:
        return None
    log.debug("Checking that %s can be updated...", str(update_dict.keys()))
    if not can_update_dicom(dicom_path, update_dict, fw_config_kwargs):
        log.error("%s cannot be updated", dicom_path)
        return None
    with fw_pydicom_config(**fw_config_kwargs):
        try:
            dcm = pydicom.dcmread(dicom_path, force=True)
            for key, value in update_dict.items():
                setattr(dcm, key, value)
            dcm.save_as(dicom_path)
            log.debug("Sucessfully saved edited %s", dicom_path)
            return dicom_path
        except:
            log.error("An exception was raised when attempting to save %s", dicom_path)
            return None


class DicomUpdater:
    """
    Class for comparing and updating DICOM files against Flywheel DICOM metadata
    """

    exclude_vrs = ("OF", "SQ", "UI", None)

    def __init__(self, dicom_path_list, flywheel_header, files_log):
        """

        Args:
            dicom_path_list (list): list of paths to DICOM files to compare against
                flywheel_header
            flywheel_header (dict): flywheel dicom metadata to use for comparison
                and update of DICOM files in dicom_path_list
            files_log (logging.Logger): logger to use
        """
        self.dicom_path_list = dicom_path_list
        self.log = files_log
        # Backwards compatibility for VM strings
        self.fw_header = get_compatible_fw_header(flywheel_header)
        self._dicom_dict_list = None
        self._local_common_dicom_dict = None
        self._local_dicom_tags = None
        self._header_diff_dict = None
        self._update_dict = None
        self._safe_to_update = None

    @property
    def dicom_dict_list(self):
        """
        List of dictionaries representing the local DICOM headers for the files
            in self.dicom_path_list + a `path` key. Files without public DICOM
            tags are excluded from this list.

        """
        if not isinstance(self._dicom_dict_list, list):
            self._dicom_dict_list = get_header_dict_list(self.dicom_path_list)
        return self._dicom_dict_list

    @property
    def non_dicom_paths(self):
        """paths from the list to files that do not contain public DICOM tags"""
        dict_paths = {idict.get("path") for idict in self.dicom_dict_list}
        return [path for path in self.dicom_path_list if path not in dict_paths]

    @property
    def local_common_dicom_dict(self):
        """dict with local DICOM tags that share the same value across all files."""
        if not isinstance(self._local_common_dicom_dict, dict):
            common_dict = get_dict_list_common_dict(self.dicom_dict_list)
            # Remove non-dicom tags such as path for list of 1 file
            common_dict = {k: v for k, v in common_dict.items() if k in keyword_dict}
            self._local_common_dicom_dict = common_dict
        return self._local_common_dicom_dict

    @property
    def local_dicom_tags(self):
        """List of DICOM tags that are defined in the list of local DICOMs"""
        if not isinstance(self._local_dicom_tags, list):
            key_list = list(set().union(*(d.keys() for d in self.dicom_dict_list)))
            self._local_dicom_tags = [key for key in key_list if keyword_dict.get(key)]
        return self._local_dicom_tags

    @property
    def header_diff_dict(self):
        """
        Dict representing the difference between Flywheel info.header.dicom
            and the local DICOM headers. Tags that are in the local files but
            do not share a single value across the list of DICOMs are specifically
            excluded.
        """
        if not isinstance(self._header_diff_dict, dict):
            diff_dict = dict()
            update_tag_entry = namedtuple("Update", "fw_value, local_value")
            add_tag_entry = namedtuple("Add", "fw_value")
            for tag, tag_value in self.fw_header.items():
                local_tag_value = self.local_common_dicom_dict.get(tag)
                add_tag = bool(tag not in self.local_dicom_tags)
                if tag_value != local_tag_value:
                    if tag in self.local_common_dicom_dict:
                        diff_dict[tag] = update_tag_entry(tag_value, local_tag_value)
                    elif add_tag:
                        diff_dict[tag] = add_tag_entry(tag_value)
            self._header_diff_dict = diff_dict

        return self._header_diff_dict

    @property
    def safe_to_update(self):
        """
        boolean indicating whether the local DICOM files can be safely updated
            to match the Flywheel info.header.dicom tag values
        """
        if not isinstance(self._safe_to_update, bool):
            valid = True
            # No local DICOM headers extracted
            if self.dicom_path_list == self.non_dicom_paths and self.fw_header:
                warn_str = (
                    "Despite having info.header.dicom metadata, no dicom header "
                    "information could be parsed from any files. Metadata will not "
                    f"be mapped to the following: {self.dicom_path_list}"
                )
                self.log.warning(warn_str)
                valid = False

            # No common tags (multiple series)
            elif not self.local_common_dicom_dict:
                warn_str = (
                    f"These {len(self.dicom_path_list)} DICOMs do not share common "
                    "public DICOM tag values and are unlikely to belong to the "
                    "same series. info.header.dicom metadata will not be mapped "
                    f"to these files: {self.dicom_path_list}"
                )
                self.log.warning(warn_str)
                valid = False

            # Majority of common tags are to be edited (wrong file)
            elif len(self.header_diff_dict) > (len(self.local_common_dicom_dict) / 3):
                warn_str = (
                    f"{len(self.header_diff_dict)} of the info.header.dicom tags "
                    "are absent or differ from the local dicom file(s) when "
                    f"{len(self.local_common_dicom_dict)} DICOM tags share "
                    "the same value across the DICOM series. This indicates "
                    "that info.header.dicom does not match the current DICOM(s)"
                    "DICOM(s) will not be edited to match info.header.dicom. "
                )
                self.log.warning(warn_str)
                valid = False
            self._safe_to_update = valid
        return self._safe_to_update

    @property
    def update_dict(self):
        """
        The dictionary to be provided as input to edit_dicom to make the file(s)
            consistent with self.fw_header
        """
        if not isinstance(self._update_dict, dict):
            if self.header_diff_dict:
                info_str = f"Differing DICOM tags:\n {pformat(self.header_diff_dict)}"

                self.log.debug(info_str)
                update_dict = {k: v.fw_value for k, v in self.header_diff_dict.items()}
                # Remove OF, SQ, UI VR tags
                exclude_keys = [
                    k
                    for k in update_dict.keys()
                    if dictionary_VR(k) in self.exclude_vrs
                ]
                exclude_vr_tags = {k: update_dict.pop(k) for k in exclude_keys}
                if exclude_vr_tags:
                    warn_str = (
                        f"{len(exclude_vr_tags)} DICOM tags have VRs {self.exclude_vrs} "
                        "for which editing is not supported. The values for the "
                        "following tags will not be edited despite info.header.dicom"
                        f" and local values differing: {pformat(exclude_vr_tags)}"
                    )
                    self.log.warning(warn_str)
                self._update_dict = update_dict

        return self._update_dict

    def update_dicoms(self):
        """
        Update files with public DICOM tags to match self.fw_header

        Returns:
            list of paths
        """
        if self.safe_to_update:
            dicom_paths = [dcm["path"] for dcm in self.dicom_dict_list]
            if self.update_dict:
                updated_paths = [
                    edit_dicom(path, self.update_dict) for path in dicom_paths
                ]
                if all(updated_paths):
                    info_str = f"Successfully updated {len(updated_paths)} DICOMs"
                    self.log.info(info_str)
                    return updated_paths
                else:
                    updated_paths = [path for path in updated_paths if path is not None]
                    failed_list = list(set(dicom_paths) - set(updated_paths))
                    warn_str = (
                        f"Failed to update {len(failed_list)} DICOMs: {failed_list}"
                    )
                    self.log.warning(warn_str)
                return updated_paths
            else:
                self.log.info("No DICOM tags to update!")
                return dicom_paths

    @staticmethod
    def replace_zip_contents(file_directory_path, file_list, zip_path):
        """
        Replace the zip at zip_path with a zip of the files in file_list
        Args:
            file_directory_path (str): path to the directory containing the files
                in file_list
            file_list (list): absolute paths to the files to add to the zip at
                zip_path
            zip_path (str): path to the zip to replace

        Returns:
            path to the replaced zip

        """
        # Remove the original if it exists
        if os.path.exists(zip_path):
            os.remove(zip_path)
        with zipfile.ZipFile(
            zip_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True
        ) as zipf:
            for path in file_list:
                zipf.write(path, os.path.relpath(path, file_directory_path))
        return zip_path

    @classmethod
    def update_dicom_zip(cls, zip_path, fw_header, files_log):
        """
        Update the DICOM files within the zip at zip_path to match fw_header
        Args:
            zip_path (str): path to the DICOM zip to update
            fw_header (dict): flywheel's info.header.dicom metadata for the zip
            files_log (logging.Logger): the log to use for DicomUpdater created
                for updating the zip DICOM files

        Returns:
            None or str: path to the updated zip if update was successful,
                else None
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(zip_path) as zipf:
                extracted_files = [
                    os.path.join(temp_dir, rel_path.filename)
                    for rel_path in zipf.infolist()
                    if not rel_path.is_dir()
                ]
                zipf.extractall(temp_dir)
            updater = cls(extracted_files, fw_header, files_log)
            res = updater.update_dicoms()
            if not res:
                return None
            else:
                return cls.replace_zip_contents(temp_dir, extracted_files, zip_path)

    @classmethod
    def update_fw_dicom(cls, dicom_path, fw_header):
        """
        Update the DICOM file/zip to match fw_header
        Args:
            dicom_path (str): path to the DICOM file/zip to update
            fw_header (dict): flywheel's info.header.dicom metadata for the
                DICOM file/zip

        Returns:
             None or str: path to the updated DICOM file/zip if update was
                successful, else None
        """
        files_log = logging.getLogger(os.path.basename(dicom_path))
        if zipfile.is_zipfile(dicom_path):
            return cls.update_dicom_zip(dicom_path, fw_header, files_log)
        else:
            updater = cls([dicom_path], fw_header, files_log)
            updated_list = updater.update_dicoms()
            if not updated_list:
                return None
            else:
                return updated_list[0]
