import logging
import tempfile
import pydicom

from flywheel_metadata.file.dicom.fixer import fw_pydicom_config

log = logging.getLogger(__name__)


def write_dcm_to_tempfile(dicom_ds):
    with tempfile.NamedTemporaryFile(suffix='.dcm') as tempf:
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
    fw_fixer_un_vr_config = {"callback": character_set_callback, "fix_vm1_strings": False, "replace_un_with_known_vr": False}
    for conf_kwargs in [default_pydicom_config, fw_fixer_config, fw_fixer_un_vr_config]:
        try:
            write_dcm_at_path_to_temp_with_config(dicom_path, **conf_kwargs)
            return conf_kwargs
        except:
            pass
    log.error("Cannot save %s - traceback:\n", dicom_path, exc_info=True)


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
            log.debug("%s is present in DICOM header with value %s", tag_keyword, str(dcm_tag_value))
        except:
            log.warning("Could not read current %s value for %s", tag_keyword, dcm_path)
    else:
        log.debug("%s is not currently present in DICOM header")

    log.debug("Testing whether %s can be set as %s and saved for %s", tag_keyword, str(tag_value), dcm_path)
    try:
        setattr(dcm, tag_keyword, tag_value)
        write_dcm_to_tempfile(dcm)
        can_update_tag = True
    except:
        log.error("Exception raised when attempting to set %s as %s for %s", tag_keyword, str(tag_value), dcm_path, exc_info=True)
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

