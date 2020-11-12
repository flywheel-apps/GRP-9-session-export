#!/usr/bin/env python
import logging
import re
import string

import pydicom
from pydicom.datadict import (
    DicomDictionary,
    get_entry,
    tag_for_keyword,
)


log = logging.getLogger("dicom-metadata")


def assign_type(s):
    """
    Sets the type of a given input.
    """
    if type(s) == pydicom.valuerep.PersonName:
        return format_string(s)
    if type(s) == list or type(s) == pydicom.multival.MultiValue:
        try:
            return [float(x) for x in s]
        except ValueError:
            try:
                return [int(x) for x in s]
            except ValueError:
                return [format_string(x) for x in s if len(x) > 0]
    elif type(s) == float or type(s) == int:
        return s
    elif type(s) == pydicom.uid.UID:
        s = str(s)
        return format_string(s)
    else:
        s = str(s)
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                return format_string(s)


def format_string(in_string):
    formatted = re.sub(
        r"[^\x00-\x7f]", r"", str(in_string)
    )  # Remove non-ascii characters
    formatted = "".join(filter(lambda x: x in string.printable, formatted))
    if len(formatted) == 1 and formatted == "?":
        formatted = None
    return formatted


def get_seq_data(sequence, ignore_keys):
    """Return list of nested dictionaries matching sequence

    Args:
        sequence (pydicom.Sequence): A pydicom sequence
        ignore_keys (list): List of keys to ignore

    Returns:
        (list): list of nested dictionary matching sequence
    """
    res = []
    for seq in sequence:
        seq_dict = {}
        for k, v in seq.items():
            if (
                not hasattr(v, "keyword")
                or (hasattr(v, "keyword") and v.keyword in ignore_keys)
                or (hasattr(v, "keyword") and not v.keyword)
            ):  # keyword of type "" for unknown tags
                continue
            kw = v.keyword
            if isinstance(v.value, pydicom.sequence.Sequence):
                seq_dict[kw] = get_seq_data(v, ignore_keys)
            elif isinstance(v.value, str):
                seq_dict[kw] = format_string(v.value)
            else:
                seq_dict[kw] = assign_type(v.value)
        res.append(seq_dict)
    return res


def walk_dicom(dcm, callbacks=None, recursive=True):
    """Same as pydicom.DataSet.walk but with logging the exception instead of raising.

    Args:
        dcm (pydicom.DataSet): A pydicom.DataSet.
        callbacks (list): A list of function to apply on each DataElement of the
            DataSet (default = None).
        recursive (bool): It True, walk the dicom recursively when encountering a SQ.

    Returns:
        list: List of errors
    """
    taglist = sorted(dcm._dict.keys())
    errors = []
    for tag in taglist:
        try:
            data_element = dcm[tag]
            if callbacks:
                for cb in callbacks:
                    cb(dcm, data_element)
            if recursive and tag in dcm and data_element.VR == "SQ":
                sequence = data_element.value
                for dataset in sequence:
                    walk_dicom(dataset, callbacks, recursive=recursive)
        except Exception as ex:
            msg = f"With tag {tag} got exception: {str(ex)}"
            errors.append(msg)
    return errors


def fix_VM1_callback(dataset, data_element):
    r"""Update the data element fixing VM based on public tag definition

    This addresses the following none conformance for element with string VR having
    a `\` in the their value which gets interpret as array by pydicom.
    This function re-join string and is aimed to be used as callback.

    From the DICOM Standard, Part 5, Section 6.2, for elements with a VR of LO, such as
    Series Description: A character string that may be padded with leading and/or
    spaces. The character code 5CH (the BACKSLASH "\" in ISO-IR 6) shall not be
    present, as it is used as the delimiter between values in multi-valued data
    elements. The string shall not have Control Characters except for ESC.

    Args:
        dataset (pydicom.DataSet): A pydicom DataSet
        data_element (pydicom.DataElement): A pydicom DataElement from the DataSet

    Returns:
        pydicom.DataElement: An updated pydicom DataElement
    """
    try:
        vr, vm, _, _, _ = get_entry(data_element.tag)
        # Check if it is a VR string
        if (
            vr
            not in [
                "UT",
                "ST",
                "LT",
                "FL",
                "FD",
                "AT",
                "OB",
                "OW",
                "OF",
                "SL",
                "SQ",
                "SS",
                "UL",
                "OB/OW",
                "OW/OB",
                "OB or OW",
                "OW or OB",
                "UN",
            ]
            and "US" not in vr
        ):
            if vm == "1" and hasattr(data_element, "VM") and data_element.VM > 1:
                data_element._value = "\\".join(data_element.value)
    except KeyError:
        # we are only fixing VM for tag supported by get_entry (i.e. DicomDictionary or
        # RepeatersDictionary)
        pass


def fix_type_based_on_dicom_vm(header):

    exc_keys = []
    for key, val in header.items():
        try:
            vr, vm, _, _, _ = DicomDictionary.get(tag_for_keyword(key))
        except (ValueError, TypeError):
            exc_keys.append(key)
            continue

        if vr != "SQ":
            if vm != "1" and not isinstance(val, list):  # anything else is a list
                header[key] = [val]
            elif vm == "1" and isinstance(val, list):
                if len(val) == 1:
                    header[key] = val[0]
                else:
                    if (
                        vr
                        not in [
                            "UT",
                            "ST",
                            "LT",
                            "FL",
                            "FD",
                            "AT",
                            "OB",
                            "OW",
                            "OF",
                            "SL",
                            "SQ",
                            "SS",
                            "UL",
                            "OB/OW",
                            "OW/OB",
                            "OB or OW",
                            "OW or OB",
                            "UN",
                        ]
                        and "US" not in vr
                    ):

                        header[key] = "\\".join([str(item) for item in val])
        else:
            for dataset in val:
                if isinstance(dataset, dict):
                    fix_type_based_on_dicom_vm(dataset)
                else:
                    log.warning(
                        "%s SQ list item is not a dictionary - value = %s", key, dataset
                    )
    if len(exc_keys) > 0:
        log.warning(
            "%s Dicom data elements were not type fixed based on VM", len(exc_keys)
        )


def get_pydicom_header(dcm):
    # Extract the header values
    errors = walk_dicom(dcm, callbacks=[fix_VM1_callback], recursive=True)
    if errors:
        result = ""
        for error in errors:
            result += "\n  {}".format(error)
        log.warning(f"Errors found in walking dicom: {result}")
    header = {}
    exclude_tags = [
        "[Unknown]",
        "PixelData",
        "Pixel Data",
        "[User defined data]",
        "[Protocol Data Block (compressed)]",
        "[Histogram tables]",
        "[Unique image iden]",
        "ContourData",
        "EncryptedAttributesSequence",
    ]
    tags = dcm.dir()
    for tag in tags:
        try:
            if (tag not in exclude_tags) and (
                type(dcm.get(tag)) != pydicom.sequence.Sequence
            ):
                value = dcm.get(tag)
                if value or value == 0:  # Some values are zero
                    # Put the value in the header
                    if (
                        type(value) == str and len(value) < 10240
                    ):  # Max pydicom field length
                        header[tag] = format_string(value)
                    else:
                        header[tag] = assign_type(value)

                else:
                    log.debug("No value found for tag: " + tag)

            if (tag not in exclude_tags) and type(
                dcm.get(tag)
            ) == pydicom.sequence.Sequence:
                seq_data = get_seq_data(dcm.get(tag), exclude_tags)
                # Check that the sequence is not empty
                if seq_data:
                    header[tag] = seq_data
        except:
            log.debug("Failed to get " + tag)
            pass

    fix_type_based_on_dicom_vm(header)

    return header


def get_compatible_fw_header(fw_header):
    """
    Ensure backwards compatibility with older versions of GRP-3, namely
        ensuring that VM-1 strings are not arrays.
    Args:
        fw_header (dict): dictionary representation of DICOM header retrieved
            from or to be set as info.header.dicom in Flywheel

    Returns:
        dict: backwards compatible dictionary representation of DICOM header
            retrieved from or to be set as info.header.dicom in Flywheel

    """
    new_header = fw_header.copy()
    # Fix VM issues
    fix_type_based_on_dicom_vm(new_header)
    return new_header


def get_header_dict_list(dcm_path_list):
    """
    Get a list of dictionaries representing the headers for the DICOMs at the
        paths in dcm_path_list, excluding any paths to files without public
        DICOM tags (unlikely to be DICOM)

    Args:
        dcm_path_list (list): list of paths to DICOM files

    Returns:
        list of dicts representing DICOM headers
    """
    dict_list = list()
    for dcm_path in dcm_path_list:
        dcm = pydicom.dcmread(dcm_path, force=True)
        data_dict_tmp = get_pydicom_header(dcm)
        # Exclude files with no public keys (unlikely to be dicoms)
        if data_dict_tmp:
            data_dict_tmp["path"] = dcm_path
            dict_list.append(data_dict_tmp)
    return dict_list
