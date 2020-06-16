#!/usr/bin/env python

import json
import logging
import os
import re
import string
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import pydicom
from pydicom.datadict import DicomDictionary, tag_for_keyword

logging.basicConfig()
log = logging.getLogger('dicom-metadata')


def assign_type(s):
    """
    Sets the type of a given input.
    """
    if type(s) == pydicom.valuerep.PersonName or type(s) == pydicom.valuerep.PersonName3 or type(s) == pydicom.valuerep.PersonNameBase:
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
    formatted = re.sub(r'[^\x00-\x7f]', r'', str(in_string))  # Remove non-ascii characters
    formatted = ''.join(filter(lambda x: x in string.printable, formatted))
    if len(formatted) == 1 and formatted == '?':
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
            if not hasattr(v, 'keyword') or \
                    (hasattr(v, 'keyword') and v.keyword in ignore_keys) or \
                    (hasattr(v, 'keyword') and not v.keyword):  # keyword of type "" for unknown tags
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
            msg = f'With tag {tag} got exception: {str(ex)}'
            errors.append(msg)
    return errors


def compare_dicom_headers(local_dicom_header, flywheel_dicom_header):
    """
    Compares file dicom header and flywheel dicom header for differences

    The local dicom header is extracted to have no Sequences

    Args:
        local_dicom_header (dict): Dictionary representation of the dicom file header
        flywheel_dicom_header (dict): Dictionary representation of the curated Flywheel header

    Returns:
        list: a list of which keys to update (update_keys)
    """
    headers_match = True
    update_keys = list()
    # Add backwards compatibility for VM arrays
    fix_type_based_on_dicom_vm(flywheel_dicom_header)
    if local_dicom_header != flywheel_dicom_header:
        # Generate a list of keys that need to be updated within the local dicom file
        # Compare the headers, and track which keys are different
        # Exclude Sequence Tags and list values which match non-list values
        for key in sorted(flywheel_dicom_header.keys()):
            try:
                vr, vm, _, _, _ = DicomDictionary.get(tag_for_keyword(key))
            except (ValueError, TypeError):
                log.warning(
                    'The proposed key, "{}", is not a valid DICOM tag. '.format(key) +
                    'It will not be considered to update the DICOM file.'
                )
                continue

            if key not in local_dicom_header and vr != 'SQ':
                log.warning(
                    'MISSING key: {} not found in local_header. \n'.format(key) +
                    'INSERTING valid tag: {} into local dicom file. '.format(key)
                )
                if headers_match:
                    log.warning('Local DICOM header and Flywheel header do NOT match...')
                headers_match = False
                update_keys.append(key)
            elif key in local_dicom_header and vr == 'SQ':
                log_message = 'Sequence (SQ) Tags are not compared for update. \n'
                log_message += 'Any difference in {} is not accounted for.'.format(key)
                log.warning(log_message)
            else:
                # Check if the tags are equal
                if local_dicom_header[key] != flywheel_dicom_header[key]:
                    # Check to see if flywheel_dicom_header[key] is a list
                    # and matches a non-list local_dicom_header[key]
                    if (
                        not isinstance(local_dicom_header[key], list) and
                        isinstance(flywheel_dicom_header[key], list) and
                        local_dicom_header[key] == flywheel_dicom_header[key][0]
                    ):
                        log.info(
                            'The values in both headers, %s and %s, are considered functionally equivalent.',
                            local_dicom_header[key],
                            flywheel_dicom_header[key])
                    else:
                        if headers_match:
                            log.warning('Local DICOM header and Flywheel header do NOT match...')
                        # Make sure we're comparing the header from the same file...
                        if key == 'SOPInstanceUID':
                            log.warning(
                                'WARNING: SOPInstanceUID does not match across ' +
                                'the headers of individual dicom files!!!'
                            )
                        log.warning('MISMATCH in key: {}'.format(key))
                        log.warning('DICOM    = {}'.format(local_dicom_header[key]))
                        log.warning('Flywheel = {}'.format(flywheel_dicom_header[key]))
                        update_keys.append(key)
                        headers_match = False
    if headers_match:
        log.info('Local DICOM header and Flywheel headers match!')

    return update_keys


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
    vr, vm, _, _, _ = DicomDictionary.get(data_element.tag)
    # Check if it is a VR string
    if vr not in ['UT', 'ST', 'LT', 'FL', 'FD', 'AT', 'OB', 'OW', 'OF', 'SL', 'SQ',
                  'SS', 'UL', 'OB/OW', 'OW/OB', 'OB or OW', 'OW or OB', 'UN'] \
            and 'US' not in vr:
        if vm == '1' and hasattr(data_element, 'VM') and data_element.VM > 1:
            data_element._value = '\\'.join(data_element.value)


def fix_type_based_on_dicom_vm(header):
    exc_keys = []
    for key, val in header.items():
        try:
            vr, vm, _, _, _ = DicomDictionary.get(tag_for_keyword(key))
        except (ValueError, TypeError):
            exc_keys.append(key)
            continue

        if vr != 'SQ':
            if vm != '1' and not isinstance(val, list):  # anything else is a list
                header[key] = [val]
        else:
            for dataset in val:
                fix_type_based_on_dicom_vm(dataset)
    if len(exc_keys) > 0:
        log.warning('%s Dicom data elements were not type fixed based on VM', len(exc_keys))


def get_pydicom_header(dcm):
    # Extract the header values
    errors = walk_dicom(dcm, callbacks=[fix_VM1_callback], recursive=True)
    if errors:
        result = ''
        for error in errors:
            result += '\n  {}'.format(error)
        log.warning(f'Errors found in walking dicom: {result}')
    header = {}
    exclude_tags = ['[Unknown]',
                    'PixelData',
                    'Pixel Data',
                    '[User defined data]',
                    '[Protocol Data Block (compressed)]',
                    '[Histogram tables]',
                    '[Unique image iden]',
                    'ContourData',
                    'EncryptedAttributesSequence'
                    ]
    tags = dcm.dir()
    for tag in tags:
        try:
            if (tag not in exclude_tags) and ( type(dcm.get(tag)) != pydicom.sequence.Sequence ):
                value = dcm.get(tag)
                if value or value == 0:  # Some values are zero
                    # Put the value in the header
                    if type(value) == str and len(value) < 10240:  # Max pydicom field length
                        header[tag] = format_string(value)
                    else:
                        header[tag] = assign_type(value)

                else:
                    log.debug('No value found for tag: ' + tag)

            if (tag not in exclude_tags) and type(dcm.get(tag)) == pydicom.sequence.Sequence:
                seq_data = get_seq_data(dcm.get(tag), exclude_tags)
                # Check that the sequence is not empty
                if seq_data:
                    header[tag] = seq_data
        except:
            log.debug('Failed to get ' + tag)
            pass

    fix_type_based_on_dicom_vm(header)

    return header


def dcm_dict_is_representative(dcm_data_dict, use_rawdatastorage=False):
    """
    This function is intended to mimic the logic used by GRP-3 to select a representative dicom header
    Args:
        dcm_data_dict (dict):
        use_rawdatastorage (bool): whether SOPClassUID Raw Data Storage is representative

    Returns:
        bool: whether the dcm_data_dict is representative
    """
    representative = False
    if dcm_data_dict['size'] > 0 and dcm_data_dict['header'] and not dcm_data_dict['pydicom_exception']:
        # Here we check for the Raw Data Storage SOP Class, if there
        # are other pydicom files in the zip then we read the next one,
        # if this is the only class of pydicom in the file, we accept
        # our fate and move on.
        if dcm_data_dict['header'].get('SOPClassUID') == 'Raw Data Storage' and not use_rawdatastorage:
            log.warning('SOPClassUID=Raw Data Storage for %s. Skipping', dcm_data_dict['path'])

        else:
            # Note: no need to try/except, all files have already been open when calling get_dcm_data_dict
            representative = True
    elif dcm_data_dict['size'] < 1:
        log.warning('%s is empty. Skipping.', os.path.basename(dcm_data_dict['path']))
    elif dcm_data_dict['pydicom_exception']:
        log.warning('Pydicom raised on reading %s. Skipping.', os.path.basename(dcm_data_dict['path']))
    return representative


def dicom_header_extract(file_path, flywheel_header_dict):
    """
    Get a dictionary representing the dicom header at the file_path (or within the archive at file_path)
    Args:
        flywheel_header_dict (dict): a dictionary representing the current
            flywheel header metadata
        file_path (str): path to dicom file/archive

    Returns:
        dict: dictionary representing the dicom header (empty dict if None are representative)

    """
    # Build list of dcm files
    if zipfile.is_zipfile(file_path):
        try:
            log.info('Extracting %s ' % os.path.basename(file_path))
            zip = zipfile.ZipFile(file_path)
            tmp_dir = tempfile.TemporaryDirectory().name
            zip.extractall(path=tmp_dir)
            dcm_path_list = Path(tmp_dir).rglob('*')
            # keep only files
            dcm_path_list = [str(path) for path in dcm_path_list if path.is_file()]
        except:
            dcm_path_list = list()
    else:
        log.info('Not a zip. Attempting to read %s directly' % os.path.basename(file_path))
        dcm_path_list = [file_path]
    dcm_path = select_matching_file(dcm_path_list, flywheel_header_dict)
    if dcm_path is None:
        dcm_header_dict = dict()
        for idx, dcm_path in enumerate(dcm_path_list):
            last = bool(idx == (len(dcm_path_list) - 1))
            tmp_dcm_data_dict = get_dcm_data_dict(dcm_path, force=True)
            if dcm_dict_is_representative(tmp_dcm_data_dict, use_rawdatastorage=last):
                dcm_header_dict = tmp_dcm_data_dict.get('header')
            break
    else:
        dcm_header_dict = get_dcm_data_dict(dcm_path).get('header', dict())

    return dcm_header_dict


def select_matching_file(file_list, flywheel_header_dict):
    """
    Selects the file that matches flywheel_header_dict on instance tags from
        file_list. Returns None if none of the files match on these tags.
    Args:
        file_list (list): a list of paths to dicom_files
        flywheel_header_dict (dict): dictionary representation of dicom header
            from a flywheel file

    Returns:
        str or None
    """
    instance_tag_list = [
        'SOPInstanceUID',
        'SliceLocation',
        'ContentTime',
        'InstanceCreationTime',
        'InstanceNumber'
    ]
    flywheel_inst_dict = dict()
    for tag in instance_tag_list:
        flywheel_inst_dict[tag] = flywheel_header_dict.get(tag)

    if not flywheel_inst_dict:
        log.warning(
            'Could not match file to Flywheel header - missing match tags.'
        )
        return None
    for path in file_list:
        try:
            dcm = pydicom.dcmread(path, specific_tags=instance_tag_list)
            header_inst_dict = get_pydicom_header(dcm)
            if all([header_inst_dict.get(tag) == flywheel_inst_dict.get(tag) for tag in instance_tag_list]):
                return path
            else:
                continue
        except Exception as e:
            print(e)
            continue


def get_dicom_df(dicom_path_list, specific_tag_list=None, force=False):
    """
    Assembles a DataFrame of DICOM tag values for dicom_path_list
    Args:
        dicom_path_list (list): list of paths to dicom files
        specific_tag_list (list): list of tags to be provided as
            pydicom.dcmread's specific_tags parameter
        force (bool): whether to set force to True for pydicom.dcmread

    Returns:
        pandas.DataFrame: a dataframe representing the DICOM tag values for
            dicom_path_list
    """
    dict_list = list()
    for dcm_path in dicom_path_list:
        dcm = pydicom.dcmread(dcm_path, specific_tags=specific_tag_list, force=force)
        data_dict_tmp = get_pydicom_header(dcm)
        if data_dict_tmp:
            dict_list.append(data_dict_tmp)
    if dict_list:
        df = pd.DataFrame(dict_list)
        return df
    else:
        return None


def filter_update_keys(update_keys, dicom_path_list, force=False):
    """
    Removes tags that are not in the DicomDictionary, tags that have a SQ VR,
        tags that vary across a dicom archive
    Args:
        update_keys (list): list of DICOM tags to filter
        dicom_path_list (list): list of paths to dicom files
        force (bool): whether to set force to True for pydicom.dcmread

    Returns:
        list: a list of the DICOM tags that can be safely set from update_keys

    """
    # Remove keys not in the DICOM dictionary
    update_keys = [key for key in update_keys if DicomDictionary.get(tag_for_keyword(key))]
    # Remove SQ VR
    update_keys = [key for key in update_keys if DicomDictionary.get(tag_for_keyword(key))[0] != 'SQ']
    df = get_dicom_df(dicom_path_list, specific_tag_list=update_keys, force=force)
    df = df.applymap(make_list_hashable)
    exc_keys = list()
    for key, value in df.nunique().items():
        if value > 1:
            log.error(
                '%s has more than one unique value and will not be edited.',
                key
            )
            exc_keys.append(key)
    update_keys = [key for key in update_keys if key not in exc_keys]
    return update_keys


def make_list_hashable(value):
    """
    Transforms lists/lists of lists to tuples/tuples of tuples for hashability.
    If value is not a list, it is returned unchanged.
    Args:
        value: an item from an iterable that may or may not be a list

    Returns:
        a tuple if value was a list, otherwise the input value
    """
    if isinstance(value, list):
        value = tuple(make_list_hashable(x) for x in value)
    return value


def get_dcm_data_dict(dcm_path, force=False, specific_tags=None):
    file_size = os.path.getsize(dcm_path)
    res = {
        'path': dcm_path,
        'size': file_size,
        'force': force,
        'pydicom_exception': False,
        'header': None
    }
    if file_size > 0:
        try:
            dcm = pydicom.dcmread(dcm_path, force=force, specific_tags=specific_tags)
            res['header'] = get_pydicom_header(dcm)
        except Exception:
            log.exception('Pydicom raised exception reading dicom file %s', os.path.basename(dcm_path), exc_info=True)
            res['pydicom_exception'] = True
    return res
