#!/usr/bin/env python

import json
import logging
import os
from pathlib import Path
import re
import string
import tempfile
import zipfile


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



def dicom_header_extract(file_path):
    """
    Get a dictionary representing the dicom header at the file_path (or within the archive at file_path)
    Args:
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
    dcm_header_dict = dict()
    for idx, dcm_path in enumerate(dcm_path_list):
        last = bool(idx == (len(dcm_path_list) - 1))
        tmp_dcm_data_dict = get_dcm_data_dict(dcm_path, force=True)
        if dcm_dict_is_representative(tmp_dcm_data_dict, use_rawdatastorage=last):
            dcm_header_dict = tmp_dcm_data_dict.get('header')
            break

    return dcm_header_dict


def get_dcm_data_dict(dcm_path, force=False):
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
            dcm = pydicom.dcmread(dcm_path, force=force)
            res['header'] = get_pydicom_header(dcm)
        except Exception:
            log.exception('Pydicom raised exception reading dicom file %s', os.path.basename(dcm_path), exc_info=True)
            res['pydicom_exception'] = True
    return res

