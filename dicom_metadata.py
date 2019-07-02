#!/usr/bin/env python

import os
import re
import json
import jsonschema
import pytz
import pydicom
import string
import tzlocal
import logging
import zipfile
import datetime
import argparse
# import nibabel
import pandas as pd
from fnmatch import fnmatch
from pprint import pprint

logging.basicConfig()
log = logging.getLogger('dicom-metadata')

def get_session_label(dcm):
    """
    Switch on manufacturer and either pull out the StudyID or the StudyInstanceUID
    """
    session_label = ''
    if ( dcm.get('Manufacturer') and (dcm.get('Manufacturer').find('GE') != -1 or dcm.get('Manufacturer').find('Philips') != -1 ) and dcm.get('StudyID')):
        session_label = dcm.get('StudyID')
    else:
        session_label = dcm.get('StudyInstanceUID')

    return session_label


def validate_timezone(zone):
    # pylint: disable=missing-docstring
    if zone is None:
        zone = tzlocal.get_localzone()
    else:
        try:
            zone = pytz.timezone(zone.zone)
        except pytz.UnknownTimeZoneError:
            zone = None
    return zone


def parse_patient_age(age):
    """
    Parse patient age from string.
    convert from 70d, 10w, 2m, 1y to datetime.timedelta object.
    Returns age as duration in seconds.
    """
    if age == 'None' or not age:
        return None

    conversion = {  # conversion to days
        'Y': 365.25,
        'M': 30,
        'W': 7,
        'D': 1,
    }
    scale = age[-1:]
    value = age[:-1]
    if scale not in conversion.keys():
        # Assume years
        scale = 'Y'
        value = age

    age_in_seconds = datetime.timedelta(int(value) * conversion.get(scale)).total_seconds()

    # Make sure that the age is reasonable
    if not age_in_seconds or age_in_seconds <= 0:
        age_in_seconds = None

    return age_in_seconds


def timestamp(date, time, timezone):
    """
    Return datetime formatted string
    """
    if date and time and timezone:
        try:
            return timezone.localize(datetime.datetime.strptime(date + time[:6], '%Y%m%d%H%M%S'), timezone)
        except:
            log.warning('Failed to create timestamp!')
            log.info(date)
            log.info(time)
            log.info(timezone)
            return None
    return None


def get_timestamp(dcm, timezone):
    """
    Parse Study Date and Time, return acquisition and session timestamps
    """
    if hasattr(dcm, 'StudyDate') and hasattr(dcm, 'StudyTime'):
        study_date = dcm.StudyDate
        study_time = dcm.StudyTime
    elif hasattr(dcm, 'StudyDateTime'):
        study_date = dcm.StudyDateTime[0:8]
        study_time = dcm.StudyDateTime[8:]
    else:
        study_date = None
        study_time = None

    if hasattr(dcm, 'AcquisitionDate') and hasattr(dcm, 'AcquisitionTime'):
        acquitision_date = dcm.AcquisitionDate
        acquisition_time = dcm.AcquisitionTime
    elif hasattr(dcm, 'AcquisitionDateTime'):
        acquitision_date = dcm.AcquisitionDateTime[0:8]
        acquisition_time = dcm.AcquisitionDateTime[8:]
    # The following allows the timestamps to be set for ScreenSaves
    elif hasattr(dcm, 'ContentDate') and hasattr(dcm, 'ContentTime'):
        acquitision_date = dcm.ContentDate
        acquisition_time = dcm.ContentTime
    else:
        acquitision_date = None
        acquisition_time = None

    session_timestamp = timestamp(study_date, study_time, timezone)
    acquisition_timestamp = timestamp(acquitision_date, acquisition_time, timezone)

    if session_timestamp:
        if session_timestamp.tzinfo is None:
            log.info('no tzinfo found, using UTC...')
            session_timestamp = pytz.timezone('UTC').localize(session_timestamp)
        session_timestamp = session_timestamp.isoformat()
    else:
        session_timestamp = ''
    if acquisition_timestamp:
        if acquisition_timestamp.tzinfo is None:
            log.info('no tzinfo found, using UTC')
            acquisition_timestamp = pytz.timezone('UTC').localize(acquisition_timestamp)
        acquisition_timestamp = acquisition_timestamp.isoformat()
    else:
        acquisition_timestamp = ''
    return session_timestamp, acquisition_timestamp


def get_sex_string(sex_str):
    """
    Return male or female string.
    """
    if sex_str == 'M':
        sex = 'male'
    elif sex_str == 'F':
        sex = 'female'
    else:
        sex = ''
    return sex


def assign_type(s):
    """
    Sets the type of a given input.
    """
    if type(s) == pydicom.valuerep.PersonName or type(s) == pydicom.valuerep.PersonName3 or type(s) == pydicom.valuerep.PersonNameBase:
        return format_string(s)
    if type(s) == list or type(s) == pydicom.multival.MultiValue:
        try:
            return [ float(x) for x in s ]
        except ValueError:
            try:
                return [ int(x) for x in s ]
            except ValueError:
                return [ format_string(x) for x in s if len(x) > 0 ]
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
    formatted = re.sub(r'[^\x00-\x7f]',r'', str(in_string)) # Remove non-ascii characters
    formatted = ''.join(filter(lambda x: x in string.printable, formatted))
    if len(formatted) == 1 and formatted == '?':
        formatted = None
    return formatted#.encode('utf-8').strip()


def get_seq_data(sequence, ignore_keys):
    seq_dict = {}
    for seq in sequence:
        for s_key in seq.dir():
            s_val = getattr(seq, s_key, '')
            if type(s_val) is pydicom.UID.UID or s_key in ignore_keys:
                continue

            if type(s_val) == pydicom.sequence.Sequence:
                _seq = get_seq_data(s_val, ignore_keys)
                seq_dict[s_key] = _seq
                continue

            if type(s_val) == str:
                s_val = format_string(s_val)
            else:
                s_val = assign_type(s_val)

            if s_val:
                seq_dict[s_key] = s_val

    return seq_dict


def get_pydicom_header(dcm):
    # Extract the header values
    header = {}
    exclude_tags = ['[Unknown]', 'PixelData', 'Pixel Data',  '[User defined data]', '[Protocol Data Block (compressed)]', '[Histogram tables]', '[Unique image iden]']
    tags = dcm.dir()
    for tag in tags:
        try:
            if (tag not in exclude_tags) and ( type(dcm.get(tag)) != pydicom.sequence.Sequence ):
                value = dcm.get(tag)
                if value or value == 0: # Some values are zero
                    # Put the value in the header
                    if type(value) == str and len(value) < 10240: # Max pydicom field length
                        header[tag] = format_string(value)
                    else:
                        header[tag] = assign_type(value)
                else:
                    log.debug('No value found for tag: ' + tag)

            if type(dcm.get(tag)) == pydicom.sequence.Sequence:
                seq_data = get_seq_data(dcm.get(tag), exclude_tags)
                # Check that the sequence is not empty
                if seq_data:
                    header[tag] = seq_data
        except:
            log.debug('Failed to get ' + tag)
            pass
    return header


# def get_csa_header(dcm):
#     exclude_tags = ['PhoenixZIP', 'SrMsgBuffer']
#     header = {}
#     try:
#         raw_csa_header = nibabel.nicom.dicomwrappers.SiemensWrapper(dcm).csa_header
#         tags = raw_csa_header['tags']
#     except:
#         log.warning('Failed to parse csa header!')
#         return header
#
#     for tag in tags:
#         if not raw_csa_header['tags'][tag]['items'] or tag in exclude_tags:
#             log.debug('Skipping : %s' % tag)
#             pass
#         else:
#             value = raw_csa_header['tags'][tag]['items']
#             if len(value) == 1:
#                 value = value[0]
#                 if type(value) == str and ( len(value) > 0 and len(value) < 1024 ):
#                     header[format_string(tag)] = format_string(value)
#                 else:
#                     header[format_string(tag)] = assign_type(value)
#             else:
#                 header[format_string(tag)] = assign_type(value)
#
#     return header


def dicom_date_handler(dcm):
    if dcm.get('AcquisitionDate'):
        pass
    elif dcm.get('SeriesDate'):
        dcm.AcquisitionDate = dcm.get('SeriesDate')
    elif dcm.get('StudyDate'):
        dcm.AcquisitionDate = dcm.get('StudyDate')
    else:
        log.warning('No date found for DICOM file')
    return dcm


def dicom_header_extract(zip_file_path, timezone=validate_timezone(tzlocal.get_localzone())):

    # Extract the last file in the zip to /tmp/ and read it
    if zipfile.is_zipfile(zip_file_path):
        dcm_list = []
        zip = zipfile.ZipFile(zip_file_path)
        num_files = len(zip.namelist())
        for n in range((num_files - 1), -1, -1):
            dcm_path = zip.extract(zip.namelist()[n], '/tmp')
            dcm_tmp = None
            if os.path.isfile(dcm_path):
                try:
                    log.info('reading %s' % dcm_path)
                    dcm_tmp = pydicom.read_file(dcm_path)
                    # Here we check for the Raw Data Storage SOP Class, if there
                    # are other pydicom files in the zip then we read the next one,
                    # if this is the only class of pydicom in the file, we accept
                    # our fate and move on.
                    if dcm_tmp.get('SOPClassUID') == 'Raw Data Storage' and n != range((num_files - 1), -1, -1)[-1]:
                        continue
                    else:
                        dcm_list.append(dcm_tmp)
                except:
                    pass
            else:
                log.warning('%s does not exist!' % dcm_path)
        dcm = dcm_list[-1]
    else:
        log.info('Not a zip. Attempting to read %s directly' % os.path.basename(zip_file_path))
        dcm = pydicom.read_file(zip_file_path)
        dcm_list = [dcm]
    if not dcm:
        log.warning('dcm is empty!!!')
        os.sys.exit(1)

    # Handle date on dcm
    dcm = dicom_date_handler(dcm)

    # Create pandas object for comparing headers
    df_list = []
    for header in dcm_list:
        tmp_dict = get_pydicom_header(header)
        for key in tmp_dict:
            if type(tmp_dict[key]) == list:
                tmp_dict[key] = str(tmp_dict[key])
            else:
                tmp_dict[key] = [tmp_dict[key]]
        df_tmp = pd.DataFrame.from_dict(tmp_dict)
        df_list.append(df_tmp)
    df = pd.concat(df_list, ignore_index=True, sort=True)

    # File classification
    pydicom_file = {}
    pydicom_file['name'] = os.path.basename(zip_file_path)
    pydicom_file['modality'] = format_string(dcm.get('Modality'))
    pydicom_file['info'] = {
                                "header": {
                                    "dicom": {}
                                }
                            }


    # File metadata from pydicom header
    pydicom_file['info']['header']['dicom'] = get_pydicom_header(dcm)

    # # Add CSAHeader to DICOM
    # if dcm.get('Manufacturer') == 'SIEMENS':
    #     csa_header = get_csa_header(dcm)
    #     if csa_header:
    #         pydicom_file['info']['header']['dicom']['CSAHeader'] = csa_header

    return pydicom_file['info']['header']['dicom']


if __name__ == '__main__':
    # Set paths
    input_folder = '/flywheel/v0/input/file/'
    output_folder = '/flywheel/v0/output/'
    config_file_path = '/flywheel/v0/config.json'
    output_filepath = os.path.join(output_folder, '.metadata.json')

    # Load config file
    with open(config_file_path) as config_data:
        config = json.load(config_data)

    # Set dicom path and name from config file
    dicom_filepath = config['inputs']['dicom']['location']['path']

    # Get metadata for dicom file
    metadata = dicom_header_extract(dicom_filepath)

    # Write out the metadata to file (.metadata.json)
    metafile_outname = os.path.join(os.path.dirname(output_folder), '.metadata.json')
    with open(metafile_outname, 'w') as metafile:
        json.dump(metadata, metafile, separators=(', ', ': '), sort_keys=True, indent=4)
    if os.path.isfile(metadatafile):
        os.sys.exit(0)
