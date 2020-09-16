#!/usr/bin/env python

import csv
import json
import logging
import os
import pprint
import re
import sys
import tempfile
import time
import zipfile

import backoff
from pathlib import Path
from pprint import pprint as pp

import flywheel
import pydicom

from dicom_metadata import compare_dicom_headers, dicom_header_extract,\
    fix_type_based_on_dicom_vm, filter_update_keys
from util import get_sanitized_filename, quote_numeric_string

logging.basicConfig()
log = logging.getLogger('[GRP 9]:')
log.setLevel(logging.INFO)


def false_if_exc_is_timeout(exception):
    if hasattr(exception, "status"):
        if exception.status in [504, 502, 500]:
            return False
    return True


def false_if_exc_timeout_or_sub_exists(exception):
    is_timeout = not false_if_exc_is_timeout(exception)
    subject_exists = bool(
        exception.status in [409, 422] and 'already exists' in exception.detail
    )
    if is_timeout or subject_exists:
        return False
    else:
        return True





###############################################################################
# LOCAL FUNCTION DEFINITIONS


# Backoff giveup only checks for timeout, not "exists" because file uploads do not
# Give "already exists" errors
@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=false_if_exc_is_timeout,
                      jitter=backoff.full_jitter)
def _copy_files_from_session(fw, from_session, to_session):
    """Exports file attachments from one session to another. DICOM files will not be exported.

    1. Download each file from "from_session"
    2. Upload each file to "to_session"
    
    NOTE:  This will overwrite any existing files on the target session that already
    exist with the same name.

    Args:
        fw (flywheel.Client): A flywheel client
        from_session (flywheel.Session): An existing flywheel session that has attached files to be
        copied over to "to_session"
        to_session (flywheel.Session): An existing flywheel session that will have the files from
        "from_session" copied over to.

    """
    log.debug(f"Copying session {from_session.label} file attachments to session {to_session.label}")

    session_files = [f for f in from_session.files]
    if len(session_files) == 0:
        log.debug('No files to copy over')
        return

    for session_file in session_files:
        
        if session_file.type == 'dicom':
            log.warning(f"File {session_file.name} is type DICOM.\n"
                        f"DICOMS Uploaded as attachments to a session will NOT be exported,\n"
                        f"As we do not support DICOM mapping at this level.\n"
                        f"{session_file.name} Will be Skipped")
            continue

        
        try:
            
            download_file = os.path.join('/tmp', get_sanitized_filename(session_file.name))
            log.debug(f"\tdownloading file {session_file.name} to {download_file}")
            from_session.download_file(session_file.name, download_file)
            log.debug("\tcomplete")
    
            log.debug("\tuploading file to new session")
            exported_file = upload_file_with_metadata(fw, session_file, to_session, download_file)
            if exported_file is None:
                raise RuntimeError(f"Failed to export file {os.path.basename(download_file)} to {to_session.label}")
            
            log.debug("\tcomplete")
    
            log.debug('\tCleaning up file')
            os.remove(download_file)
    
            log.debug(f"\tFinished file {session_file.name}")
        
        except flywheel.ApiException as e:
            err_str = (
                f'Could not export file {session_file.name} from session '
                f'{from_session.label} to session {to_session.label} {e.reason}'
            )
            log.warning(err_str)
            raise
                
    log.info("Finished Transferring Session Files")


def _update_file_metadata(fw, orig_f, new_f):
    
    # Update file metadata
    if orig_f.modality:
        log.debug('Updating modality to %s for %s' % (orig_f.modality, new_f.name))
        new_f.update(modality=orig_f.modality)
    if not orig_f.modality and orig_f.name.endswith('mriqc.qa.html'):
        # Special case - mriqc output files do not have modality set, so
        # we must set the modality prior to the classification to avoid errors.
        new_f.update(modality='MR')
    if orig_f.type:
        log.debug('Updating type to %s for %s' % (orig_f.type, orig_f.name))
        new_f.update(type=orig_f.type)
    if orig_f.classification:
        classification_dict = remove_empty_lists_from_dict(orig_f.classification)
        if validate_classification(fw, orig_f.modality, classification_dict, orig_f.name):

            log.debug('Updating classification to %s for %s' % (classification_dict, new_f.name))
            new_f.update_classification(classification_dict)
        else:
            log.error('Not updating classification to %s for %s' % (orig_f.classification, new_f.name))
    if orig_f.info:
        log.debug('Updating info for %s' % (new_f.name))
        new_f.update_info(orig_f.info)


@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=false_if_exc_timeout_or_sub_exists,
                      jitter=backoff.full_jitter)
def _find_or_create_subject(fw, session, project, subject_code):
    # Try to find if a subject with that code already exists in the project
    old_subject = session.subject.reload()
    subject = find_subject(
        fw_client=fw, project_id=project.id, subject_code=subject_code
    )
    if not subject:
        try:
            # Attempt to create the subject. This may fail as a batch-run.py could
            # result in the subject having been created already, thus we try/except
            # and look for the subject again.
            subject = create_subject_copy(
                fw_client=fw, project_id=project.id, subject=old_subject
            )
            return subject, True

        except flywheel.ApiException as e:
            err_str = (
                f'Could not generate subject {subject_code} in project '
                f'{project.id}: {e.status} -- {e.reason}'
            )
            log.warning(err_str)
            raise

    return subject, False


def find_subject(fw_client, project_id, subject_code):
    query_code = quote_numeric_string(subject_code)
    query = f'project={project_id},code={query_code}'
    subject = fw_client.subjects.find_first(query)
    return subject


def create_subject_copy(fw_client, project_id, subject):
    subject_code = subject.code or subject.label
    new_subject = flywheel.Subject(project=project_id,
                                   firstname=subject.firstname,
                                   code=subject_code,
                                   lastname=subject.lastname,
                                   sex=subject.sex,
                                   cohort=subject.cohort,
                                   ethnicity=subject.ethnicity,
                                   race=subject.race,
                                   species=subject.species,
                                   strain=subject.strain,
                                   info=subject.info)
    subject_id = fw_client.add_subject(new_subject)
    subject = fw_client.get_subject(subject_id)
    return subject


@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=false_if_exc_is_timeout)
def _archive_session(fw, session, archive_project):
    """Move session to archive project

        'session_id', help='the id of the session to move'
        'archive_project', help='the label of the project to move the subject to'
    """
    subj_code = session.subject.code or session.subject.label
    subject, _ = _find_or_create_subject(
        fw, session, archive_project, subj_code
    )

    # Move_session_to_subject (archive)
    session.update({'subject': {'_id': subject.id}})


def _create_archive(content_dir, arcname, file_list, zipfilepath=None):
    """Create zip archive from content_dir"""
    if not zipfilepath:
        zipfilepath = content_dir + '.zip'
    with zipfile.ZipFile(zipfilepath, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        for fn in file_list:
            zf.write(os.path.join(content_dir, fn), fn)
    return zipfilepath


def _extract_archive(zip_file_path, extract_location):
    """Extract zipfile to <zip_file_path> and return the path to the directory containing the dicoms,
    which should be the zipfile name without the zip extension."""
    import zipfile

    with zipfile.ZipFile(zip_file_path) as ZF:

        extract_dest = os.path.join(extract_location, os.path.basename(zip_file_path).split('.zip')[0])
        if not os.path.isdir(extract_dest):
            log.debug('Creating extract directory: {}'.format(extract_dest))
            os.mkdir(extract_dest)
        log.debug('Extracting {} archive to: {}'.format(zip_file_path, extract_dest))
        ZF.extractall(extract_dest)
    return extract_dest


def _retrieve_path_list(file_path):
    """ For a given DICOM archive, check to see if it's a zip file.  If it is zip, extract the
        archive, and return a list of all the files in the archive.  If it's not a zip,
        simply return the file. returns a tuple with a list of fille paths and a boolean to indicate
        if the file was a zip or not.
        
 
        
        RETURNS: tuple ( [<file_paths>], is_zip )
        
    """
    file_path = Path(file_path)
    
    if zipfile.is_zipfile(file_path):
        zf = zipfile.ZipFile(file_path)
        is_zip = True
        zip_list = zf.namelist()

    else:
        is_zip = False
        zip_list = [file_path.as_posix()]
    
    # Remove any entries from the list that are directories:
    file_list = []
    for f in zip_list:
        if f and f[-1] != '/':
            file_list.append(f)
    
    
    return((file_list, is_zip))
        

def _export_dicom(dicom_file, tmp_dir, acquisition, session, subject, project, config):
    """ For a given DICOM archive, or file(?) update the DICOM header metadata according to the
        metadata in Flywheel.

        RETURNS: <upload_file_path> path to the updated DICOM archive on disk, which will be uploaded.

    """

    # Download the dicom archive


    dicom_file_path = os.path.join(tmp_dir, get_sanitized_filename(dicom_file.name))
    dicom_file.download(dicom_file_path)
    
    dicom_path_list = _retrieve_path_list(dicom_file_path)


    # This is the header from Flywheel, which may have been modified
    if 'header' in dicom_file.info:
        flywheel_dicom_header = dicom_file.info['header']['dicom']
        # Add backwards compatibility for VM arrays
        fix_type_based_on_dicom_vm(flywheel_dicom_header)
        # For the downloaded file, extract the metadata
        local_dicom_header = dicom_header_extract(
            dicom_file_path,
            flywheel_dicom_header
        )
        if not local_dicom_header:
            log.error(
                'Could not parse DICOM header from %s - file will not be modified prior to upload!',
                dicom_file_path
            )
            return dicom_file_path
    else:
        log.warning('WARNING: Flywheel DICOM does not have DICOM header at info.header.dicom!')
        if config['map_flywheel_to_dicom']:
            log.warning('WARNING! map_flywheel_to_dicom is True, however there is no DICOM header information in Flywheel. Please run.py GRP-3 (medatadata extraction) to read DICOM header data into Flywheel.')
        return dicom_file_path

    # Check if headers match, if not then update local dicom files to match Flywheel Header
    update_keys = compare_dicom_headers(local_dicom_header, flywheel_dicom_header)
    
    # If mapping to flywheel then we do that here
    if config['map_flywheel_to_dicom']:

        log.info('Mapping Flywheel attributes to local DICOMs...')

        # Map Flywheel fields to DICOM fields
        fields_map = {
                "PatientID": subject.get('code', ''),
                "SeriesDescription": acquisition.label,
                "PatientAge": '%sY' % str(session.get('age_years')) if session.get('age_years') else None,
                "PatientWeight": session.get('weight', ''),
                "PatientSex": get_patientsex_from_subject(session.subject),
                "StudyID": session.label  # StudyInstanceUID if SIEMENS
             }

        # Check the flywheel_dicom_header for the fields in the map, if they are not there,
        # or they don't match what FW has, then add/modfiy
        for k, val in fields_map.items():
            if k not in flywheel_dicom_header or (k in flywheel_dicom_header and flywheel_dicom_header[k] != val):
                flywheel_dicom_header[k] = val

                # Add this key to the list of keys to be updated from the FW metadata
                if k not in update_keys:
                    update_keys.append(k)
    # If the list of update_keys is empty, then there's nothing to do with the DICOM archive,
    # thus we just return the dicom_file_path and move on with life.
    if not update_keys:
        return dicom_file_path

    # Iterate through the DICOM files and update the values according to the flywheel_dicom_header
    log.info('The following keys will be updated: {}'.format(update_keys))
    upload_file_path = _modify_dicom_archive(dicom_file_path, update_keys, flywheel_dicom_header, 
                                             dicom_path_list, tmp_dir)


    return upload_file_path


def _modify_dicom_archive(dicom_file_path, update_keys, flywheel_dicom_header, dicom_file_list, tmp_dir):
    """Given a dicom archive <dicom_file_path>, iterate through a list of keys <update_keys> and
    modify each file in the archive with the value in the passed in dict <flywheel_dicom_header>

    # TRY to update they keys, log issues.

    """
    import pydicom
    dicom_files, is_zip = dicom_file_list
    # Extract the archive
    if is_zip:
        dicom_base_folder = _extract_archive(dicom_file_path, tmp_dir)
    else:
        dicom_base_folder, base = os.path.split(dicom_file_path)
    file_path_list = [os.path.join(dicom_base_folder, fname) for fname in dicom_files]
    update_keys = filter_update_keys(update_keys, file_path_list, force=True)
    # Remove the zipfile
    # Still explicitly removing this because we later create a zip archive of the same name
    if os.path.exists(dicom_file_path) and zipfile.is_zipfile(dicom_file_path):
        log.debug('Removing zip file {}'.format(dicom_file_path))
        os.remove(dicom_file_path)

    log.info('Updating {} keys in {} dicom files...'.format(len(update_keys), len(dicom_files)))
    # for df in sorted(dicom_files):
    for df in dicom_files:
        dfp = os.path.join(dicom_base_folder, df)
        log.debug('Reading {}'.format(dfp))
        try:
            dicom = pydicom.read_file(dfp, force=False)
        except:
            log.warning('{} could not be parsed! Attempting to force pydicom to read the file!'.format(df))
            dicom = pydicom.read_file(dfp, force=True)
        log.debug('Modifying: {}'.format(os.path.basename(dfp)))
        for key in update_keys:
            if key in dicom:
                if flywheel_dicom_header.get(key):
                    try:
                        log.debug('key={}, value={}'.format(key, flywheel_dicom_header.get(key)))
                        setattr(dicom, key, flywheel_dicom_header.get(key))
                    except:
                        log.warning('Could not modify DICOM attribute: {}!'.format(key))
                else:
                    log.warning('{} key is empty in Flywheel [{}={}]. DICOM header will remain [{}={}]'.format(key, key, flywheel_dicom_header.get(key), key, dicom.get(key)))
            else:
                if pydicom.datadict.tag_for_keyword(key):  # checking keyword is valid
                    log.info(
                        '{} data element not present in DICOM header. Creating it.'.format(
                            key))
                    setattr(dicom, key, flywheel_dicom_header.get(key))
                else:
                    log.warning(
                        'Unknown DICOM keyword: {}. Date element will not be created.'.format(
                            key))
        log.debug('Saving {}'.format(os.path.basename(dfp)))
        try:
            dicom.save_as(dfp)
        except Exception as err:
            log.error('PYDICOM encountered an error when attempting to save {}!: \n{}'.format(dfp, err))
            raise


    # Package up the archive

    if is_zip:
        log.debug('Packaging archive: {}'.format(dicom_base_folder))
        modified_dicom_file_path = _create_archive(dicom_base_folder,
                                                   os.path.basename(dicom_base_folder),
                                                   dicom_files)
    else:
        modified_dicom_file_path = dicom_file_path

    return modified_dicom_file_path


def remove_empty_lists_from_dict(input_dict):
    new_dict = input_dict.copy()
    for key, value in input_dict.items():
        if not value and isinstance(value, list):
            new_dict.pop(key)
    return new_dict


def class_dict_invalid(classification):
    if not isinstance(classification, dict) or not classification:
        return True
    for value in classification.values():
        if not isinstance(value, list) or len(value) < 1:
            return True
    return False


@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=false_if_exc_is_timeout)
def validate_classification(fw, f_modality, f_classification, f_name):
    """Make sure classification is valid under the modality schema.

    Args:
        fw (flywheel.GearContext.client): gear context client
        f_modality (str): file's instrument type, e.g. 'MR', 'CT', 'PT'
        f_classification ({str: [str, ...]}): key ('Features', 'Intent', or 'Measurement')
            values list (e.g. 'EPI', 'Calibration', 'T1')
        f_name (str): the name that will be used for file upload

    Returns:
        valid_for_modality (bool): True if all values are valid for modality's
            classification schema.
    """

    valid_for_modality = True
    classification_schema = dict()
    if isinstance(f_classification, dict):
        f_classification = remove_empty_lists_from_dict(f_classification)
    if class_dict_invalid(f_classification):
        return False
    if not f_modality:
        # files without modality can only have custom classifications
        if list(f_classification.keys()) != ["Custom"]:
            log_msg = (
                f'File {f_name} does not have a modality.'
                f'Classification {f_classification} invalid for files without '
                'a modality, as it has non-Custom classifications.'
            )
            log.error(log_msg)
            valid_for_modality = False
    try:
        classification_schema = fw.get_modality(f_modality)

    except flywheel.ApiException as exc:

        valid_for_modality = False
        log.error(exc)
    classification_dict = classification_schema.get('classification')
    # Handle Custom appropriately
    if 'Custom' in f_classification:
        custom = f_classification.pop('Custom')
        if not isinstance(custom, list):
            valid_for_modality = False
        elif f_classification and not classification_dict:
            valid_for_modality = False

    if valid_for_modality:

        for key, values in f_classification.items():
            if key in classification_dict:
                for val in values:

                    if val not in classification_dict[key]:
                        log.error('For %s, modality "%s", "%s" is not valid for "%s".' \
                                   % (f_name, f_modality, val, key))
                        valid_for_modality = False

            else:
                log.error('For %s, modality "%s", "%s" is not in classification schema.' \
                            % (f_name, f_modality, key))
                valid_for_modality = False

    return valid_for_modality


def get_file_modality(file_object, export_file_name):
    """
    Parse file modality from file_object
    Args:
        file_object (flywheel.FileEntry): the flywheel file from which to parse
            modality
        export_file_name (str): the name that will be used for file upload

    Returns:
        str or None: the parsed modality
    """
    modality = None
    file_modality = file_object.get("modality")
    if isinstance(file_modality, str) and file_modality != "":
        modality = file_modality
    if not file_modality and export_file_name.endswith('mriqc.qa.html'):
        # Special case - mriqc output files do not have modality set, so
        # we must set the modality prior to the classification to avoid errors.
        modality = "MR"
    if modality:
        log.debug('%s modality will be set to %s' % (export_file_name, modality))
    else:
        log.debug('%s does not have a modality' % export_file_name)
    return modality


def get_file_classification(fw_client, file_object, file_modality, export_file_name):
    """
    Parse and validate the classification from file_object
    Args:
        fw_client (flywheel.Client): an instance of the flywheel client
        file_object (flywheel.FileEntry): the flywheel file from which to parse
            classification
        file_modality (str): modality that has been parsed with get_file_modality
        export_file_name (str): the name that will be used for file upload

    Returns:
        dict or None: the parsed classification

    """
    file_classification = file_object.get("classification", {})
    if isinstance(file_classification, dict):
        file_classification = remove_empty_lists_from_dict(file_classification)
    else:
        file_classification = None
    if file_classification:

        if validate_classification(fw_client, file_modality, file_classification, export_file_name):

            log.debug('File %s classification will be set to %s' % (export_file_name, file_classification))

        else:
            log.error(
                'Classification %s is invalid. Classification will not be set for file %s' % (
                    file_classification,
                    export_file_name
                )
            )
            file_classification = None
    else:
        log.debug('File %s has no classification', export_file_name)
    return file_classification


def format_file_metadata_upload_str(fw_client, file_object, export_file_name):
    """
    Parse and format the metadata string to be provided at upload for file_object

    Args:
        fw_client (flywheel.Client): an instance of the flywheel client
        file_object (flywheel.FileEntry): the flywheel file from which to parse
            metadata
        export_file_name (str): the name that will be used for file upload

    Returns:
        str: the metadata string to use at upload
    """
    metadata_str = "{}"
    file_type = file_object.get("type")
    file_info = file_object.get("info")
    # Parse file modality
    file_modality = get_file_modality(file_object, export_file_name)
    # Parse and validate file classification
    file_classification = get_file_classification(fw_client, file_object, file_modality, export_file_name)
    # Prepare metadata dictionary
    metadata_dict = dict()
    if file_modality:
        metadata_dict["modality"] = file_modality
    if file_classification:
        metadata_dict["classification"] = file_classification
    if file_type:
        metadata_dict["type"] = file_type
    if file_info:
        metadata_dict["info"] = file_info
    if metadata_dict:
        metadata_str = json.dumps(metadata_dict)
    return metadata_str


@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=false_if_exc_is_timeout)
def upload_file_with_metadata(fw_client, origin_file, destination_container, local_file_path):
    """
    Upload the file at local_file_path to destination_container with the metadata from origin_file
    Args:
        fw_client (flywheel.Client): an instance of the flywheel client
        origin_file (flywheel.FileEntry): the fw file that is located at local_file_path
        destination_container: the container to which to upload the file at local_file_path
        local_file_path (str): path to the file to upload

    Returns:
        flywheel.FileEntry or None: the uploaded file object if upload was successful
    """
    export_file_name = os.path.basename(local_file_path)
    # Parse file metadata
    log.debug("Parsing metadata for file %s", export_file_name)
    metadata_str = format_file_metadata_upload_str(fw_client, origin_file, export_file_name)
    # Upload the file to the export_acquisition
    log.debug("Uploading %s to %s" % (export_file_name, destination_container.label))
    # Add logic around retrying failed uploads
    max_attempts = 5
    attempt = 0
    exported_file = None
    while attempt < max_attempts:
        attempt += 1
        fw_client.upload_file_to_container(destination_container.id, local_file_path, metadata=metadata_str)
        # Confirm upload - give as long as 10 seconds to wait for file to appear
        start_time = time.time()
        time_passed = 0
        while time_passed < 10:
            time_passed = time.time() - start_time
            destination_container = destination_container.reload()
            exported_file = destination_container.get_file(export_file_name)

            if exported_file:
                break

        if exported_file:
            log.info('Successfully exported: {}'.format(export_file_name))
            break
        elif attempt < 5:
            log.warning('Upload failed for {} - retrying...'.format(export_file_name))

    return exported_file


@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=false_if_exc_is_timeout)
def _export_files(fw, acquisition, export_acquisition, session, subject, project, config):
    """Export acquisition files to the exported acquisiton.

    For each file in the acquisition:
        1. Download the file
            a. If the file is a DICOM file, modify the DICOM archives individual files to match
               the appropriate metadata as exists in Flywheel.
        2. Upload the file to the export_acquisition
        3. Modify the file in the export_acquisition to have the same metadata

    """

    # Get the acquisition so that the metadata are all there.
    acquisition = fw.get_acquisition(acquisition.id)

    for f in acquisition.files:
        log.info('Exporting %s/%s/%s/%s/%s...' % (project.label,
                                            subject.label,
                                            session.label,
                                            acquisition.label,
                                            f.name))
        
        with tempfile.TemporaryDirectory() as temp_dir:
            
            if f.type == 'dicom':
                upload_file_path = _export_dicom(f, temp_dir, acquisition, session, subject, project, config)
            else:
                upload_file_path = os.path.join(temp_dir, get_sanitized_filename(f.name))
                f.download(upload_file_path)
            export_file_name = os.path.basename(upload_file_path)

            # Upload the file to the export_acquisition
            exported_file = upload_file_with_metadata(fw, f, export_acquisition, upload_file_path)
            if exported_file is None:
                raise RuntimeError(f"Failed to export file {export_file_name} to {export_acquisition.label}")


@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=false_if_exc_is_timeout)
def ok_to_delete_subject(fw_client, subject_dict, session_dict_list):
    """
    Determine whether it is appropriate to delete the subject, based on whether
        the subject has sessions outside of those created by this gear
    Args:
        fw_client (flywheel.Client): an instance of the flywheel client
        subject_dict (dict): a dictionary that contains key 'id' with a str
            value equal to a flywheel subject id
        session_dict_list (list): a list of dicts that contain key 'id' with a
            str value equal to a flywheel session id

    Returns:
        bool: whether the subject can safely be deleted
    """

    session_ids = [sess['id'] for sess in session_dict_list]
    try:
        subject_obj = fw_client.get_subject(subject_dict.get('id'))

        for session in subject_obj.sessions.iter():
            if session.id not in session_ids:
                return False
    except flywheel.rest.ApiException as exc:
        if exc.status in [403, 404]:
            return False
        else:
            raise exc
    return True


@backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                      max_time=300, giveup=false_if_exc_is_timeout)
def _cleanup(fw, creatio):
    """
    In the case of a failure, cleanup all containers that were created.
    """

    acquisitions = [ x for x in creatio if x['container'] == "acquisition" ]
    if acquisitions:
        log.info("Deleting {} acquisition containers".format(len(acquisitions)))
        for a in acquisitions:
            try:
                log.debug(a)
                fw.delete_acquisition(a['id'])
            except flywheel.rest.ApiException as exc:
                if exc.status in [502, 504]:
                    raise exc
                else:
                    a_id = a['id']
                    error_msg = (
                        'Exception encountered when attempting to delete'
                        f' acquisition {a_id}.'
                    )
                    log.error(error_msg, exc_info=True)
                    continue

    sessions = [ x for x in creatio if x['container'] == "session" ]
    if sessions:
        log.info("Deleting {} session containers".format(len(sessions)))
        for s in sessions:
            log.debug(s)
            try:
                fw.delete_session(s['id'])
            except flywheel.rest.ApiException as exc:
                if exc.status in [502, 504]:
                    raise exc
                else:
                    s_id = s['id']
                    error_msg = (
                        'Exception encountered when attempting to delete'
                        f' session {s_id}.'
                    )
                    log.error(error_msg, exc_info=True)
                    continue

    subjects = [ x for x in creatio if x['container'] == "subject" and x['new'] == True ]
    if subjects:
        log.info("Deleting {} subject containers".format(len(subjects)))
        for s in subjects:
            if ok_to_delete_subject(fw, s, sessions):
                log.debug('Deleting subject %s', s.get('id'))
                try:
                    fw.delete_subject(s['id'])
                except flywheel.rest.ApiException as exc:
                    if exc.status in [502, 504]:
                        raise exc
                    else:
                        s_id = s['id']
                        error_msg = (
                            'Exception encountered when attempting to delete'
                            f' subject {s_id}.'
                        )
                        log.error(error_msg, exc_info=True)
                        continue


def main(context):

    fw = context.client

    ########################################################################
    # Lookup destination projects

    archive_project = {}
    export_project = {}

    if context.config.get('export_project'):
        try:
            export_project = fw.lookup(context.config.get('export_project'))
            log.info('Export Project: {}'.format(context.config.get('export_project')))
        except:
            log.error('%s does not exist' % (context.config.get('export_project')))
            raise BaseException()

    if context.config.get('archive_project'):
        try:
            archive_project = fw.lookup(context.config.get('archive_project'))
            log.info('Archive Project: {}'.format(context.config.get('archive_project')))
        except:
            log.error('Archive project %s could not be found!' % (context.config.get('archive_project')))
            raise BaseException()

    ## CHECK FOR PROJECT RULES
    if context.config.get('check_gear_rules'):
        log.info('Checking for enabled gears on the export_project...')
        if any([x for x in fw.get_project_rules(export_project.id) if x.disabled != True]):
            message = "Aborting Session Export: {} has ENABLED GEAR RULES and 'check_gear_rules' == True. If you would like to force the export regardless of enabled gear rules re-run.py the gear with 'check_gear_rules' == False. Warning: Doing so may result in undesired behavior.".format(context.config.get('export_project'))
            log.error(message)
            raise BaseException('Session Export Error')
        else:
            log.info('No enabled rules were found. Moving on...')

    ########################################################################
    # Get the session subject and project and check for export/force

    if fw.get_analysis(context.destination['id']).parent['type'] != 'session':
        log.critical('ONLY SESSION LEVEL EXPORTS ARE SUPPORTED AT THIS TIME!')
        raise BaseException

    session = fw.get_session(fw.get_analysis(context.destination['id']).parent['id'])
    subject = fw.get_subject(session.subject.id)
    project = fw.get_project(session.parents.project)

    exported = True if ('exported' in session.get('tags', [])) else False
    if exported and context.config.get('force_export') == False:
        log.warning('Session {}/{} has already been exported and <force_export> = False. Nothing to do!'.format(subject.code, session.label))
        return


    # Track what is being created
    creatio = []
    creatio_instance = {
                        "container": None,
                        "id": None,
                        "new": None
                        }
    try:
        ########################################################################
        # Create the export_data dict
        export_data = []
        export_instance = {
                            "container": "",
                            "name": "",
                            "status": "",
                            "origin_path": "",
                            "export_path": "",
                            "archive_path": ""
                        }


        ########################################################################
        # Create the subject/session container

        # What we want at the end of this is the export session
        export_session = None


        ########################################################################
        # Create Subject
        subject_code = subject.code or subject.label
        sub_export = export_instance.copy()
        sub_export['container'] = "subject"
        sub_export['name'] = subject.code
        sub_export['origin_path'] = '{}/{}/{}'.format(project.group, project.label, subject_code)
        sub_export['export_path'] = '{}/{}/{}'.format(export_project.group, export_project.label, subject_code)
        if archive_project:
            sub_export['archive_path'] = '{}/{}/{}'.format(archive_project.group, archive_project.label, subject_code)

        subj, created = _find_or_create_subject(
            fw=fw,
            session=session,
            project=export_project,
            subject_code=subject.code
        )
        if created:
            log_str = (
                f'Created subject {subject_code} in project '
                f'{export_project.label}'
            )
            log.info(log_str)
            sub_export['status'] = "created"
            c = creatio_instance.copy()
            c['container'] = 'subject'
            c['id'] = subj.id
            c['new'] = True
            creatio.append(c)

        else:
            log_str = (
                f'Found existing subject {subject_code} in project: '
                f'{export_project.label}. Using existing container.'
            )
            log.info(log_str)
            sub_export['status'] = "used existing"

        export_data.append(sub_export)

        ########################################################################
        # Create the export_session

        log.info('CREATING SESSION CONTAINER {} IN {}/{}'.format(session.label, export_project.label, subject.label))

        # Data logging
        session_export = export_instance.copy()
        session_export['container'] = "session"
        session_export['status'] = "created"
        session_export['name'] = session.label
        session_export['origin_path'] = '{}/{}/{}/{}'.format(project.group, project.label, subject.code, session.label)
        session_export['export_path'] = '{}/{}/{}/{}'.format(export_project.group, export_project.label, subject.code, session.label)
        if archive_project:
            session_export['archive_path'] = '{}/{}/{}/{}'.format(archive_project.group, archive_project.label, subject.code, session.label)
        export_data.append(session_export)

        session_keys = ['age',
                        'info',
                        'label',
                        'operator',
                        'timestamp',
                        'timezone',
                        'weight',
                        'uid'
                       ]
        session_metadata = {}
        for key in session_keys:
            value = session.get(key)
            if value:
                session_metadata[key] = value

        # Add session to the subject
        export_session = subj.add_session(session_metadata)

        c = creatio_instance.copy()
        c['container'] = 'session'
        c['id'] = export_session.id
        c['new'] = True
        creatio.append(c)

        for tag in session.tags:
            export_session.add_tag(tag)

        # Copy over files from old session to new session
        if context.config['export_session_attachments']:
            _copy_files_from_session(fw, session, export_session)
            

        ########################################################################
        # For each acquisition, create the export_acquisition, upload and modify the files

        num_acq = len(session.acquisitions())
        log.info('EXPORTING {} ACQUISITIONS...'.format(num_acq))
        acq_count = 0
        if len(session.acquisitions()) == 0:
            log.warning('NO ACQUISITIONS FOUND ON THE SESSION! Resulting session will have no acquisitions.')

        for acq in session.acquisitions():
            # Reload the acquisition to fully populate the info
            acq = acq.reload()
            # Data logging
            acquisition_export = export_instance.copy()
            acquisition_export['container'] = "acquisition"
            acquisition_export['status'] = "created"
            acquisition_export['name'] = acq.label
            acquisition_export['origin_path'] = '{}/{}/{}/{}/{}'.format(project.group, project.label, subject.code, session.label, acq.label)
            acquisition_export['export_path'] = '{}/{}/{}/{}/{}'.format(export_project.group, export_project.label, subject.code, session.label, acq.label)
            if archive_project:
                acquisition_export['archive_path'] = '{}/{}/{}/{}/{}'.format(archive_project.group, archive_project.label, subject.code, session.label, acq.label)
            export_data.append(acquisition_export)

            acq_count +=1
            log.info('ACQUISITION {}/{}'.format(acq_count, num_acq))
            log.info('CREATING ACQUISITION CONTAINER: [label=%s]' % (acq.label))
            acquisition_keys = ['info',
                                'label',
                                'timestamp',
                                'timezone',
                                'uid'
                               ]
            acquisition_metadata = {}
            for key in acquisition_keys:
                value = acq.get(key)
                if value:
                    acquisition_metadata[key] = value

            # Add acquisition to the session
            export_acquisition = export_session.add_acquisition(acquisition_metadata)

            c = creatio_instance.copy()
            c['container'] = 'acquisition'
            c['id'] = export_acquisition.id
            c['new'] = True
            creatio.append(c)

            for tag in acq.tags:
                export_acquisition.add_tag(tag)

            # Export the individual files in each acquisition
            log.info('Exporting files to %s...' % (export_acquisition.label))
            _export_files(fw, acq, export_acquisition, session, subject, project, context.config)

        log.info('All acquisitions exported.')
        if 'EXPORTED' not in session.get('tags', []):
            log.info('Adding "EXPORTED" tag to {}.'.format(session.label))
            session.add_tag('EXPORTED')


        ########################################################################
        # Optionally move the session to the archive_project or tag the session as exported

        if archive_project:
            log.info('ARCHIVING SESSION TO PROJECT: {}'.format(archive_project.label))
            _archive_session(fw, session, archive_project)


        ########################################################################
        # Generate export log
        
        file_name = '{}-{}_export_log.csv'.format(subject.code, session.label)
        safe_file_name = get_sanitized_filename(file_name)
        
        export_log = os.path.join(context.output_dir, safe_file_name)
        log.info('Generating export log: {}'.format(export_log))
        with open(export_log, 'w') as lf:
            csvwriter = csv.writer(lf, delimiter=',',
                                    quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerow(['Container', 'Name', 'Status', 'Origin Path', 'Export Path', 'Archive Path'])
            for e in export_data:
                csvwriter.writerow([e['container'], e['name'], e['status'], e['origin_path'], e['export_path'], e['archive_path']])
    except Exception:
        # Something failed - so we cleanup the session
        log.exception('ERRORS DETECTED!')
        if context.config.get('cleanup'):
            log.info('CLEANING UP...')
            _cleanup(fw, creatio)
        os._exit(1)


def get_patientsex_from_subject(subject):
    """
    transforms flywheel subject.sex into a string valid for the PatientSex
        DICOM header tag
    Args:
        subject (flywheel.Subject): a flywheel subject container object

    Returns:
        str: empty string (''), 'M', 'F', or 'O'

    """
    if subject.sex in ['male', 'female', 'other']:
        patientsex = subject.sex[0].upper()
        return patientsex
    else:
        return ''
###############################################################################


if __name__ == '__main__':
    with flywheel.GearContext() as context:
        if context.config.get('log_debug'):
            log.setLevel(logging.DEBUG)
        log.info('{}'.format(context.config))
        main(context)
        log.info('DONE!')
        os._exit(0)
