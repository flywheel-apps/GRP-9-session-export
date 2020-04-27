#!/usr/bin/env python

import os
import re
import sys
import csv
import json
import time
import pprint
import zipfile
import pydicom
import logging
import flywheel
from pprint import pprint as pp
from dicom_metadata import dicom_header_extract
from util import quote_numeric_string, ensure_filename_safety

logging.basicConfig()
log = logging.getLogger('[GRP 9]:')
log.setLevel(logging.INFO)


###############################################################################
# LOCAL FUNCTION DEFINITIONS

def _find_or_create_subject(fw, session, project, subject_code):
    # Try to find if a subject with that code already exists in the project
    query_code = quote_numeric_string(subject_code)
    subject = fw.subjects.find_first(filter='project={},code={}'.format(project.id, query_code))
    if not subject:
        # If the subject does not exist in the project, make one with the same metadata
        old_subject = session.subject
        new_subject = flywheel.Subject(project=project.id,
                                       firstname=old_subject.firstname,
                                       code=subject_code,
                                       lastname=old_subject.lastname,
                                       sex=old_subject.sex,
                                       cohort=old_subject.cohort,
                                       ethnicity=old_subject.ethnicity,
                                       race=old_subject.race,
                                       species=old_subject.species,
                                       strain=old_subject.strain,
                                       files=old_subject.files)

        # Attempt to create the subject. This may fail as a batch-run.py could
        # result in the subject having been created already, thus we try/except
        # and look for the subject again.
        try:
            response = fw.add_subject(new_subject)
            subject = fw.get_subject(response)
        except flywheel.ApiException as e:
            log.warning('Could not generate subject: {} -- {}'.format(e.status, e.reason))
            log.info('Attempting to find subject...')
            time.sleep(2)
            subject = fw.subjects.find_first(filter='project={},code={}'.format(project.id, query_code))
            if subject:
               log.info('... found subject {}'.format(subject.code))
            else:
               raise

    return subject


def _archive_session(fw, session, archive_project):
    """Move session to archive project

        'session_id', help='the id of the session to move'
        'archive_project', help='the label of the project to move the subject to'
    """

    subject = _find_or_create_subject(fw, session, archive_project, session.subject.code)

    # Move_session_to_subject (archive)
    session.update({'subject': {'_id': subject.id}})


def _create_archive(content_dir, arcname, zipfilepath=None):
    """Create zip archive from content_dir"""
    if not zipfilepath:
        zipfilepath = content_dir + '.zip'
    with zipfile.ZipFile(zipfilepath, 'w', zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
        zf.write(content_dir, arcname)
        for fn in os.listdir(content_dir):
            zf.write(os.path.join(content_dir, fn), os.path.join(os.path.basename(arcname), fn))
    return zipfilepath


def _extract_archive(zip_file_path, extract_location):
    """Extract zipfile to <zip_file_path> and return the path to the directory containing the dicoms,
    which should be the zipfile name without the zip extension."""
    import zipfile
    if not zipfile.is_zipfile(zip_file_path):
        # If this file isn't a zipfile...zip is so we can then upzip it.  Shut up, this is easiest.
        log.warning('{} is not a Zip File!'.format(zip_file_path))
        file_path, base = os.path.split(zip_file_path)
        return(file_path)
        # new_zip_path = '{}.zip'.format(zip_file_path)
        # 
        # log.info('creating {}'.format(new_zip_path))
        # 
        # with zipfile.ZipFile(new_zip_path,'w') as zip:
        #     zip.write(zip_file_path)
        #     zip.close()
        #     
        # os.remove(zip_file_path)
        # zip_file_path = new_zip_path

    with zipfile.ZipFile(zip_file_path) as ZF:
        # Comments here would be very helpful.
        log.debug(ZF.namelist())
        if '/' in ZF.namelist()[0]:
            extract_dest = os.path.join(extract_location, ZF.namelist()[0].split('/')[0])
            ZF.extractall(extract_location)
            return extract_dest
        else:
            extract_dest = os.path.join(extract_location, os.path.basename(zip_file_path).split('.zip')[0])
            if not os.path.isdir(extract_dest):
                log.debug('Creating extract directory: {}'.format(extract_dest))
                os.mkdir(extract_dest)
            log.debug('Extracting {} archive to: {}'.format(zip_file_path, extract_dest))
            ZF.extractall(extract_dest)
            return extract_dest


def _export_dicom(dicom_file, acquisition, session, subject, project, config):
    """ For a given DICOM archive, or file(?) update the DICOM header metadata according to the
        metadata in Flywheel.

        RETURNS: <upload_file_path> path to the updated DICOM archive on disk, which will be uploaded.

    """

    update_keys = []

    # Download the dicom archive
    dicom_file_path = os.path.join('/tmp', dicom_file.name)
    dicom_file.download(dicom_file_path)

    # For the downloaded file, extract the metadata
    local_dicom_header = dicom_header_extract(dicom_file_path)
    if not local_dicom_header:
        log.error('Could not parse DICOM header from %s - file will not be modified prior to upload!')
        return dicom_file_path
    # This is the header from Flywheel, which may have been modified
    if 'header' in dicom_file.info:
        flywheel_dicom_header = dicom_file.info['header']['dicom']
    else:
        log.warning('WARNING: Flywheel DICOM does not have DICOM header at info.header.dicom!')
        if config['map_flywheel_to_dicom']:
            log.warning('WARNING! map_flywheel_to_dicom is True, however there is no DICOM header information in Flywheel. Please run.py GRP-3 (medatadata extraction) to read DICOM header data into Flywheel.')
        return dicom_file_path

    # Check if headers match, if not then update local dicom files to match Flywheel Header
    if local_dicom_header != flywheel_dicom_header:

        log.info('Local DICOM header and Flywheel header do NOT match...')

        # Generate a list of keys that need to be updated within the local dicom file
        # Compare the headers, and track which keys are different
        for key in sorted(flywheel_dicom_header.keys()):
            if key not in local_dicom_header:
                log.info('MISSING key: %s not found in local_header' % (key))
            else:

                # Make sure we're comapring the header from the same file...
                if local_dicom_header['SOPInstanceUID'] != flywheel_dicom_header['SOPInstanceUID']:
                    log.warning('WARNING: SOPInstanceUID does not match across the headers!!!')

                # Check if the headers are equal
                if local_dicom_header[key] != flywheel_dicom_header[key]:
                    log.info('MISMATCH in key: {}'.format(key))
                    log.info('DICOM    = {}'.format(local_dicom_header[key]))
                    log.info('Flywheel = {}'.format(flywheel_dicom_header[key]))
                    update_keys.append(key)
    else:
        log.info('Local DICOM header and Flywheel headers match!')

    # If mapping to flywheel then we do that here
    if config['map_flywheel_to_dicom']:

        log.info('Mapping Flywheel attributes to local DICOMs...')

        # Map Flywheel fields to DICOM fields
        fields_map = {
                "PatientID": subject.get('code',''),
                "SeriesDescription": acquisition.label,
                "PatientAge": '%sY' % str(session.get('age_years')) if session.get('age_years') else None,
                "PatientWeight": session.get('weight',''),
                "PatientSex": session.subject.get('sex', ''),
                "StudyID": session.label # StudyInstanceUID if SIEMENS
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
    upload_file_path = _modify_dicom_archive(dicom_file_path, update_keys, flywheel_dicom_header)


    return upload_file_path


def _modify_dicom_archive(dicom_file_path, update_keys, flywheel_dicom_header):
    """Given a dicom archive <dicom_file_path>, iterate through a list of keys <update_keys> and
    modify each file in the archive with the value in the passed in dict <flywheel_dicom_header>

    # TRY to update they keys, log issues.

    """
    import pydicom

    # Extract the archive
    dicom_base_folder = _extract_archive(dicom_file_path, '/tmp')

    # Remove the zipfile
    if os.path.exists(dicom_file_path) and zipfile.is_zipfile(dicom_file_path):
        log.debug('Removing zip file {}'.format(dicom_file_path))
        os.remove(dicom_file_path)


    # For each file in the archive, update the keys
    dicom_files = os.listdir(dicom_base_folder)
    log.info('Updating {} keys in {} dicom files...'.format(len(update_keys), len(dicom_files)))
    for df in sorted(dicom_files):
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
        log.debug('Saving {}'.format(os.path.basename(dfp)))
        try:
            dicom.save_as(dfp)
        except Exception as err:
            log.error('PYDICOM encountered an error when attempting to save {}!: \n{}'.format(dfp, err))
            raise


    # Package up the archive

    if zipfile.is_zipfile(dicom_file_path):
        log.debug('Packaging archive: {}'.format(dicom_base_folder))
        modified_dicom_file_path = _create_archive(dicom_base_folder, os.path.basename(dicom_base_folder))
    else:
        modified_dicom_file_path = dicom_file_path

    return modified_dicom_file_path


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
        if f.type == 'dicom':
            upload_file_path = _export_dicom(f, acquisition, session, subject, project, config)
        else:
            upload_file_path = os.path.join('/tmp', f.name)
            f.download(upload_file_path)

        # Upload the file to the export_acquisition
        log.debug("Uploading %s to %s" % (f.name, export_acquisition.label))

        # Add logic around retrying failed uploads
        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
            attempt +=1
            s = export_acquisition.upload_file(upload_file_path)
            log.info('Upload status = {}'.format(s))
            export_acquisition = fw.get_acquisition(export_acquisition.id)
            file_names = [ x.name for x in export_acquisition.files ]
            log.debug(file_names)
            if os.path.basename(upload_file_path) not in file_names:
                log.warning('Upload failed for {} - retrying...'.format(os.path.basename(upload_file_path)))
            else:
                log.info('Successfully exported: {}'.format(os.path.basename(upload_file_path)))
                break

        # Delete the uploaded file locally.
        log.debug('Removing local file: %s' % (upload_file_path))
        os.remove(upload_file_path)

        # Update file metadata
        if f.modality:
            log.debug('Updating modality to %s for %s' % (f.modality, f.name))
            export_acquisition.update_file(f.name, modality=f.modality)
        if not f.modality and f.name.endswith('mriqc.qa.html'):
            # Special case - mriqc output files do not have modality set, so
            # we must set the modality prior to the classification to avoid errors.
            export_acquisition.update_file(f.name, modality='MR')
        if f.type:
            log.debug('Updating type to %s for %s' % (f.type, f.name))
            export_acquisition.update_file(f.name, type=f.type)
        if f.classification:
            log.debug('Updating classification to %s for %s' % (f.classification, f.name))
            export_acquisition.update_file_classification(f.name, f.classification)
        if f.info:
            log.debug('Updating info for %s' % (f.name))
            export_acquisition.update_file_info(f.name, f.info)

        export_acquisition.reload()


def _cleanup(fw, creatio):
    """
    In the case of a failure, cleanup all containers that were created.
    """

    acquisitions = [ x for x in creatio if x['container'] == "acquisition" ]
    if acquisitions:
        log.info("Deleting {} acquisition containers".format(len(acquisitions)))
        for a in acquisitions:
            log.debug(a)
            fw.delete_acquisition(a['id'])

    sessions = [ x for x in creatio if x['container'] == "session" ]
    if sessions:
        log.info("Deleting {} session containers".format(len(sessions)))
        for s in sessions:
            log.debug(s)
            fw.delete_session(s['id'])

    subjects = [ x for x in creatio if x['container'] == "subject" and x['new'] == True ]
    if subjects:
        log.info("Deleting {} subject containers".format(len(subjects)))
        for s in subjects:
            log.debug(s)
            fw.delete_subject(s['id'])


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

        # Check for subject in a given project

        subj = export_project.subjects.find_first('code=%s' % quote_numeric_string(subject.code))


        ########################################################################
        # Create Subject

        sub_export = export_instance.copy()
        sub_export['container'] = "subject"
        sub_export['name'] = subject.code
        sub_export['origin_path'] = '{}/{}/{}'.format(project.group, project.label, subject.code)
        sub_export['export_path'] = '{}/{}/{}'.format(export_project.group, export_project.label, subject.code)
        if archive_project:
            sub_export['archive_path'] = '{}/{}/{}'.format(archive_project.group, archive_project.label, subject.code)

        if not subj:
            log.info('Subject %s does not exist in project %s.' % (subject.code, export_project.label))
            log.info('CREATING SUBJECT CONTAINER')
            sub_export['status'] = "created"
            subject_keys = ['code',
                            'cohort',
                            'ethnicity',
                            'firstname',
                            'info',
                            'label',
                            'lastname',
                            'race',
                            'sex',
                            'species',
                            'strain',
                            'tags',
                            'type'
                           ]
            subject_metadata = {}
            for key in subject_keys:
                value = subject.get(key)
                if value:
                    subject_metadata[key] = value

            # Attempt to create the subject. This may fail as a batch-run.py could
            # result in the subject having been created already, thus we try/except
            # and look for the subject again.
            try:
                subj = export_project.add_subject(subject_metadata)
                log.info('Created %s in %s' % (subj.code, export_project.label))

                c = creatio_instance.copy()
                c['container'] = 'subject'
                c['id'] = subj.id
                c['new'] = True
                creatio.append(c)
            except flywheel.ApiException as e:
                log.warning('Could not generate subject: {} -- {}'.format(e.status, e.reason))
                log.info('Attempting to find subject...')
                time.sleep(2)
                subj = export_project.subjects.find_first('code=%s' % quote_numeric_string(subject.code))
                if subj:
                    log.info('... found existing subject %s in project: %s. Using existing container.'
                             % (subj.code, export_project.label))
                    sub_export['status'] = "used existing"
                else:
                    raise
        else:
            log.info('Found existing subject %s in project: %s. Using existing container.' % (subj.code, export_project.label))
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
        safe_file_name = ensure_filename_safety(file_name)
        
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


###############################################################################

if __name__ == '__main__':
    with flywheel.GearContext() as context:
        if context.config.get('log_debug'):
            log.setLevel(logging.DEBUG)
        log.info('{}'.format(context.config))
        main(context)
        log.info('DONE!')
        os._exit(0)
