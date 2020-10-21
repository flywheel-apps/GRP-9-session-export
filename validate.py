
# TODO capture run.py's main validation functionality with the enhancement of validating as
# many things as possible rather than raising immediately so that users are
# informed of all problems and don't have to iteratively resolve them
# also, we'll support subject-level export
def validate_context(gc):
    """
    Validates the gear_context, and export/archive project states to determine
        if the gear can be executed as configured.
    Args:
        gc (flywheel.GearContext): Gear context object

    Raises:
        RunTimeError if one or more validation problems are identified

    Returns:
        (tuple): tuple containing:
            (flywheel.Project): export project 
            (flywheel.Project or None): archive project
            (flywheel.Session or flywheel.Subject): Destination container 

    """
    # Setup
    errors = []
    fw = context.client
    returns = [None]*3

    #####
    # Lookup destination projects
    if gc.config.get('export_project'):
        log.info('Looking up export_project')
        returns[0] = get_project(fw, gc.config.get('export_project'), errors)

    if gc.config.get('archive_project'):
        returns[1] = get_project(fw, gc.config.get('archive_project'), errors)

    #####
    # Check for projet rules
    if gc.config.get('check_gear_rules'):
        validate_gear_rules(fw, returns[0], errors)

    #####
    # Get and validate destinatino container
    returns[2] = get_destination(fw, gc.destination.get('id'), errors)

    ####
    # Check whether there is work to do
    need_to_export = check_exported(fw, returns[2], gc.config.get('force_export'), errors)

    ####
    # Handle errors
    if len(errors) > 0:
        log.error('Validation Errors: ')
        for err in errors:
            log.error(err)
        #raise RuntimeError() ?
        log.info('Exiting')
        sys.exit(1)

    if not need_to_export:
        # TODO: Fix warning message to include subject compatibity
        log.warning('Session {}/{} has already been exported and <force_export> = False. Nothing to do!'.format(subject.code, session.label))
        log.info('Exiting')

        #raise RuntimeError() ? 
        sys.exit(0)
    
    return tuple(*returns)

# Questions:
# * Raise or exit on error/warning
# * How is context.client pointing to destination instance or is the same api key used across instances?
# * Check the assumption that this is all the same series.

####### TODO: Pretty sure in the following errors will be changed with each function, but I need to check on that.
def get_project(fw, project_name, errors=[]):
    """Lookup given project add to errors if needed

    Args:
        fw (flywheel.Client): Flywheel client 
        project_name (str): Name of project to look up
        errors (list, optional): List of error messages. Defaults to [].
    
    Returns:
        (flywheel.Project or None): Found project or None if not found
    """
    project = None
    try:
        project = fw.lookup(project_name)
        log.info(f'Found Project {project.name}, id {project.id')
    except:
        errors.append(f'Project {project_name} does not exist')

    return project 

def get_destination(fw, dest_id, errors=[]):
    """Get export destination container

    Args:
        fw (flywheel.Client): Flywheel Client
        dest_id (str): Destination id
        errors (list, optional): List of error messages. Defaults to [].

    Returns:
        (flywheel.Subject or flywheel.Session): Destination container, either session or subject
    """
    # Destination will be analysis since this is a gear run.  
    #   Find parent to get export destination container
    dest_analysis = fw.get_analysis(dest_id)
    dest_container = dest_analysis.parent

    log.debug(f'Found destination container id {dest_container.id}')

    if dest_container.get('type') not in ['session','subject']:
        errors.append('Only session and subject level exports are supported at this time!')

    return dest_container

def validate_gear_rules(fw, proj, errors=[]):
    """Validate that there are no enabled gear rules on project

    Args:
        fw (flywheel.Client): Flywheel Client
        proj (flywheel.Project): Project to check for gear rules
        errors (list, optional): List of error messages. Defaults to [].
    """
    proj_name = f"{proj.parents.group}/{proj.label}"
    log.info(f'Checking for enabled gears on {proj_name}...')
    gear_rules = [x for x in fw.get_project_rules(proj.id) if x.disabled != True]
    # Any reason the above is not ... if x.disabled is False ??
    if any(gear_rules):
        message = f"Aborting Session Export: {proj_name} has ENABLED GEAR RULES and 'check_gear_rules' == True. If you would like to force the export regardless of enabled gear rules re-run.py the gear with 'check_gear_rules' == False. Warning: Doing so may result in undesired behavior."
        errors.append(message)
    else:
        log.info('No enabled rules were found. Moving on...')

def check_exported(fw, dest, force_export=True, errors=[]):
    """Check whether given destination container has already been exported

    Args:
        fw (flywheel.Client): flywheel.Client 
        dest (flywheel.Session or flywheel.Subject): Destination container
        errors (list, optional): List of error messages. Defaults to [].

    Returns:
        (bool): Whether container has been exported or not.  
            Return value indicates whether there is anything to 'do' for example, if a container has already been exported
            but force_export is True, then check_export will return True since we still need to export regardless of whether
            or not the container has already been exported
    """
    # Get full container with so we can get tags on container
    try:
        get_fn = getattr(fw,f'get_{dest.container_type')
        full_dest = get_fn(dest.get('id'))

        exported = True if ('exported' in full_dest.get('tags', [])) else False
    except:
        errors.append(f'Could not get tags on destination {dest.container_type} container, id: {dest.id}')

    if exported and force_export is False:
        return True
    else:
        return False



