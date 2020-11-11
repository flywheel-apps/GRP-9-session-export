import logging
import sys

import backoff
import flywheel


log = logging.getLogger(__name__)


def false_if_exc_is_not_found_or_forbidden(exception):
    if hasattr(exception, "status"):
        if exception.status in [401, 403, 404, 405, 422]:
            return True
    return False


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
    fw = gc.client
    export_project, archive_project, destination = None, None, None

    #####
    # Lookup destination projects
    # At this time no error would be thrown if neither export, or archive project is passed.
    if gc.config.get("export_project"):
        log.info("Looking up export_project")
        try:
            export_project = get_project(fw, gc.config.get("export_project"))
        except flywheel.rest.ApiException:
            log.error(
                f"Could not find export project {gc.config.get('export_project')}",
                exc_info=True,
            )
            sys.exit(1)

    if gc.config.get("archive_project"):
        try:
            archive_project = get_project(fw, gc.config.get("archive_project"))
        except flywheel.rest.ApiException:
            log.error(
                f"Could not find archive project {gc.config.get('export_project')}",
                exc_info=True,
            )
            sys.exit(1)

    if export_project is None:
        log.error("Export project needs to be specified", exc_info=True)
        sys.exit(1)

    #####
    # Check for projet rules
    if gc.config.get("check_gear_rules"):
        rules = validate_gear_rules(fw, export_project)
        if rules:
            message = f"Aborting Session Export: {export_project.label} has ENABLED GEAR RULES and 'check_gear_rules' == True. If you would like to force the export regardless of enabled gear rules re-run.py the gear with 'check_gear_rules' == False. Warning: Doing so may result in undesired behavior."
            log.error(message, exc_info=True)
            sys.exit(1)
        else:
            log.info("No enabled rules were found. Moving on...")

    #####
    # Get and validate destination container
    try:
        destination = get_destination(fw, gc.destination.get("id"))
    except flywheel.rest.ApiException:
        log.error(
            f"Could not find destination with id {gc.destination.get('id')}",
            exc_info=True,
        )
        sys.exit(1)
    except ValueError as e:
        log.error(e.args[0], exc_info=True)
        sys.exit(1)

    ####
    # Check whether there is work to do
    need_to_export = container_needs_export(destination, gc.config)

    if not need_to_export:
        log.warning(
            f"{destination.container_type} {destination.label} has already been exported and <force_export> = False. Nothing to do!"
        )
        log.info("Exiting")

        # raise RuntimeError() ?
        sys.exit(0)

    return export_project, archive_project, destination


@backoff.on_exception(
    backoff.expo,
    flywheel.rest.ApiException,
    max_time=300,
    giveup=false_if_exc_is_not_found_or_forbidden,
)
def get_project(fw, project_name):
    """Lookup given project add to errors if needed

    Args:
        fw (flywheel.Client): Flywheel client 
        project_name (str): Name of project to look up
        errors (list, optional): List of error messages. Defaults to [].
    
    Returns:
        (flywheel.Project or None): Found project or None if not found
    """
    project = None
    project = fw.lookup(project_name)
    log.debug(f"Found Project {project.label}, id {project.id}")
    return project


@backoff.on_exception(
    backoff.expo,
    flywheel.rest.ApiException,
    max_time=300,
    giveup=false_if_exc_is_not_found_or_forbidden,
)
def get_destination(fw, dest_id, errors=[]):
    """Get export destination container

    Args:
        fw (flywheel.Client): Flywheel Client
        dest_id (str): Destination id

    Returns:
        (flywheel.Subject or flywheel.Session): Destination container, either session or subject
    """
    # Destination will be analysis since this is a gear run.
    #   Find parent to get export destination container

    dest_analysis = fw.get_analysis(dest_id)
    dest_container = fw.get(dest_analysis.parent.id)

    log.debug(f"Found destination container id {dest_container.id}")

    if dest_container.container_type not in ["session", "subject"]:
        raise ValueError(
            "Only session and subject level exports are supported at this time!"
        )

    return dest_container


@backoff.on_exception(
    backoff.expo,
    flywheel.rest.ApiException,
    max_time=300,
    giveup=false_if_exc_is_not_found_or_forbidden,
)
def validate_gear_rules(fw, proj):
    """Validate that there are no enabled gear rules on project

    Args:
        fw (flywheel.Client): Flywheel Client
        proj (flywheel.Project): Project to check for gear rules
    
    Returns:
        (bool): True when export should proceed, False if there are enabled gear rules on the export_project
    """
    proj_name = f"{proj.parents.group}/{proj.label}"
    log.info(f"Checking for enabled gears on {proj_name}...")
    gear_rules = [x for x in fw.get_project_rules(proj.id) if x.disabled != True]

    if any(gear_rules):
        return False
    else:
        return True


def container_needs_export(container, config):
    """Check whether given destination container has already been exported

    Args:
        container (flywheel.Subject or flywheel.Session): Container to check for export
        config (dict): Gear configuration

    Returns:
        (bool): Whether container has been exported or not.  
            Return value indicates whether there is anything to 'do' for example, if a container has already been exported
            but force_export is True, then check_export will return True since we still need to export regardless of whether
            or not the container has already been exported
    """
    force_export = config.get("force_export", False)
    exported_tag = bool(
        "EXPORTED" in container.get("tags") if container.get("tags") else list()
    )
    return force_export or not exported_tag
