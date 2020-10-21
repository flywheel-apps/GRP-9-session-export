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
        tuple(export_project (flywheel.Project), archive_project(None or flywheel.Project), destination_container (flywheel.Session or flywheel.Subject))

    """
    # Setup
    errors = []
    fw = context.client
    returns = [None] * 3
    ########################################################################
    # Lookup destination projects

    archive_project = {}
    export_project = {}

    if gc.config.get("export_project"):
        try:
            export_project = fw.lookup(gc.config.get("export_project"))
            log.info("Export Project: {}".format(gc.config.get("export_project")))
            returns[0] = export_project
        except:
            errors.append("%s does not exist" % (gc.config.get("export_project")))

    if gc.config.get("archive_project"):
        try:
            archive_project = fw.lookup(gc.config.get("archive_project"))
            log.info("Archive Project: {}".format(gc.config.get("archive_project")))
            returns[1] = archive_project
        except:
            errors.append(
                "Archive project %s could not be found!"
                % (gc.config.get("archive_project"))
            )

    ## CHECK FOR PROJECT RULES
    if gc.config.get("check_gear_rules"):
        log.info("Checking for enabled gears on the export_project...")
        if any(
            [x for x in fw.get_project_rules(export_project.id) if x.disabled != True]
        ):
            message = "Aborting Session Export: {} has ENABLED GEAR RULES and 'check_gear_rules' == True. If you would like to force the export regardless of enabled gear rules re-run.py the gear with 'check_gear_rules' == False. Warning: Doing so may result in undesired behavior.".format(
                gc.config.get("export_project")
            )
            errors.append(message)
        else:
            log.info("No enabled rules were found. Moving on...")

    ########################################################################
    # Get the session subject and project and check for export/force
    # Since this is a gear run it creates an analysis as destination
    dest_analysis = fw.get_analysis(gc.destination.get("id"))
    dest_container = dest_analysis.parent
    returns[2] = dest_container

    if dest_container.get("type") not in ["session", "subject"]:
        errors.append(
            "ONLY SESSION AND SUBJECT LEVEL EXPORTS ARE SUPPORTED AT THIS TIME!"
        )

    session = fw.get_session(dest_container.get("id"))
    subject = fw.get_subject(session.subject.id)
    project = fw.get_project(session.parents.project)

    nothing_to_do = False
    exported = True if ("exported" in session.get("tags", [])) else False
    if exported and gc.config.get("force_export") == False:
        nothing_to_do = True

    # Handle errors
    if len(errors) > 0:
        log.error("Validation Errors: ")
        for err in errors:
            log.error(err)
        # raise RuntimeError() ?
        log.info("Exiting")
        sys.exit(1)

    if nothing_to_do:
        log.warning(
            "Session {}/{} has already been exported and <force_export> = False. Nothing to do!".format(
                subject.code, session.label
            )
        )
        log.info("Exiting")

        # raise RuntimeError() ?
        sys.exit(0)

    return tuple(*returns)


# Questions:
# * Raise or exit on error/warning
# * How is context.client pointing to destination instance or is the same api key used across instances?


def graceful_exit(errors):
    """Exit gracefully after printing all errors

    Args:
        errors (List[str]): List of errors to print
    """
    for error in errors:
        log.error(error)
    sys.exit(1)
