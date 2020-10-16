
# TODO capture run.py's main validation functionality with the enhancement of validating as
# many things as possible rather than raising immediately so that users are
# informed of all problems and don't have to iteratively resolve them
# also, we'll support subject-level export
def validate_context(gear_context):
    """
    Validates the gear_context, and export/archive project states to determine
        if the gear can be executed as configured.
    Args:
        gear_context (flywheel.GearContext):

    Raises:
        RunTimeError if one or more validation problems are identified

    Returns:
        tuple(export_project (flywheel.Project), archive_project(None or flywheel.Project), destination_container (flywheel.Session or flywheel.Subject))

    """

