#!/usr/bin/env python
import logging
import os

import flywheel

from container_export import ContainerExporter


log = logging.getLogger("[GRP 9]:")
log.setLevel(logging.INFO)


def main(gear_context):
    exporter = ContainerExporter.from_gear_context(gear_context)
    return exporter.export()


if __name__ == "__main__":
    # with flywheel.GearContext() as context:
    with flywheel.GearContext() as context:
        if context.config.get("log_debug"):
            level = logging.DEBUG
        else:
            level = logging.INFO
        logging.basicConfig(level=level)
        log.info("{}".format(context.config))
        return_code = main(context)
        log.info("DONE!")
        os._exit(return_code)
