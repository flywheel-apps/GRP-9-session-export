import argparse
import flywheel
from util import hash_value
from queue import Queue
import sys

import logging


log = logging.getLogger(__name__)


class DiffRecord:
    """Class to contain records of difference between source and destination containers
    """

    def __init__(self):
        self.records = []  # List of dicts

    def add_record(self, c_type, msg):
        """Add a record for the given container type with a message

        Args:
            c_type (str): Container type or 'file', i.e. 'session','subject' or 'file'
            msg (str): Difference message
        """
        self.records.append([c_type, msg])

    @property
    def get_records(self):
        return self.records

    def iter_records(self):
        for record in self.records:
            yield record


class ExportComparison:
    """Class for comparing source and destination containers.  Loops through child containers and reports differences in containers and files.  This would mainly be used to produce a human and/or machine readable list of differences between a source and exported container
    """

    def __init__(self, source_container, dest_container, fw):
        """Initializer

        Args:
            source_container (flywheel.Subject or flywheel.Session): Source container
            dest_container (flywheel.Subject or flywheel.Session): Destination or exported container
            fw (flywheel.Flywheel): Flywheel client
        """
        self.diffs = DiffRecord()
        self.queue = Queue()
        self.s_cont = source_container
        self.d_cont = dest_container
        self.fw = fw
        self.c_type = source_container.container_type

        if source_container.container_type != dest_container.container_type:
            log.error("You have chosen poorly")
            sys.exit(1)

        # Other?

    def compare_children_containers(self, source_finder, dest_finder, name):
        """Function to compare containers and their children

        Args:
            source_finder (flywheel.finder.Finder): Finder of containers from source container
            dest_finder (flywheel.finder.Finder): Finder of containers from destination container
            name (str): Parent container name, used for reporting differences as paths
        """

        hashes = []
        conts = []
        for c in dest_finder.iter_find():
            c = c.reload()
            hashes.append(c.info.get("export").get("origin_id"))
            conts.append(c)
        hash_set = set(hashes)
        for c in source_finder.iter_find():
            hash = hash_value(c.id)
            if hash not in hash_set:
                self.diffs.add_record(
                    "container", f"{name}/{c.label} not in dest project"
                )
            else:
                # Queue source and dest children
                self.queue_children(c, conts[hashes.index(hash)], prepend=f"{name}/")

    def compare_children_files(self, source, dest, name):
        """Function to compare files that are children of a container

        Args:
            source (List[flywheel.FileEntry]): Children files form source container
            dest (List[flywheel.FileEntry]): Children files from source container
            name (str): Parent container name, used for reporting differences as paths
        """
        source_files = set([file.hash for file in source])
        dest_files = set([file.hash for file in dest])

        # Find hashes in left set not in right
        diff = source_files.difference(dest_files)

        # Find files corresponding to those hashes

        for file in source:
            if file.hash in diff:
                self.diffs.add_record(
                    "file", f"{name}/: file {file.name} not in dest project"
                )

    def queue_children(self, source, dest, prepend="/"):
        """Add children of source and destination container to the queue

        Args:
            source (flywheel.Session, flywheel.Subject, or flywheel.Acquisition): Source container
            dest (flywheel.Session, flywheel.Subject, or flywheel.Acquisition): Destination container
            prepend (str, optional): Optional string to prepend onto name, used to build path to report differences. Defaults to "/".
        """
        for child_type in source.child_types:
            if child_type == "analyses":
                continue
            source_children = getattr(source, child_type)
            dest_children = getattr(dest, child_type)
            # flywheel.finder.Finder(), None, or list(flywheel.models.file_entry.FileEntry)
            name = prepend + (source.label if source.label else source.code)
            self.queue.put((name, [source_children, dest_children]))

    def compare(self):
        """Main algorithm to compare two containers.  Maintains a queue and pops elements from queue until empty.  For each element popped from the queue, the relevant compare method is called: compare_children_containers if a finder is found, or compare_children_files if a list is found.
        """

        self.queue_children(self.s_cont, self.d_cont, prepend="")

        while not self.queue.empty():
            name, [source, dest] = self.queue.get()
            if not source:
                # None or empty list
                continue
            elif isinstance(source, flywheel.finder.Finder):
                # Finder of containers
                self.compare_children_containers(source, dest, name)

            elif type(source) is list:
                self.compare_children_files(source, dest, name)

    def report(self):
        """Print out report of container and file differences
        """
        for record in self.diffs.iter_records():
            log.warning(f"{record[0]} -- {record[1]}")


def setup(analysis, fw):
    """Find source and destination container of the given session-export analysis ID

    Args:
        analysis (str): Analysis ID
        fw (flywheel.Flywheel): Flywheel client

    Returns:
        tuple: Source and destination container
            (flywheel.Session or flywheel.Subject): Source container
            (flywheel.Session or flywheel.Subject): Destination container
    """
    analysis = fw.get_analysis(analysis)

    source_id = analysis.parent
    source_container_fn = getattr(fw, f"get_{source_id.type}")
    source_container = source_container_fn(source_id.get("id"))

    dest_proj_id = analysis.job.config.get("config").get("export_project")
    dest_proj_lookup = fw.lookup(dest_proj_id)
    dest_proj = fw.get_project(dest_proj_lookup.id)

    # May have scale issues
    dest_container_finder = getattr(dest_proj, f"{source_id.type}s")
    dest_container = dest_container_finder.find_first(
        f"info.export.origin_id={hash_value(source_container.id)}"
    )

    return source_container, dest_container


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-a", "--analysis", help="Analysis ID of GRP-9")
    parser.add_argument("-k", "--api-key", help="API key")

    args = parser.parse_args()

    fw = flywheel.Client(args.api_key)

    source_container, dest_container = setup(args.analysis, fw)

    comparison = ExportComparison(source_container, dest_container, fw)

    comparison.compare()
    comparison.report()

