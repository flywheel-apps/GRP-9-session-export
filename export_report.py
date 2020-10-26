import argparse
import flywheel
from util import hash_value
from collections import deque
from queue import Queue


class DiffRecord:
    def __init__(self,):
        self.container_diffs = []  # List of dicts
        self.file_diffs = []  # List of dicts

    def add_record(self, type, msg):
        diff_store = getattr(self, f"{type}_diffs")
        diff_store.append(msg)

    @property
    def get_container_diffs():
        return self.container_diffs

    @property
    def get_file_diffs():
        return self.file_diffs


class ExportComparison:
    def __init__(self, source_container, dest_container, fw):
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

    def compare_children_containers(self, source_finder, dest_finder):

        dest_ids = set()
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
                self.diffs.add_record("container", f"{c.label} not in dest project")
                break
            # Queue source and dest children
            self.queue_children(c, conts[hashes.index(hash)])

    def compare_children_files(self, source, dest):
        source_files = set([file.hash for file in source])
        dest_files = set([file.hash for file in dest])

        # Find hashes in left set not in right
        diff = source_files.difference(dest_files)

        # Find files corresponding to those hashes

        for file in source:
            if file.hash in diff:
                self.diffs.add_record("file", f"file {file.name} not in dest project ")
        # Return files that are in source, but not in destination

    def queue_children(self, source, dest):
        for child_type in source.child_types:
            if child_type == "analyses":
                continue
            source_children = getattr(source, child_type)
            dest_children = getattr(dest, child_type)
            # flywheel.finder.Finder(), None, or list(flywheel.models.file_entry.FileEntry)
            self.queue.put((source_children, dest_children))

    def compare(self):

        self.queue_children(self.s_cont, self.d_cont)

        while not self.queue.empty():
            source, dest = self.queue.get()
            if not source:
                # None or empty list
                continue
            elif isinstance(source, flywheel.finder.Finder):
                # Finder of containers
                self.compare_children_containers(source, dest)

            elif type(source) is list:
                self.compare_children_files(source, dest)

    def report(self):

        print(self.diffs.container_diffs)
        print(self.diffs.file_diffs)


def setup(analysis, fw):
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


##if __name__ == "__main__":
#    parser = argparse.ArgumentParser()
#
#    parser.add_argument("-a", "--analysis", help="Analysis ID of GRP-9")
#    parser.add_argument("-k", "--api-key", help="API key")
#
#    args = parser.parse_args()
#
#    fw = flywheel.Client(args.api_key)
#
#    source_container, dest_container = setup(args.analysis, fw)
#
#    comparison = ExportComparison(source_container, dest_container, fw)
#
#    comparison.compare()
#    comparison.report()

