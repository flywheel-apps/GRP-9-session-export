import argparse
import flywheel
from util import hash_value,
from collections import deque
from queue import Queue

class DiffRecord:
    def __init__(self,):
        self.container_diffs = [] # List of dicts
        self.file_diffs = [] #List of dicts

    def add_record(self, type, msg):
        diff_store = getattr(self, f'{type}_diffs')


queue = Queue()
def main(analysis_id, fw):

    analysis = fw.get_analysis(analysis_id)

    source_id = analysis.parent
    source_container_fn = getattr(fw, f"get_{source_id.type}")
    source_container = source_container_fn(source_id.get("id"))

    dest_proj_id = analysis.job.config.get("export_project")
    dest_proj = fw.projects.find_first(dest_proj_id)

    # TODO: Check if there is a better way to get destination container
    dest_container = getattr(dest_proj, f"{source_id.type}s").find_first(
        (
            f"label={source_container.label}"
            if source_container.label
            else f"code={source_container.code}"
        )
    )


    queue_children(source_container, dest_container)
    
    while not queue.empty():
        source, dest = queue.get()
        if not source:
            # None or empty list
            continue
        elif isinstance(source, flywheel.finder.Finder):
            # Finder of containers
            compare_children_containers(source, dest)
                
        elif type(source) is list:
            # Files
            result = []
            result = compare_files(source, dest)
            if len(result) > 0:
                log.warning(f'Differences found in {}')


def compare_children_containers(source_finder, dest_finder):
    dest_ids = {}
    for cont in dest.iter_find():
        dest_ids.add(cont.info.origin_id)

    for cont in source.iter_find():
        if hash_value(cont.id) not in dest_ids:
            add_record('container', f'{cont.label} not in dest project')
        queue_children(container)


def compare_children_files(source, dest):
    source_files = set([file.hash for file in source])
    dest_files = set([file.hash for file in dest])

    # Find hashes in left set not in right
    diff = source_files.difference(dest_files)
    
    # Find files corresponding to those hashes
    file_diff = [file for file in source if file.hash in diff]

    # Return files that are in source, but not in destination
    return file_diff
        
def queue_children(source, dest):
    for child_type in cont.child_types:
        source_children = getattr(source,child_type)
        dest_children = getattr(dest,child_type)
        # flywheel.finder.Finder(), None, or list(flywheel.models.file_entry.FileEntry)
        queue.put((source_children, dest_children))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-a", "--analysis", help="Analysis ID of GRP-9")
    parser.add_argument("-k", "--api-key", help="API key")

    args = parser.parse_args()

    main(args.analysis, flywheel.Client(args.api_key))

