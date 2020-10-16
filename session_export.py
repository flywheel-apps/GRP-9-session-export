import logging

from collections import namedtuple
from copy import deepcopy

import backoff
import flywheel
from flywheel.models.mixins import ContainerBase

from util import hash_value, quote_numeric_string
from run import false_if_exc_is_timeout, false_if_exc_timeout_or_sub_exists


CONTAINER_KWARGS_KEYS = {
    "acquisition": ("label", "timestamp", "timezone", "uid"),
    "session": ("age", "label", "operator", "timestamp", "timezone", "uid", "weight"),
    "subject": ("code", "cohort", "ethnicity", "firstname", "label", "lastname", "race", "sex", "species", "strain"),
    "project": ("description",)
}

EXCLUDE_TAGS = ["EXPORTED"]


class ContainerExportRecord:
    """
    Attributes:
        origin_container (ContainerBase): the container to export
        origin_id (str): the value to store on copy containers' info.export.origin_id
            to enable future retrieval
        container_type (str): origin_container.container_type for concise notation

    """
    def __init__(self, origin_container, export_log=None):
        self.origin_container = origin_container
        # sha256 hash of origin_container.id
        self.origin_id = hash_value(origin_container.id)
        self.container_type = origin_container.container_type
        self.export_log = export_log
        self._create_container_kwargs = None
        self._container_copy_find_queries = None
        self._origin_parent_id = None
        self._log = None

    @property
    def log(self):
        if hasattr(self.export_log, "logger"):
            self._log = self.export_log.logger
        else:
            self._log = logging.getLogger(__name__)
        return self._log

    @property
    def origin_parent_id(self):
        """The id of self.origin_container's parent"""
        for parent_type, parent_id in self.origin_container.parents.items():
            if self.origin_container.get(parent_type) == parent_id:
                self._origin_parent_id = parent_id
                break

        return self._origin_parent_id

    @property
    def create_container_kwargs(self):
        """
        dictionary to be provided as kwargs to a destination_parent's
            add_<self.origin_container.container_type> method
        """
        container_kwargs_keys = CONTAINER_KWARGS_KEYS.get(self.container_type)
        kwargs_dict = dict()
        for key in container_kwargs_keys:
            origin_container_key_value = self.origin_container.get(key)
            if origin_container_key_value not in [None, {}, []]:
                kwargs_dict[key] = origin_container_key_value
        kwargs_dict["info"] = deepcopy(self.origin_container.info)
        kwargs_dict["info"]["export"] = {"origin_id": self.origin_id}
        self._create_container_kwargs = kwargs_dict
        return self._create_container_kwargs

    @property
    def container_copy_find_queries(self):
        """tuple of query strings used by the self.find_container_copy method"""
        # Subject label/code has forced uniqueness
        if self.container_type == "subject":
            self._container_copy_find_queries = (
                # Search by label first
                f"label={quote_numeric_string(self.origin_container.label)}",
                # label is unreliable so also search code
                f"code={quote_numeric_string(self.origin_container.code)}"
            )
        else:
            self._container_copy_find_queries = (
                f"info.export.origin_id={self.origin_id},label={quote_numeric_string(self.origin_container.label)}",
            )

        return self._container_copy_find_queries

    def find_container_copy(self, destination_parent):
        """
        Returns an existing copy of self.origin_container or the original origin_container
            if it exists on the destination_parent, otherwise returns None

        Args:
            destination_parent (ContainerBase): the parent container of the container
                copy to be found

        Returns:
            ContainerBase or None: the found destination container or None
        """
        container_copy = None
        if self.origin_parent_id == destination_parent.id:
            self.log.debug("Destination %s %s is %s %s's parent! No copies to find.", destination_parent.container_type, destination_parent.id, self.container_type, self.origin_container.id)
            container_copy = self.origin_container
        else:
            find_first_func = getattr(getattr(destination_parent, f'{self.container_type}s'), 'find_first')
            for query in self.container_copy_find_queries:
                result = find_first_func(query)
                # Fully populate container metadata if a container is returned
                if isinstance(result, flywheel.models.mixins.ContainerBase):
                    container_copy = result.reload()
                    self.log.debug("Found a copy of %s %s on %s %s", self.container_type, self.origin_container.id, destination_parent.container_type, destination_parent.id)
                    break
            # If we didn't return a result, we did not find anything
            else:
                self.log.debug("Did not find a copy of %s %s on %s %s", self.container_type, self.origin_container.id,
                          destination_parent.container_type, destination_parent.id)

        return container_copy

    def create_container_copy(self, destination_parent):
        """
        Creates and returns a copy of self.origin_container on destination_parent
        Args:
            destination_parent (ContainerBase): the parent container of the container copy
                if not provided, defaults to self.destination_parent
        Returns:
            ContainerBase: the created container
        """

        # For example, project.add_subject, subject.add_session, session.add_acquisition
        create_container_func = getattr(destination_parent, f"add_{self.origin_container.container_type}")
        created_container = create_container_func(**self.create_container_kwargs)
        # tags must be added using the add_tag method
        for tag in self.origin_container.tags:
            if tag not in EXCLUDE_TAGS:
                created_container.add_tag(tag)

        return created_container

    @backoff.on_exception(backoff.expo, flywheel.rest.ApiException,
                          max_time=300, giveup=false_if_exc_timeout_or_sub_exists,
                          jitter=backoff.full_jitter)
    def find_or_create_container_copy(self, destination_parent):
        """
        First tries to find a copy of or the original self.origin_id on destination_parent and
            creates a copy if one is not found

        Args:

            destination_parent (ContainerBase): the parent container of the
                container copy

        Returns:
            tuple(ContainerBase, bool): the found/created container and whether
                it was created

        """
        created = False
        found_container = self.find_container_copy(destination_parent)
        if found_container is None:
            created = True
            return_container = self.create_container_copy(destination_parent)
        else:
            return_container = found_container
        return return_container, created


class Exporter:
    def __init__(self, export_project, archive_project, destination_container):
        self.export_project = export_project
        self.archive_project = archive_project
        self.destination_container = destination_container
