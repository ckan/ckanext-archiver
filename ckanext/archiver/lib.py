import logging
import ckan.plugins.toolkit as tk
from ckanext.archiver.tasks import update_package, update_resource

log = logging.getLogger(__name__)


def create_archiver_resource_task(resource, queue):
    package = resource.package

    tk.enqueue_job(update_resource, [resource.id], queue=queue)

    log.debug(
        "Archival of resource put into celery queue %s: %s/%s url=%r",
        queue,
        package.name,
        resource.id,
        resource.url,
    )


def create_archiver_package_task(package, queue):
    tk.enqueue_job(update_package, [package.id], queue=queue)

    log.debug("Archival of package put into celery queue %s: %s", queue, package.name)


def get_extra_from_pkg_dict(pkg_dict, key, default=None):
    for extra in pkg_dict.get("extras", []):
        if extra["key"] == key:
            return extra["value"]
    return default
