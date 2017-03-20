import os
import logging
import ckan.plugins as p

from ckan.model.types import make_uuid
from ckan.lib.celery_app import celery

log = logging.getLogger(__name__)


def create_archiver_resource_task(resource, queue):
    from pylons import config
    if p.toolkit.check_ckan_version(max_version='2.2.99'):
        # earlier CKANs had ResourceGroup
        package = resource.resource_group.package
    else:
        package = resource.package
    task_id = '%s/%s/%s' % (package.name, resource.id[:4], make_uuid()[:4])
    ckan_ini_filepath = os.path.abspath(config['__file__'])
    celery.send_task('archiver.update_resource',
                     args=[ckan_ini_filepath, resource.id, queue],
                     task_id=task_id, queue=queue)
    log.debug('Archival of resource put into celery queue %s: %s/%s url=%r',
              queue, package.name, resource.id, resource.url)


def create_archiver_package_task(package, queue):
    from pylons import config
    task_id = '%s/%s' % (package.name, make_uuid()[:4])
    ckan_ini_filepath = os.path.abspath(config['__file__'])
    celery.send_task('archiver.update_package',
                     args=[ckan_ini_filepath, package.id, queue],
                     task_id=task_id, queue=queue)
    log.debug('Archival of package put into celery queue %s: %s',
              queue, package.name)


def get_extra_from_pkg_dict(pkg_dict, key, default=None):
    for extra in pkg_dict.get('extras', []):
        if extra['key'] == key:
            return extra['value']
    return default
