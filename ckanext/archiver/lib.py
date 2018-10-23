import os
import logging
import ckan.plugins as p

from ckanext.archiver.tasks import update_package, update_resource

log = logging.getLogger(__name__)


def compat_enqueue(name, fn, queue, args=None):
    u'''
    Enqueue a background job using Celery or RQ.
    '''
    try:
        # Try to use RQ
        from ckan.plugins.toolkit import enqueue_job
        enqueue_job(fn, args=args, queue=queue)
    except ImportError:
        # Fallback to Celery
        import uuid
        from ckan.lib.celery_app import celery
        celery.send_task(name, args=args + [queue], task_id=str(uuid.uuid4()))


def create_archiver_resource_task(resource, queue):
    from pylons import config
    if p.toolkit.check_ckan_version(max_version='2.2.99'):
        # earlier CKANs had ResourceGroup
        package = resource.resource_group.package
    else:
        package = resource.package
    ckan_ini_filepath = os.path.abspath(config['__file__'])

    compat_enqueue('archiver.update_resource', update_resource, queue, [ckan_ini_filepath, resource.id])

    log.debug('Archival of resource put into celery queue %s: %s/%s url=%r',
              queue, package.name, resource.id, resource.url)


def create_archiver_package_task(package, queue):
    from pylons import config
    ckan_ini_filepath = os.path.abspath(config['__file__'])

    compat_enqueue('archiver.update_package', update_package, queue, [ckan_ini_filepath, package.id])

    log.debug('Archival of package put into celery queue %s: %s',
              queue, package.name)


def get_extra_from_pkg_dict(pkg_dict, key, default=None):
    for extra in pkg_dict.get('extras', []):
        if extra['key'] == key:
            return extra['value']
    return default
