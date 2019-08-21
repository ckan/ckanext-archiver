import logging

from ckan import model
from ckan import plugins as p

from ckanext.report.interfaces import IReport
from ckanext.archiver.interfaces import IPipe
from ckanext.archiver.logic import action, auth
from ckanext.archiver import helpers
from ckanext.archiver import lib
from ckanext.archiver.model import Archival, aggregate_archivals_for_a_dataset

log = logging.getLogger(__name__)


class ArchiverPlugin(p.SingletonPlugin, p.toolkit.DefaultDatasetForm):
    """
    Registers to be notified whenever CKAN resources are created or their URLs
    change, and will create a new ckanext.archiver celery task to archive the
    resource.
    """
    p.implements(p.IDomainObjectModification, inherit=True)
    p.implements(IReport)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)
    p.implements(p.ITemplateHelpers)
    p.implements(p.IPackageController, inherit=True)

    # IDomainObjectModification

    def notify(self, entity, operation=None):
        if not isinstance(entity, model.Package):
            return

        log.debug('Notified of package event: %s %s', entity.name, operation)

        run_archiver = \
            self._is_it_sufficient_change_to_run_archiver(entity, operation)
        if not run_archiver:
            return

        log.debug('Creating archiver task: %s', entity.name)

        lib.create_archiver_package_task(entity, 'priority')

    def _is_it_sufficient_change_to_run_archiver(self, package, operation):
        ''' Returns True if in this revision any of these happened:
        * it is a new dataset
        * dataset licence changed (affects qa)
        * there are resources that have been added or deleted
        * resources have changed their URL or format (affects qa)
        '''
        if operation == 'new':
            log.debug('New package - will archive')
            # even if it has no resources, QA needs to show 0 stars against it
            return True
        elif operation == 'deleted':
            log.debug('Deleted package - won\'t archive')
            return False
        # therefore operation=changed

        # check to see if resources are added, deleted or URL changed

        # look for the latest revision
        rev_list = package.all_related_revisions
        if not rev_list:
            log.debug('No sign of previous revisions - will archive')
            return True
        # I am not confident we can rely on the info about the current
        # revision, because we are still in the 'before_commit' stage. So
        # simply ignore that if it's returned.
        if rev_list[0][0].id == model.Session.revision.id:
            rev_list = rev_list[1:]
        if not rev_list:
            log.warn('No sign of previous revisions - will archive')
            return True
        previous_revision = rev_list[0][0]
        log.debug('Comparing with revision: %s %s',
                  previous_revision.timestamp, previous_revision.id)

        # get the package as it was at that previous revision
        context = {'model': model, 'session': model.Session,
                   # 'user': c.user or c.author,
                   'ignore_auth': True,
                   'revision_id': previous_revision.id}
        data_dict = {'id': package.id}
        try:
            old_pkg_dict = p.toolkit.get_action('package_show')(
                context, data_dict)
        except p.toolkit.NotFound:
            log.warn('No sign of previous package - will archive anyway')
            return True

        # has the licence changed?
        old_licence = (old_pkg_dict['license_id'],
                       lib.get_extra_from_pkg_dict(old_pkg_dict, 'licence')
                       or None)
        new_licence = (package.license_id,
                       package.extras.get('licence') or None)
        if old_licence != new_licence:
            log.debug('Licence has changed - will archive: %r->%r',
                      old_licence, new_licence)
            return True

        # have any resources been added or deleted?
        old_resources = dict((res['id'], res)
                             for res in old_pkg_dict['resources'])
        old_res_ids = set(old_resources.keys())
        new_res_ids = set((res.id for res in package.resources))
        deleted_res_ids = old_res_ids - new_res_ids
        if deleted_res_ids:
            log.debug('Deleted resources - will archive. res_ids=%r',
                      deleted_res_ids)
            return True
        added_res_ids = new_res_ids - old_res_ids
        if added_res_ids:
            log.debug('Added resources - will archive. res_ids=%r',
                      added_res_ids)
            return True

        # have any resources' url/format changed?
        for res in package.resources:
            for key in ('url', 'format'):
                old_res_value = old_resources[res.id][key]
                new_res_value = getattr(res, key)
                if old_res_value != new_res_value:
                    log.debug('Resource %s changed - will archive. '
                              'id=%s pos=%s url="%s"->"%s"',
                              key, res.id[:4], res.position,
                              old_res_value, new_res_value)
                    return True

            was_in_progress = old_resources[res.id].get('upload_in_progress', None)
            is_in_progress = res.extras.get('upload_in_progress', None)
            if was_in_progress != is_in_progress:
                log.debug('Resource %s upload finished - will archive. ', 'upload_finished')
                return True

            log.debug('Resource unchanged. pos=%s id=%s',
                      res.position, res.id[:4])

        log.debug('No new, deleted or changed resources - won\'t archive')
        return False

    # IReport

    def register_reports(self):
        """Register details of an extension's reports"""
        from ckanext.archiver import reports
        return [reports.broken_links_report_info,
                ]

    # IConfigurer

    def update_config(self, config):
        p.toolkit.add_template_directory(config, 'templates')

    # IActions

    def get_actions(self):
        return {
            'archiver_resource_show': action.archiver_resource_show,
            'archiver_dataset_show': action.archiver_dataset_show,
            }

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            'archiver_resource_show': auth.archiver_resource_show,
            'archiver_dataset_show': auth.archiver_dataset_show,
            }

    # ITemplateHelpers

    def get_helpers(self):
        return dict((name, function) for name, function
                    in helpers.__dict__.items()
                    if callable(function) and name[0] != '_')

    # IPackageController

    def after_show(self, context, pkg_dict):
        # Insert the archival info into the package_dict so that it is
        # available on the API.
        # When you edit the dataset, these values will not show in the form,
        # it they will be saved in the resources (not the dataset). I can't see
        # and easy way to stop this, but I think it is harmless. It will get
        # overwritten here when output again.
        archivals = Archival.get_for_package(pkg_dict['id'])
        if not archivals:
            return
        # dataset
        dataset_archival = aggregate_archivals_for_a_dataset(archivals)
        pkg_dict['archiver'] = dataset_archival
        # resources
        archivals_by_res_id = dict((a.resource_id, a) for a in archivals)
        for res in pkg_dict['resources']:
            archival = archivals_by_res_id.get(res['id'])
            if archival:
                archival_dict = archival.as_dict()
                del archival_dict['id']
                del archival_dict['package_id']
                del archival_dict['resource_id']
                res['archiver'] = archival_dict


class TestIPipePlugin(p.SingletonPlugin):
    """
    """
    p.implements(IPipe, inherit=True)

    def __init__(self, *args, **kwargs):
        self.calls = []

    def reset(self):
        self.calls = []

    def receive_data(self, operation, queue, **params):
        self.calls.append([operation, queue, params])
