from __future__ import print_function
import logging
import sys

from ckan.lib.cli import CkanCommand

from ckanext.archiver import utils


REQUESTS_HEADER = {'content-type': 'application/json'}


class Archiver(CkanCommand):
    '''
    Download and save copies of all package resources.

    The result of each download attempt is saved to the CKAN task_status table,
    so the information can be used later for QA analysis.

    Usage:

        paster archiver init
           - Creates the database table archiver needs to run

        paster archiver update [{package-name/id}|{group-name/id}]
           - Archive all resources or just those belonging to a specific
             package or group, if specified

        paster archiver update-test [{package-name/id}|{group-name/id}]
           - Does an archive in the current process i.e. avoiding Celery queue
             so that you can test on the command-line more easily.

        paster archiver clean-status
           - Cleans the TaskStatus records that contain the status of each
             archived resource, whether it was successful or not, with errors.
             It does not change the cache_url etc. in the Resource

        paster archiver clean-cached-resources
           - Removes all cache_urls and other references to resource files on
             disk.

        paster archiver view [{dataset name/id}]
           - Views info archival info, in general and if you specify one, about
             a particular dataset\'s resources.

        paster archiver report [outputfile]
           - Generates a report on orphans, either resources where the path
             does not exist, or files on disk that don't have a corresponding
             orphan. The outputfile parameter is the name of the CSV output
             from running the report

        paster archiver delete-orphans [outputfile]
           - Deletes orphans that are files on disk with no corresponding
             resource. This uses the report command and will write out a
             report to [outputfile]

        paster archiver migrate-archive-dirs
           - Migrate the layout of the archived resource directories.
             Previous versions of ckanext-archiver stored resources on disk
             at: {resource-id}/filename.csv and this version puts them at:
             {2-chars-of-resource-id}/{resource-id}/filename.csv
             Running this moves them to the new locations and updates the
             cache_url on each resource to reflect the new location.

        paster archiver migrate
           - Updates the database schema to include new fields.

        paster archiver size-report
           - Reports on the sizes of files archived.

        paster archiver delete-files-larger-than-max
           - For when you reduce the ckanext-archiver.max_content_length and
             want to delete archived files that are now above the threshold,
             and stop referring to these files in the Archival table of the db.
    '''
    # TODO
    #    paster archiver clean-files
    #       - Remove all archived resources

    summary = __doc__.split('\n')[0]
    usage = __doc__
    min_args = 0
    max_args = 2

    def __init__(self, name):
        super(Archiver, self).__init__(name)
        self.parser.add_option('-q', '--queue',
                               action='store',
                               dest='queue',
                               help='Send to a particular queue')

    def command(self):
        """
        Parse command line arguments and call appropriate method.
        """
        if not self.args or self.args[0] in ['--help', '-h', 'help']:
            print(self.usage)
            sys.exit(1)

        cmd = self.args[0]
        self._load_config()

        # Initialise logger after the config is loaded, so it is not disabled.
        self.log = logging.getLogger(__name__)

        if cmd == 'update':
            self.update()
        elif cmd == 'update-test':
            self.update_test()
        elif cmd == 'clean-status':
            self.clean_status()
        elif cmd == 'clean-cached-resources':
            self.clean_cached_resources()
        elif cmd == 'view':
            if len(self.args) == 2:
                utils.view(self.args[1])
            else:
                utils.view()
        elif cmd == 'report':
            if len(self.args) != 2:
                self.log.error('Command requires a parameter, the name of the output')
                return
            self.report(self.args[1], delete=False)
        elif cmd == 'delete-orphans':
            if len(self.args) != 2:
                self.log.error('Command requires a parameter, the name of the output')
                return
            self.report(self.args[1], delete=True)
        elif cmd == 'init':
            utils.init()
            self.log.info('Archiver tables are initialized')
        elif cmd == 'migrate-archive-dirs':
            self.migrate_archive_dirs()
        elif cmd == 'migrate':
            self.migrate()
        elif cmd == 'size-report':
            self.size_report()
        elif cmd == 'delete-files-larger-than-max':
            self.delete_files_larger_than_max_content_length()
        else:
            self.log.error('Command %s not recognized' % (cmd,))

    def update(self):
        utils.update(self.args[1:], self.options.queue)
        self.log.info('Completed queueing')

    def update_test(self):
        utils.update_test(self.args[1:], self.options.queue)
        self.log.info('Completed test update')

    def clean_status(self):
        utils.clean_status()

    def clean_cached_resources(self):
        utils.clean_cached_resources()

    def report(self, output_file, delete=False):
        utils.report(output_file, delete)

    def migrate(self):
        utils.migrate()

    def migrate_archive_dirs(self):
        utils.migrate_archive_dirs()

    def size_report(self):
        utils.size_report()

    def delete_files_larger_than_max_content_length(self):
        utils.delete_files_larger_than_max_content_length()
