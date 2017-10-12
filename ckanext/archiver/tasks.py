import os
import hashlib
import httplib
import requests
import json
import urllib
import urlparse
import tempfile
import shutil
import datetime
import copy
import mimetypes
import re
import routes
import time
import urlparse

from requests.packages import urllib3

from ckan.common import _
_ = lambda value: value
from ckan.lib.celery_app import celery
from ckan.lib import uploader
from ckan import plugins as p
from ckanext.archiver import interfaces as archiver_interfaces
from ckan import model

toolkit = p.toolkit

ALLOWED_SCHEMES = set(('http', 'https', 'ftp'))

USER_AGENT = 'ckanext-archiver'

ERRNO_BLOCK = {
    101: 'Network is unreachable'
}

def load_config(ckan_ini_filepath):
    import paste.deploy
    config_abs_path = os.path.abspath(ckan_ini_filepath)
    conf = paste.deploy.appconfig('config:' + config_abs_path)
    import ckan
    ckan.config.environment.load_environment(conf.global_conf,
                                             conf.local_conf)

    ## give routes enough information to run url_for
    parsed = urlparse.urlparse(conf.get('ckan.site_url', 'http://0.0.0.0'))
    request_config = routes.request_config()
    request_config.host = parsed.netloc + parsed.path
    request_config.protocol = parsed.scheme

    load_translations(conf.get('ckan.locale_default', 'en'))


def load_translations(lang):
    # Register a translator in this thread so that
    # the _() functions in logic layer can work

    import ckan.lib.i18n
    from paste.registry import Registry
    from pylons import translator
    from pylons import request

    registry = Registry()
    registry.prepare()

    class FakePylons:
            translator = None
    fakepylons = FakePylons()
    class FakeRequest:
        # Stores details of the translator
        environ = {'pylons.pylons': fakepylons}
    registry.register(request, FakeRequest())

    # create translator
    ckan.lib.i18n.set_lang(lang)

    # pull out translator and register it
    registry.register(translator, fakepylons.translator)

class ArchiverError(Exception):
    pass
class ArchiverErrorBeforeDownloadStarted(ArchiverError):
    pass
class DownloadException(ArchiverError):
    pass
class ArchiverErrorAfterDownloadStarted(ArchiverError):
    def __init__(self, msg, url_redirected_to=None):
        super(ArchiverError, self).__init__(msg)
        self.url_redirected_to = url_redirected_to
class DownloadError(ArchiverErrorAfterDownloadStarted):
    pass
class ArchiveError(ArchiverErrorAfterDownloadStarted):
    pass
class ChooseNotToDownload(ArchiverErrorAfterDownloadStarted):
    pass
class NotChanged(ArchiverErrorAfterDownloadStarted):
    pass
class LinkCheckerError(ArchiverError):
    pass
class LinkInvalidError(LinkCheckerError):
    pass
class LinkHeadRequestError(LinkCheckerError):
    pass
class LinkHeadMethodNotSupported(LinkCheckerError):
    pass
class CkanError(ArchiverError):
    pass


@celery.task(name="archiver.update_resource")
def update_resource(ckan_ini_filepath, resource_id, queue='bulk'):
    '''
    Archive a resource.
    '''
    load_config(ckan_ini_filepath)

    log = update_resource.get_logger()
    log.info('Starting update_resource task: res_id=%r queue=%s', resource_id, queue)

    # HACK because of race condition #1481
    time.sleep(2)

    # Do all work in a sub-routine since it can then be tested without celery.
    # Also put try/except around it is easier to monitor ckan's log rather than
    # celery's task status.
    try:
        result = _update_resource(resource_id, queue, log)
        return result
    except Exception, e:
        if os.environ.get('DEBUG'):
            raise
        # Any problem at all is logged and reraised so that celery can log it too
        log.error('Error occurred during archiving resource: %s\nResource: %r',
                  e, resource_id)
        raise

@celery.task(name="archiver.update_package")
def update_package(ckan_ini_filepath, package_id, queue='bulk'):
    '''
    Archive a package.
    '''
    load_config(ckan_ini_filepath)

    log = update_package.get_logger()
    log.info('Starting update_package task: package_id=%r queue=%s',
             package_id, queue)

    # Do all work in a sub-routine since it can then be tested without celery.
    # Also put try/except around it is easier to monitor ckan's log rather than
    # celery's task status.
    try:
        _update_package(package_id, queue, log)
    except Exception, e:
        if os.environ.get('DEBUG'):
            raise
        # Any problem at all is logged and reraised so that celery can log it
        # too
        log.error('Error occurred during archiving package: %s\nPackage: %s',
                  e, package_id)
        raise


def _update_package(package_id, queue, log):
    from ckan import model

    get_action = toolkit.get_action

    num_archived = 0
    context_ = {'model': model, 'ignore_auth': True, 'session': model.Session}
    package = get_action('package_show')(context_, {'id': package_id})

    for resource in package['resources']:
        resource_id = resource['id']
        res = _update_resource(resource_id, queue, log)
        if res:
            num_archived += 1

    if num_archived > 0:
        log.info("Notifying package as %d items were archived", num_archived)
        notify_package(package, queue)
    else:
        #log.info("Not notifying package as 0 items were archived")
        log.info("Notifying even though 0 items were archived")
        notify_package(package, queue)
    # Refresh the index for this dataset, so that it contains the latest
    # archive info. However skip it if there are downstream plugins that will
    # do this anyway, since it is an expensive step to duplicate.
    if 'qa' not in get_plugins_waiting_on_ipipe():
        _update_search_index(package_id, log)
    else:
        log.info('Search index skipped %s', package['name'])


def _update_search_index(package_id, log):
    '''
    Tells CKAN to update its search index for a given package.
    '''
    from ckan import model
    from ckan.lib.search.index import PackageSearchIndex
    package_index = PackageSearchIndex()
    context_ = {'model': model, 'ignore_auth': True, 'session': model.Session,
                'use_cache': False, 'validate': False}
    package = toolkit.get_action('package_show')(context_, {'id': package_id})
    package_index.index_package(package, defer_commit=False)
    log.info('Search indexed %s', package['name'])


def _update_resource(resource_id, queue, log):
    """
    Link check and archive the given resource.
    If successful, updates the archival table with the cache_url & hash etc.
    Finally, a notification of the archival is broadcast.

    Params:
      resource - resource dict
      queue - name of the celery queue

    Should only raise on a fundamental error:
      ArchiverError
      CkanError

    Returns a JSON dict, ready to be returned from the celery task giving a
    success status:
        {
            'resource': the updated resource dict,
            'file_path': path to archived file (if archive successful), or None
        }
    If not successful, returns None.
    """
    from ckan import model
    from pylons import config
    from ckan.plugins import toolkit
    from ckanext.archiver import default_settings as settings
    from ckanext.archiver.model import Status, Archival

    get_action = toolkit.get_action

    assert is_id(resource_id), resource_id
    context_ = {'model': model, 'ignore_auth': True, 'session': model.Session}
    resource = get_action('resource_show')(context_, {'id': resource_id})

    if not os.path.exists(settings.ARCHIVE_DIR):
        log.info("Creating archive directory: %s" % settings.ARCHIVE_DIR)
        os.mkdir(settings.ARCHIVE_DIR)

    def _save(status_id, exception, resource, url_redirected_to=None,
              download_result=None, archive_result=None):
        reason = u'%s' % exception
        save_archival(resource, status_id,
                      reason, url_redirected_to,
                      download_result, archive_result,
                      log)
        notify_resource(
            resource,
            queue,
            archive_result.get('cache_filename') if archive_result else None)

    # Download
    try_as_api = False
    # Dont need to archive
    requires_archive = True

    url = resource['url']
    if not url.startswith('http'):
        url = config['ckan.site_url'].rstrip('/') + url

    hosted_externally = not url.startswith(config['ckan.site_url'])
    # if resource.get('resource_type') == 'file.upload' and not hosted_externally:
    if resource.get('url_type') == 'upload' and not hosted_externally:
        log.info("Won't attemp to archive resource uploaded locally: %s" % resource['url'])

        upload = uploader.ResourceUpload(resource)
        filepath = upload.get_path(resource['id'])

        try:
            hash, length = _file_hashnlength(filepath)
        except IOError, e:
            log.error('Error while accessing local resource %s: %s', filepath, e)

            download_status_id = Status.by_text('URL request failed')
            _save(download_status_id, e, resource)
            return

        mimetype = None
        headers = None
        content_type, content_encoding = mimetypes.guess_type(url)
        if content_type:
            mimetype = _clean_content_type(content_type)
            headers = {'Content-Type': content_type}

        download_result_mock = {'mimetype': mimetype,
            'size': length,
            'hash': hash,
            'headers': headers,
            'saved_file': filepath,
            'url_redirected_to': url,
            'request_type': 'GET'}

        archive_result_mock = {'cache_filepath': filepath,
        'cache_url': url}

        # Success
        _save(Status.by_text('Archived successfully'), '', resource,
            download_result_mock['url_redirected_to'], download_result_mock, archive_result_mock)

        # The return value is only used by tests. Serialized for Celery.
        return json.dumps(dict(download_result_mock, **archive_result_mock))
        # endif: processing locally uploaded resource


    log.info("Attempting to download resource: %s" % resource['url'])
    download_result = None
    download_status_id = Status.by_text('Archived successfully')
    context = {
        'site_url': config.get('ckan.site_url_internally') or config['ckan.site_url'],
        'cache_url_root': config.get('ckanext-archiver.cache_url_root'),
        'previous': Archival.get_for_resource(resource_id)
        }
    try:
        download_result = download(context, resource)
    except NotChanged, e:
        download_status_id = Status.by_text('Content has not changed')
        try_as_api = False
        requires_archive = False
    except LinkInvalidError, e:
        download_status_id = Status.by_text('URL invalid')
        try_as_api = False
    except DownloadException, e:
        download_status_id = Status.by_text('Download error')
        try_as_api = True
    except DownloadError, e:
        download_status_id = Status.by_text('Download error')
        try_as_api = True
    except ChooseNotToDownload, e:
        download_status_id = Status.by_text('Chose not to download')
        try_as_api = False
    except Exception, e:
        if os.environ.get('DEBUG'):
            raise
        log.error('Uncaught download failure: %r, %r', e, e.args)
        try:
            _save(Status.by_text('Download failure'), e, resource)
        except:
            pass
        return

    if not Status.is_ok(download_status_id):
        log.info('GET error: %s - %r, %r "%s"',
                 Status.by_id(download_status_id), e, e.args,
                 resource.get('url'))

        if try_as_api:
            download_result = api_request(context, resource)
            if download_result:
                download_status_id = Status.by_text('Archived successfully')
            # else the download_status_id (i.e. an error) is left what it was
            # from the previous download (i.e. not when we tried it as an API)

        if not try_as_api or not Status.is_ok(download_status_id):
            extra_args = [e.url_redirected_to] if 'url_redirected_to' in e else []
            _save(download_status_id, e, resource, *extra_args)
            return

    if not requires_archive:
        # We don't need to archive if the remote content has not changed
        return None

    # Archival
    log.info('Attempting to archive resource')
    try:
        #Default archive_result since we are foregoing archiving
        archive_result = {'cache_filepath': 'None', 'cache_url': 'None'}
        #archive_result = archive_resource(context, resource, log, download_result)
    except ArchiveError, e:
        log.error('System error during archival: %r, %r', e, e.args)
        _save(Status.by_text('System error during archival'), e, resource, download_result['url_redirected_to'])
        return

    # Success
    _save(Status.by_text('Archived successfully'), '', resource,
            download_result['url_redirected_to'], download_result, archive_result)

    # The return value is only used by tests. Serialized for Celery.
    return json.dumps(dict(download_result, **archive_result))


def download(context, resource, url_timeout=20,
             max_content_length='default',
             method='GET'):
    '''Given a resource, tries to download it.

    Params:
      resource - dict of the resource

    Exceptions from tidy_url may be propagated:
       LinkInvalidError if the URL is invalid

    If there is an error performing the download, raises:
       DownloadException - connection problems etc.
       DownloadError - HTTP status code is an error or 0 length

    If download is not suitable (e.g. too large), raises:
       ChooseNotToDownload

    If the basic GET fails then it will try it with common API
    parameters (SPARQL, WMS etc) to get a better response.

    Returns a dict of results of a successful download:
      mimetype, size, hash, headers, saved_file, url_redirected_to
    '''
    from ckanext.archiver import default_settings as settings
    from pylons import config
    log = update_resource.get_logger()

    if max_content_length == 'default':
        max_content_length = settings.MAX_CONTENT_LENGTH

    url = resource['url']
    url = tidy_url(url)

    if (resource.get('url_type') == 'upload' and
            not url.startswith('http')):
        url = context['site_url'].rstrip('/') + url

    hosted_externally = not url.startswith(config['ckan.site_url'])
    if resource.get('url_type') == 'upload' and hosted_externally:
        # ckanext-cloudstorage for example does that

        # enable ckanext-archiver.archive_cloud for qa to work on cloud resources
        # till https://github.com/ckan/ckanext-qa/issues/48 is resolved
        # Warning: this will result in double storage of all files below archival filesize limit

        if not config.get('ckanext-archiver.archive_cloud', False):
            raise ChooseNotToDownload('Skipping resource hosted externally to download resource: %s'
                                      % url,  url)

    headers = _set_user_agent_string({})

    #Check if url is black listed first
    resource_blacklist = RemoteResource(url)    
    is_blacklisted = resource_blacklist.check_url_blacklist()
    
    if is_blacklisted:
        raise ChooseNotToDownload(_("Url is broken"))

    # start the download - just get the headers
    # May raise DownloadException
    method_func = {'GET': requests.get, 'POST': requests.post}[method]
    res = requests_wrapper(log, method_func, resource_blacklist, url, timeout=url_timeout,
                           stream=True, headers=headers,
                           verify=verify_https() 
                           )

    url_redirected_to = res.url if url != res.url else None
    
    if context.get('previous') and ('etag' in res.headers):
        if context.get('previous').etag == res.headers['etag']:
            log.info("ETAG matches, not downloading content")
            raise NotChanged("etag suggests content has not changed")
    
    if not res.ok:  # i.e. 404 or something
        raise DownloadError('Server reported status error: %s %s' %
                            (res.status_code, res.reason),
                            url_redirected_to)
    else:
        resource_blacklist.delete_url_blacklist()

    log.info('GET started successfully. Content headers: %r', res.headers)

    # record headers
    mimetype = _clean_content_type(res.headers.get('content-type', '').lower())

    # make sure resource content-length does not exceed our maximum
    content_length = res.headers.get('content-length')

    if content_length:
        try:
            content_length = int(content_length)
        except ValueError:
            # if there are multiple Content-Length headers, requests
            # will return all the values, comma separated
            if ',' in content_length:
                try:
                    content_length = int(content_length.split(',')[0])
                except ValueError:
                    pass
    if isinstance(content_length, int) and \
       int(content_length) >= max_content_length:
            # record fact that resource is too large to archive
            log.warning('Resource too large to download: %s > max (%s). '
                        'Resource: %s %r', content_length,
                        max_content_length, resource['id'], url)
            raise ChooseNotToDownload(_('Content-length %s exceeds maximum '
                                      'allowed value %s') %
                                      (content_length, max_content_length),
                                      url_redirected_to)
    # content_length in the headers is useful but can be unreliable, so when we
    # download, we will monitor it doesn't go over the max.

    # continue the download - stream the response body
    #def get_content():
        #return res.content
    #instead of getting content, get the first 500 bytes
    def get_content():
        for chunk in res.iter_content(chunk_size=500):
            saved_chunk = chunk
            break
        return saved_chunk

    log.info('Downloading the body')
    content = requests_wrapper(log, get_content, resource_blacklist)

    # APIs can return status 200, but contain an error message in the body
    if response_is_an_api_error(content):
        raise DownloadError(_('Server content contained an API error message: %s') % \
                            content[:250],
                            url_redirected_to)

    content_length = len(content)
    
    if content_length > max_content_length:
        raise ChooseNotToDownload(_("Content-length %s exceeds maximum allowed value %s") %
                                  (content_length, max_content_length),
                                  url_redirected_to)

    log.info('Saving resource')
    try:
        #We are forgoing saving the resource, writing the response content to disk
        length, hash, saved_file_path = 3, 'None', 'None'
        #length, hash, saved_file_path = _save_resource(resource, res, max_content_length)
    except ChooseNotToDownload, e:
        raise ChooseNotToDownload(str(e), url_redirected_to)
    log.info('Resource was not saved or written to disk')
   #log.info('Resource saved. Length: %s File: %s', length, saved_file_path)

    # zero length (or just one byte) indicates a problem
    if length < 2:
        # record fact that resource is zero length
        log.warning('Resource found was length %i - not archiving. Resource: %s %r',
                 length, resource['id'], url)
        raise DownloadError(_("Content-length after streaming was %i") % length,
                            url_redirected_to)
    log.info('Resource not downloaded')
    #log.info('Resource downloaded: id=%s url=%r cache_filename=%s length=%s hash=%s',
  #           resource['id'], url, saved_file_path, length, hash)

    return {'mimetype': mimetype,
            'size': length,
            'hash': hash,
            'headers': dict(res.headers),
            'saved_file': saved_file_path,
            'url_redirected_to': url_redirected_to,
            'request_type': method}

def _file_hashnlength(local_path):
    BLOCKSIZE = 65536
    hasher = hashlib.sha1()
    length = 0
    
    with open(local_path, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            length += len(buf)

            buf = afile.read(BLOCKSIZE)

    return (unicode(hasher.hexdigest()), length)

def archive_resource(context, resource, log, result=None, url_timeout=30):
    """
    Archive the given resource. Moves the file from the temporary location
    given in download().

    Params:
       result - result of the download(), containing keys: length, saved_file

    If there is a failure, raises ArchiveError.

    Returns: {cache_filepath, cache_url}
    """
    from ckanext.archiver import default_settings as settings
    relative_archive_path = os.path.join(resource['id'][:2], resource['id'])
    archive_dir = os.path.join(settings.ARCHIVE_DIR, relative_archive_path)
    if not os.path.exists(archive_dir):
        os.makedirs(archive_dir)
    # try to get a file name from the url
    parsed_url = urlparse.urlparse(resource.get('url'))
    try:
        file_name = parsed_url.path.split('/')[-1] or 'resource'
        file_name = file_name.strip()  # trailing spaces cause problems
        file_name = file_name.encode('ascii', 'ignore')  # e.g. u'\xa3' signs
    except:
        file_name = "resource"

    # move the temp file to the resource's archival directory
    saved_file = os.path.join(archive_dir, file_name)
    shutil.move(result['saved_file'], saved_file)
    log.info('Going to do chmod: %s', saved_file)
    try:
        os.chmod(saved_file, 0644)  # allow other users to read it
    except Exception, e:
        log.error('chmod failed %s: %s', saved_file, e)
        raise
    log.info('Archived resource as: %s', saved_file)

    # calculate the cache_url
    if not context.get('cache_url_root'):
        log.warning('Not saved cache_url because no value for '
                    'ckanext-archiver.cache_url_root in config')
        raise ArchiveError(_('No value for ckanext-archiver.cache_url_root in config'))
    cache_url = urlparse.urljoin(context['cache_url_root'],
                                 '%s/%s' % (relative_archive_path, file_name))
    return {'cache_filepath': saved_file,
            'cache_url': cache_url}


def notify_resource(resource, queue, cache_filepath):
    '''
    Broadcasts an IPipe notification that an resource archival has taken place
    (or at least the archival object is changed somehow).
    '''
    archiver_interfaces.IPipe.send_data('archived',
                                        resource_id=resource['id'],
                                        queue=queue,
                                        cache_filepath=cache_filepath)


def notify_package(package, queue):
    '''
    Broadcasts an IPipe notification that a package archival has taken place
    (or at least the archival object is changed somehow). e.g.
    ckanext-packagezip listens for this
    '''
    archiver_interfaces.IPipe.send_data('package-archived',
                                        package_id=package['id'],
                                        queue=queue)


def get_plugins_waiting_on_ipipe():
    return [observer.name for observer in
            p.PluginImplementations(archiver_interfaces.IPipe)]


def verify_https():
    from pylons import config
    return toolkit.asbool(config.get('ckanext-archiver.verify_https', True))


def _clean_content_type(ct):
    # For now we should remove the charset from the content type and
    # handle it better, differently, later on.
    if 'charset' in ct:
        return ct[:ct.index(';')]
    return ct


def _set_user_agent_string(headers):
    '''
    Update the passed headers object with a `User-Agent` key, if there is a
    USER_AGENT_STRING option in settings.
    '''
    from ckanext.archiver import default_settings as settings
    ua_str = settings.USER_AGENT_STRING
    if ua_str is not None:
        headers['User-Agent'] = ua_str
    return headers


def tidy_url(url):
    '''
    Given a URL it does various checks before returning a tidied version
    suitable for calling.

    It may raise LinkInvalidError if the URL has a problem.
    '''

    # Find out if it has unicode characters, and if it does, quote them
    # so we are left with an ascii string
    try:
        url = url.decode('ascii')
    except:
        parts = list(urlparse.urlparse(url))
        parts[2] = urllib.quote(parts[2].encode('utf-8'))
        parts[1] = urllib.quote(parts[1].encode('utf-8'))
        url = urlparse.urlunparse(parts)
    url = str(url)

    # strip whitespace from url
    # (browsers appear to do this)
    url = url.strip()

    # Use urllib3 to parse the url ahead of time, since that is what
    # requests uses, but when it does it during a GET, errors are not
    # caught well
    try:
        parsed_url = urllib3.util.parse_url(url)
    except urllib3.exceptions.LocationParseError, e:
        raise LinkInvalidError(_('URL parsing failure: %s') % e)

    # Check we aren't using any schemes we shouldn't be.
    # Scheme is case-insensitive.
    if not parsed_url.scheme or not parsed_url.scheme.lower() in ALLOWED_SCHEMES:
        raise LinkInvalidError(_('Invalid url scheme. Please use one of: %s') %
                               ' '.join(ALLOWED_SCHEMES))

    if not parsed_url.host:
        raise LinkInvalidError(_('URL parsing failure - did not find a host name'))

    return url


def _save_resource(resource, response, max_file_size, chunk_size=1024*16):
    """
    Write the response content to disk.

    Returns a tuple:

        (file length: int, content hash: string, saved file path: string)
    """
    resource_hash = hashlib.sha1()
    length = 0

    fd, tmp_resource_file_path = tempfile.mkstemp()

    with open(tmp_resource_file_path, 'wb') as fp:
        for chunk in response.iter_content(chunk_size=chunk_size,
                                           decode_unicode=False):
            fp.write(chunk)
            length += len(chunk)
            resource_hash.update(chunk)

            if length >= max_file_size:
                raise ChooseNotToDownload(
                    _("Content-length %s exceeds maximum allowed value %s") %
                    (length, max_file_size))

    os.close(fd)

    content_hash = unicode(resource_hash.hexdigest())
    return length, content_hash, tmp_resource_file_path


def save_archival(resource, status_id, reason, url_redirected_to,
                  download_result, archive_result, log):
    '''Writes to the archival table the result of an attempt to download
    the resource.

    May propagate a CkanError.
    '''
    now = datetime.datetime.now()

    from ckanext.archiver.model import Archival, Status
    from ckan import model

    archival = Archival.get_for_resource(resource['id'])
    first_archival = not archival
    previous_archival_was_broken = None
    if not archival:
        archival = Archival.create(resource['id'])
        model.Session.add(archival)
    else:
        log.info('Archival from before: %r', archival)
        previous_archival_was_broken = archival.is_broken

    revision = model.Session.query(model.Revision).get(resource['revision_id'])
    archival.resource_timestamp = revision.timestamp

    # Details of the latest archival attempt
    archival.status_id = status_id
    archival.is_broken = Status.is_status_broken(status_id)
    archival.reason = reason
    archival.url_redirected_to = url_redirected_to

    # Details of successful archival
    if archival.is_broken is False:
        archival.cache_filepath = archive_result['cache_filepath']
        archival.cache_url = archive_result['cache_url']
        archival.size = download_result['size']
        archival.mimetype = download_result['mimetype']
        archival.hash = download_result['hash']
        archival.etag = download_result['headers'].get('etag')
        archival.last_modified = download_result['headers'].get('last-modified')

    # History
    if archival.is_broken is False:
        archival.last_success = now
        archival.first_failure = None
        archival.failure_count = 0
    else:
        log.info('First_archival=%r Previous_broken=%r Failure_count=%r' %
                 (first_archival, previous_archival_was_broken,
                  archival.failure_count))
        if first_archival or previous_archival_was_broken is False:
            # i.e. this is the first failure (or the first archival)
            archival.first_failure = now
            archival.failure_count = 1
        else:
            archival.failure_count += 1

    archival.updated = now
    log.info('Archival saved: %r', archival)
    model.repo.commit_and_remove()


def requests_wrapper(log, func, resource_blacklist, *args, **kwargs):
    '''
    Run a requests command, catching exceptions and reraising them as
    DownloadException. Status errors, such as 404 or 500 do not cause
    exceptions, instead exposed as not response.ok.
    e.g.
    >>> requests_wrapper(log, requests.get, url, timeout=url_timeout)
    runs:
        res = requests.get(url, timeout=url_timeout)
    '''
    from requests_ssl import SSLv3Adapter

    #catch FTP error
    if 'ftp://' in resource_blacklist.url:
            try:
                # TODO break it up and code for each type of exceptions
                from urlparse import urlparse
                import ftplib
                o = urlparse(self.url)

                ftp_timeout = 20 # in seconds
                ftp_server = o.hostname
                ftp_user = 'anonymous' if not o.username else o.username
                ftp_pass = 'anonymous@' if not o.username else o.password
                ftp_port = 21 if not o.port else o.port
                ftp_filepath = o.path

                ftp = ftplib.FTP()
                ftp.connect(ftp_server, port=ftp_port, timeout=ftp_timeout)
                ftp.login(ftp_user, ftp_pass)
                if ftp_filepath.strip('/'):
                    assert ftp.nlst(ftp_filepath)
                resource_blacklist.status_code = 200
                return {'mimetype': None,
                        'size': 100,
                        'hash': None,
                        'headers': None,
                        'saved_file': None,
                        'url_redirected_to': None,
                        'request_type': 'GET'}
            except Exception, e:
                resource_blacklist.status_code = 408
                raise DownloadException(_('FTP download error'))
    try:
        try:
            response = func(*args, **kwargs)
        except requests.exceptions.ConnectionError, e:
            if 'SSL23_GET_SERVER_HELLO' not in str(e):
                raise
            log.info('SSLv23 failed so trying again using SSLv3: %r', args)
            requests_session = requests.Session()
            requests_session.mount('https://', SSLv3Adapter())
            func = {requests.get: requests_session.get,
                    requests.post: requests_session.post}[func]
            response = func(*args, **kwargs)

    except requests.exceptions.ConnectionError, e:
        resource_blacklist.add_url_blacklist(e.args[0].reason.errno)
        raise DownloadException(_('Connection error: %s') % e)
    except requests.exceptions.HTTPError, e:
        raise DownloadException(_('Invalid HTTP response: %s') % e)
    except requests.exceptions.Timeout, e:
        resource_blacklist.add_url_blacklist(100)
        raise DownloadException(_('Connection timed out after %ss') % kwargs.get('timeout', '?'))
    except requests.exceptions.TooManyRedirects, e:
        raise DownloadException(_('Too many redirects'))
    except requests.exceptions.RequestException, e:
        raise DownloadException(_('Error downloading: %s') % e)
    except Exception, e:
        if os.environ.get('DEBUG'):
            raise
        raise DownloadException(_('Error with the download: %s') % e)
    return response


def ogc_request(context, resource, service, wms_version):
    original_url = url = resource['url']
    # Remove parameters
    url = url.split('?')[0]
    # Add WMS GetCapabilities parameters
    url += '?service=%s&request=GetCapabilities&version=%s' % \
           (service, wms_version)
    resource['url'] = url
    # Make the request
    response = download(context, resource)
    # Restore the URL so that it doesn't get saved in the actual resource
    resource['url'] = original_url
    return response


def wms_1_3_request(context, resource):
    res = ogc_request(context, resource, 'WMS', '1.3')
    res['request_type'] = 'WMS 1.3'
    return res


def wms_1_1_1_request(context, resource):
    res = ogc_request(context, resource, 'WMS', '1.1.1')
    res['request_type'] = 'WMS 1.1.1'
    return res


def wfs_request(context, resource):
    res = ogc_request(context, resource, 'WFS', '2.0')
    res['request_type'] = 'WFS 2.0'
    return res


def api_request(context, resource):
    '''
    Tries making requests as if the resource is a well-known sort of API to try
    and get a valid response. If it does it returns the response, otherwise
    Archives the response and stores what sort of request elicited it.
    '''
    log = update_resource.get_logger()
    # 'resource' holds the results of the download and will get saved. Only if
    # an API request is successful do we want to save the details of it.
    # However download() gets altered for these API requests. So only give
    # download() a copy of 'resource'.
    for api_request_func in wms_1_3_request, wms_1_1_1_request, wfs_request:
        resource_copy = copy.deepcopy(resource)
        try:
            download_dict = api_request_func(context, resource_copy)
        except ArchiverError, e:
            log.info('API %s error: %r, %r "%s"', api_request_func,
                     e, e.args, resource.get('url'))
            continue
        except Exception, e:
            if os.environ.get('DEBUG'):
                raise
            log.error('Uncaught API %s failure: %r, %r', api_request_func,
                      e, e.args)
            continue

        return download_dict


def is_id(id_string):
    '''Tells the client if the string looks like a revision id or not'''
    reg_ex = '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(reg_ex, id_string))


def response_is_an_api_error(response_body):
    '''Some APIs return errors as the response body, but HTTP status 200. So we
    need to check response bodies for these error messages.
    '''
    response_sample = response_body[:250]  # to allow for <?xml> and <!DOCTYPE> lines

    # WMS spec
    # e.g. https://map.bgs.ac.uk/ArcGIS/services/BGS_Detailed_Geology/MapServer/WMSServer?service=abc
    # <?xml version="1.0" encoding="UTF-8" standalone="yes" ?>
    # <ServiceExceptionReport version="1.3.0"
    if '<ServiceExceptionReport' in response_sample:
        return True

    # This appears to be an alternative - I can't find the spec.
    # e.g. http://sedsh13.sedsh.gov.uk/ArcGIS/services/HS/Historic_Scotland/MapServer/WFSServer?service=abc
    # <ows:ExceptionReport version='1.1.0' language='en' xmlns:ows='http://www.opengis.net/ows'><ows:Exception exceptionCode='NoApplicableCode'><ows:ExceptionText>Wrong service type.</ows:ExceptionText></ows:Exception></ows:ExceptionReport>
    if '<ows:ExceptionReport' in response_sample:
        return True


@celery.task(name="archiver.clean")
def clean():
    """
    Remove all archived resources.
    """
    log = clean.get_logger()
    log.error("clean task not implemented yet")


@celery.task(name="archiver.link_checker")
def link_checker(context, data):
    """
    Check that the resource's url is valid, and accepts a HEAD request.

    Redirects are not followed - they simple return 'location' in the headers.

    data is a JSON dict describing the link:
        { 'url': url,
          'url_timeout': url_timeout }

    Raises LinkInvalidError if the URL is invalid
    Raises LinkHeadRequestError if HEAD request fails
    Raises LinkHeadMethodNotSupported if server says HEAD is not supported

    Returns a json dict of the headers of the request
    """
    log = update_resource.get_logger()
    data = json.loads(data)
    url_timeout = data.get('url_timeout', 30)

    error_message = ''
    headers = {'User-Agent': USER_AGENT}

    url = tidy_url(data['url'])

    # Send a head request
    try:
        res = requests.head(url, timeout=url_timeout)
        headers = res.headers
    except httplib.InvalidURL, ve:
        log.error("Could not make a head request to %r, error is: %s. Package is: %r. This sometimes happens when using an old version of requests on a URL which issues a 301 redirect. Version=%s", url, ve, data.get('package'), requests.__version__)
        raise LinkHeadRequestError(_("Invalid URL or Redirect Link"))
    except ValueError, ve:
        log.error("Could not make a head request to %r, error is: %s. Package is: %r.", url, ve, data.get('package'))
        raise LinkHeadRequestError(_("Could not make HEAD request"))
    except requests.exceptions.ConnectionError, e:
        raise LinkHeadRequestError(_('Connection error: %s') % e)
    except requests.exceptions.HTTPError, e:
        raise LinkHeadRequestError(_('Invalid HTTP response: %s') % e)
    except requests.exceptions.Timeout, e:
        raise LinkHeadRequestError(_('Connection timed out after %ss') % url_timeout)
    except requests.exceptions.TooManyRedirects, e:
        raise LinkHeadRequestError(_('Too many redirects'))
    except requests.exceptions.RequestException, e:
        raise LinkHeadRequestError(_('Error during request: %s') % e)
    except Exception, e:
        raise LinkHeadRequestError(_('Error with the request: %s') % e)
    else:
        if res.status_code == 405:
            # this suggests a GET request may be ok, so proceed to that
            # in the download
            raise LinkHeadMethodNotSupported()
        if not res.ok or res.status_code >= 400:
            error_message = _('Server returned HTTP error status: %s %s') % \
                (res.status_code, res.reason)
            raise LinkHeadRequestError(error_message)
    return json.dumps(dict(headers))

URL_REGEX = re.compile(
    r'^(?:http|ftp)s?://'   # http:// or https:// or ftp:// or ftps://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

class RemoteResource(object):
    # list of errors we want to avoid wasting time on.
    ERRNO_BLOCK = {
        101: 'Network is unreachable',
    }
    BLOCK_THRESHOLD = 5 
    def __init__(self, url):
        self.url = url.strip()
        self.status_code = 0
        self.reason = ''
        self.method = None
        self.content_type = None
        self.headers = {'User-Agent': 'Data.Gov Broken Link Checker'}

    def get_content_type(self):
        if not self.valid_url():
            self.status_code = 400
            self.reason = 'Invalid URL'
            self.content_type = ''
            return self.content_type

        if 'ftp://' in self.url:
            try:
                # TODO break it up and code for each type of exceptions
                from urlparse import urlparse
                import ftplib
                o = urlparse(self.url)

                ftp_timeout = 20 # in seconds
                ftp_server = o.hostname
                ftp_user = 'anonymous' if not o.username else o.username
                ftp_pass = 'anonymous@' if not o.username else o.password
                ftp_port = 21 if not o.port else o.port
                ftp_filepath = o.path

                ftp = ftplib.FTP()
                ftp.connect(ftp_server, port=ftp_port, timeout=ftp_timeout)
                ftp.login(ftp_user, ftp_pass)
                if ftp_filepath.strip('/'):
                    assert ftp.nlst(ftp_filepath)
                self.status_code = 200
                return
            except Exception, e:
                self.status_code = 408
                return

        blacklist_errno = self.check_url_blacklist(self.url)
        if blacklist_errno:
            self.status_code = 500
            self.reason = self.ERRNO_BLOCK.get(blacklist_errno)
            log.error('get_content_type blacklisted ( %s ): %s: %s ' % (
                    self.url, self.status_code, self.reason))
            return None

        try:
            # http://docs.python-requests.org/en/latest/api/
            method = 'HEAD'
            r = requests.head(self.url, verify=False, timeout=20.0, allow_redirects=True, headers=self.headers)

            if r.status_code > 399 or r.headers.get('content-type') is None:
                method = 'GET'
                r = requests.get(self.url, verify=False, timeout=20.0, allow_redirects=True, stream=True,
                                 headers=self.headers)
                r.raw.read(50)
                if r.status_code > 399 or r.headers.get('content-type') is None:
                    self.status_code = r.status_code
                    self.reason = r.reason
                    self.method = method
                    self.content_type = None
                    return self.content_type

            content_type = r.headers.get('content-type')
            self.content_type = content_type.split(';', 1)[0]
            self.status_code = r.status_code
            self.reason = r.reason
            self.method = method
            self.delete_url_blacklist(self.url)
            return self.content_type

        except Exception as ex:
            self.status_code = 500
            self.reason = ex.__class__.__name__
            log.error('get_content_type exception ( %s ): %s ' % (self.url, ex))
            errno = ex.args[0].reason.errno
            if errno in self.ERRNO_BLOCK.keys():
                self.add_url_blacklist(self.url, errno)

            return None

    def valid_url(self):
        return URL_REGEX.match(self.url)

    def get_error_code(self):
        if self.status_code < 400:
            return 0
        return self.status_code

    def add_url_blacklist(self, errno):
        url_paths = self.get_url_paths(self.url)
        url_paths.reverse() # so we deal with closest parent first
        sql_SELECT = '''
                SELECT path
                FROM resource_domain_blacklist
                WHERE path = :path;
        '''
        sql_UPDATE = '''
                UPDATE resource_domain_blacklist
                SET count = count +1
                WHERE path = :path;
        '''
        sql_INSERT = '''
                INSERT INTO resource_domain_blacklist(path, count, errno)
                VALUES (:path, 1, :errno);
        '''
        for path in url_paths:
            q = model.Session.execute(sql_SELECT, {'path': path})
            rowcount = q.rowcount
            if rowcount:
                model.Session.execute(sql_UPDATE, {'path': path})
                model.Session.commit()
                break # only update closest existing parent
            else:
                model.Session.execute(sql_INSERT, {
                        'path': path,
                        'errno':errno, # todo deal with multiple errno
                })
                model.Session.commit()

    def delete_url_blacklist(self):
        url_paths = self.get_url_paths(self.url)
        sql_DELETE = '''
                DELETE FROM resource_domain_blacklist
                WHERE path = :path;
        '''
        for path in url_paths:
            q = model.Session.execute(sql_DELETE, {'path': path})
            model.Session.commit()

    @staticmethod
    def clear_url_blacklist():
        sql_CLEAR = '''
                DELETE FROM resource_domain_blacklist;
        '''
        q = model.Session.execute(sql_CLEAR)
        model.Session.commit()

    def check_url_blacklist(self):
        errno = None
        url_paths = self.get_url_paths(self.url)
        sql_CHECK = '''
                SELECT errno
                FROM resource_domain_blacklist
                WHERE path = :path
                AND count >= :count
                LIMIT 1;
        '''
        for path in url_paths:
            q = model.Session.execute(sql_CHECK, {
                    'path': path,
                    'count': self.BLOCK_THRESHOLD,
            })
            if q.rowcount:
                errno = q.fetchone()[0]
                break

        return errno

    def get_url_paths(self, url):
        from urlparse import urlparse
        url_paths = []

        o = urlparse(url)
        if not o.scheme and not o.netloc:
            return url_paths

        current_path = o.scheme + '://' + o.netloc.lower()
        url_paths.append(current_path)
        paths = o.path.split('/')
        for path in paths:
            if path:
                current_path = current_path + '/' + path
                url_paths.append(current_path)

        return url_paths
