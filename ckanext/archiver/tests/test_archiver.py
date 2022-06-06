from __future__ import print_function
import logging
import os
import shutil
import tempfile
import json

from future.moves.urllib.parse import quote_plus
from ckan.plugins.toolkit import config
import pytest

from ckan import model
from ckan import plugins
from ckan.logic import get_action
from ckan.tests import factories as ckan_factories

from ckanext.archiver import model as archiver_model
from ckanext.archiver.model import Archival


from ckanext.archiver.tasks import (link_checker,
                                    update_resource,
                                    update_package,
                                    download,
                                    api_request,
                                    LinkCheckerError,
                                    LinkInvalidError,
                                    response_is_an_api_error
                                    )


# enable celery logging for when you run nosetests -s
log = logging.getLogger('ckanext.archiver.tasks')


def get_logger():
    return log


update_resource.get_logger = get_logger
update_package.get_logger = get_logger


class TestLinkChecker:
    """
    Tests for link checker task
    """

    @pytest.fixture(autouse=True)
    @pytest.mark.usefixtures(u"clean_db")
    @pytest.mark.ckan_config("ckan.plugins", "archiver")
    def initial_data(self, clean_db):
        return {}

    def test_file_url(self):
        url = u'file:///home/root/test.txt'  # schema not allowed
        context = json.dumps({})
        data = json.dumps({'url': url})
        with pytest.raises(LinkInvalidError):
            link_checker(context, data)

    def test_bad_url(self):
        url = u'http:www.buckshealthcare.nhs.uk/freedom-of-information.htm'
        context = json.dumps({})
        data = json.dumps({'url': url})
        with pytest.raises(LinkInvalidError):
            link_checker(context, data)

    def test_non_escaped_url(self, client):
        url = client + '/+/http://www.homeoffice.gov.uk/publications/science-research-statistics/research-statistics/' \
              + 'drugs-alcohol-research/hosb1310/hosb1310-ann2tabs?view=Binary'
        context = json.dumps({})
        data = json.dumps({'url': url})
        res = link_checker(context, data)
        assert res

    def test_empty_url(self):
        url = u''
        context = json.dumps({})
        data = json.dumps({'url': url})
        with pytest.raises(LinkCheckerError):
            link_checker(context, data)

    def test_url_with_503(self, client):
        url = client + '/?status=503'
        context = json.dumps({})
        data = json.dumps({'url': url})
        with pytest.raises(LinkCheckerError):
            link_checker(context, data)

    def test_url_with_404(self, client):
        url = client + 'http://localhost:9091/?status=404'
        context = json.dumps({})
        data = json.dumps({'url': url})
        with pytest.raises(LinkCheckerError):
            link_checker(context, data)

    def test_url_with_405(self, client):  # 405: method (HEAD) not allowed
        url = client + '/?status=405'
        context = json.dumps({})
        data = json.dumps({'url': url})
        with pytest.raises(LinkCheckerError):
            link_checker(context, data)

    def test_url_with_30x_follows_redirect(self, client):
        redirect_url = client + u'/?status=200&content=test&content-type=text/csv'
        url = client + u'/?status=301&location=%s' % quote_plus(redirect_url)
        context = json.dumps({})
        data = json.dumps({'url': url})
        result = json.loads(link_checker(context, data))
        assert result

    # e.g. "http://www.dasa.mod.uk/applications/newWeb/www/index.php?page=48&thiscontent=180&date=2011-05-26
    # &pubType=1&PublishTime=09:30:00&from=home&tabOption=1"
    def test_colon_in_query_string(self, client):
        # accept, because browsers accept this
        # see discussion: http://trac.ckan.org/ticket/318

        url = client + '/?time=09:30&status=200'
        context = json.dumps({})
        data = json.dumps({'url': url})
        result = json.loads(link_checker(context, data))
        assert result

    def test_trailing_whitespace(self, client):
        # accept, because browsers accept this
        url = client + '/?status=200 '
        context = json.dumps({})
        data = json.dumps({'url': url})
        result = json.loads(link_checker(context, data))
        assert result

    def test_good_url(self, client):
        context = json.dumps({})
        url = client + "/?status=200"
        data = json.dumps({'url': url})
        result = json.loads(link_checker(context, data))
        assert result


@pytest.mark.usefixtures('with_plugins')
@pytest.mark.ckan_config("ckanext-archiver.cache_url_root", "http://localhost:50001/resources/")
@pytest.mark.ckan_config("ckanext-archiver.max_content_length", 1000000)
@pytest.mark.ckan_config("ckan.plugins", "testipipe")
class TestArchiver:
    """
    Tests for Archiver 'update_resource'/'update_package' tasks
    """

    @pytest.fixture(autouse=True)
    @pytest.mark.usefixtures(u"clean_db")
    def initial_data(cls, clean_db):
        archiver_model.init_tables(model.meta.engine)
        cls.temp_dir = tempfile.mkdtemp()

    def _test_package(self, url, format=None):
        pkg = {'resources': [
            {'url': url, 'format': format or 'TXT', 'description': 'Test'}
            ]}
        pkg = ckan_factories.Dataset(**pkg)
        return pkg

    def _test_resource(self, url, format=None):
        pkg = self._test_package(url, format)
        return pkg['resources'][0]

    def assert_archival_error(self, error_message_fragment, resource_id):
        archival = Archival.get_for_resource(resource_id)
        if error_message_fragment not in archival.reason:
            print('ERROR: %s (%s)' % (archival.reason, archival.status))
            raise AssertionError(archival.reason)

    def test_file_url(self):
        res_id = self._test_resource('file:///home/root/test.txt')['id']  # scheme not allowed
        result = update_resource(res_id)
        assert not result, result
        self.assert_archival_error('Invalid url scheme', res_id)

    def test_bad_url(self):
        res_id = self._test_resource('http:host.com')['id']  # no slashes
        result = update_resource(res_id)
        assert not result, result
        self.assert_archival_error('URL parsing failure', res_id)

    def test_resource_hash_and_content_length(self, client):
        url = client + '/?status=200&content=test&content-type=csv'
        res_id = self._test_resource(url)['id']
        result = json.loads(update_resource(res_id))
        assert result['size'] == len('test')
        from hashlib import sha1
        assert result['hash'] == sha1('test'.encode('utf-8')).hexdigest(), result
        _remove_archived_file(result.get('cache_filepath'))

    def test_archived_file(self, client):
        url = client + '/?status=200&content=test&content-type=csv'
        res_id = self._test_resource(url)['id']
        result = json.loads(update_resource(res_id))

        assert result['cache_filepath']
        assert os.path.exists(result['cache_filepath'])

        with open(result['cache_filepath']) as f:
            content = f.readlines()
            assert len(content) == 1
            assert content[0] == "test"

        _remove_archived_file(result.get('cache_filepath'))

    def test_update_url_with_unknown_content_type(self, client):
        url = client + '/?content-type=application/foo&content=test'
        res_id = self._test_resource(url, format='foo')['id']  # format has no effect
        result = json.loads(update_resource(res_id))
        assert result, result
        assert result['mimetype'] == 'application/foo'  # stored from the header

    def test_wms_1_3(self, client):
        url = client + '/WMS_1_3/'
        res_id = self._test_resource(url)['id']
        result = json.loads(update_resource(res_id))
        assert result, result
        assert result['request_type'] == 'WMS 1.3'

        with open(result['cache_filepath']) as f:
            content = f.read()
            assert '<WMT_MS_Capabilities' in content, content[:1000]
        _remove_archived_file(result.get('cache_filepath'))

    def test_update_with_zero_length(self, client):
        url = client + '/?status=200&content-type=csv'
        # i.e. no content
        res_id = self._test_resource(url)['id']
        result = update_resource(res_id)
        assert not result, result
        self.assert_archival_error('Content-length after streaming was 0', res_id)

    def test_file_not_found(self, client):
        url = client + '/?status=404&content=test&content-type=csv'
        res_id = self._test_resource(url)['id']
        result = update_resource(res_id)
        assert not result, result
        self.assert_archival_error('Server reported status error: 404 NOT FOUND', res_id)

    def test_server_error(self, client):
        url = client + '/?status=500&content=test&content-type=csv'
        res_id = self._test_resource(url)['id']
        result = update_resource(res_id)
        assert not result, result
        self.assert_archival_error('Server reported status error: 500 INTERNAL SERVER ERROR', res_id)

    def test_file_too_large_1(self, client):
        url = client + '/?status=200&content=short&length=1000001&content-type=csv'
        # will stop after receiving the header
        res_id = self._test_resource(url)['id']
        result = update_resource(res_id)
        assert not result, result
        self.assert_archival_error('Content-length 1000001 exceeds maximum allowed value 1000000', res_id)

    def test_file_too_large_2(self, client):
        url = client + '/?status=200&content_long=test_contents_greater_than_the_max_length&no-content-length&content-type=csv'
        # no size info in headers - it stops only after downloading the content
        res_id = self._test_resource(url)['id']
        result = update_resource(res_id)
        assert not result, result
        self.assert_archival_error('Content-length 1000001 exceeds maximum allowed value 1000000', res_id)

    def test_content_length_not_integer(self, client):
        url = client + '/?status=200&content=content&length=abc&content-type=csv'
        res_id = self._test_resource(url)['id']
        result = json.loads(update_resource(res_id))
        assert result, result

    def test_content_length_repeated(self, client):
        url = client + '/?status=200&content=content&repeat-length&content-type=csv'
        # listing the Content-Length header twice causes requests to
        # store the value as a comma-separated list
        res_id = self._test_resource(url)['id']
        result = json.loads(update_resource(res_id))
        assert result, result

    def test_url_with_30x_follows_and_records_redirect(self, client):
        url = client + '/'
        redirect_url = url + u'?status=200&content=test&content-type=text/csv'
        url += u'?status=301&location=%s' % quote_plus(redirect_url)
        res_id = self._test_resource(url)['id']
        result = json.loads(update_resource(res_id))
        assert result
        assert result['url_redirected_to'] == redirect_url

    def test_ipipe_notified(self, client):
        url = client + '/?status=200&content=test&content-type=csv'
        testipipe = plugins.get_plugin('testipipe')
        testipipe.reset()

        res_id = self._test_resource(url)['id']

        update_resource(res_id, 'queue1')

        assert len(testipipe.calls) == 1

        operation, queue, params = testipipe.calls[0]
        assert operation == 'archived'
        assert queue == 'queue1'
        assert params.get('package_id') is None
        assert params.get('resource_id') == res_id

    @pytest.mark.ckan_config("ckan.plugins", "archiver testipipe")
    def test_ipipe_notified_dataset(self, client):
        url = client + '/?status=200&content=test&content-type=csv'
        testipipe = plugins.get_plugin('testipipe')
        testipipe.reset()

        pkg = self._test_package(url)

        update_package(pkg['id'], 'queue1')

        assert len(testipipe.calls) == 2, len(testipipe.calls)

        operation, queue, params = testipipe.calls[0]
        assert operation == 'archived'
        assert queue == 'queue1'
        assert params.get('package_id') is None
        assert params.get('resource_id') == pkg['resources'][0]['id']

        operation, queue, params = testipipe.calls[1]
        assert operation == 'package-archived'
        assert queue == 'queue1'
        assert params.get('package_id') == pkg['id']
        assert params.get('resource_id') is None


class TestDownload:
    '''Tests of the download method (and things it calls).

    Doesn't need a fake CKAN to get/set the status of.
    '''
    @pytest.fixture(autouse=True)
    @pytest.mark.usefixtures(u"clean_index")
    def initialData(cls, clean_db):
        config
        cls.fake_context = {
            'site_url': config.get('ckan.site_url_internally') or config['ckan.site_url'],
            'cache_url_root': config.get('ckanext-archiver.cache_url_root'),
        }

    def _test_resource(self, url, format=None):
        context = {'model': model, 'ignore_auth': True, 'session': model.Session, 'user': 'test'}
        pkg = {'name': 'testpkg', 'resources': [
            {'url': url, 'format': format or 'TXT', 'description': 'Test'}
            ]}
        pkg = get_action('package_create')(context, pkg)
        return pkg['resources'][0]

    def test_head_unsupported(self, client):
        url = client + '/?status=200&method=get&content=test&content-type=csv'
        # This test was more relevant when we did HEAD requests. Now servers
        # which respond badly to HEAD requests are not an issue.
        resource = self._test_resource(url)

        # HEAD request will return a 405 error, but it will persevere
        # and do a GET request which will work.
        result = download(self.fake_context, resource)
        assert result['saved_file']

    def test_download_file(self, client):
        url = client + '/?status=200&content=test&content-type=csv'
        resource = self._test_resource(url)

        result = download(self.fake_context, resource)

        assert result['saved_file']
        assert os.path.exists(result['saved_file'])
        _remove_archived_file(result.get('saved_file'))

        # Modify the resource and check that the resource size gets updated
        resource['url'] = url.replace('content=test', 'content=test2')
        result = download(self.fake_context, resource)
        assert result['size'] == len('test2')

        _remove_archived_file(result.get('saved_file'))

    def test_wms_1_3(self, client):
        url = client + '/WMS_1_3/'
        resource = self._test_resource(url)
        result = api_request(self.fake_context, resource)

        assert result
        assert int(result['size']) > 7800, result['length']
        assert result['request_type'] == 'WMS 1.3'
        _remove_archived_file(result.get('saved_file'))

    def test_wms_1_1_1(self, client):
        url = client + '/WMS_1_1_1/'
        resource = self._test_resource(url)
        result = api_request(self.fake_context, resource)

        assert result
        assert int(result['size']) > 7800, result['length']
        assert result['request_type'] == 'WMS 1.1.1'
        _remove_archived_file(result.get('saved_file'))

    def test_wfs(self, client):
        url = client + '/WFS/'
        resource = self._test_resource(url)
        result = api_request(self.fake_context, resource)

        assert result
        assert int(result['size']) > 7800, result['length']
        assert result['request_type'] == 'WFS 2.0'
        _remove_archived_file(result.get('saved_file'))

    def test_wms_error(self, client):
        wms_error_1 = '''<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>
<ServiceExceptionReport version="1.3.0"
  xmlns="http://www.opengis.net/ogc"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.opengis.net/ogc http://schemas.opengis.net/wms/1.3.0/exceptions_1_3_0.xsd">
  <ServiceException code="InvalidFormat">
Unknown service requested.
  </ServiceException>
</ServiceExceptionReport>'''
        assert response_is_an_api_error(wms_error_1) is True
        wms_error_2 = '''<ows:ExceptionReport version='1.1.0' language='en' xmlns:ows='http://www.opengis.net/ows'>
        <ows:Exception exceptionCode='NoApplicableCode'><ows:ExceptionText>Unknown operation name.</ows:ExceptionText>
        </ows:Exception></ows:ExceptionReport>'''
        assert response_is_an_api_error(wms_error_2) is True


def _remove_archived_file(cache_filepath):
    if cache_filepath:
        if os.path.exists(cache_filepath):
            resource_folder = os.path.split(cache_filepath)[0]
            if 'fake_resource_id' in resource_folder:
                shutil.rmtree(resource_folder)
