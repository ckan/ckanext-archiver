import pytest

from ckan import plugins
from ckan.tests import factories
import ckan.tests.helpers as helpers

from ckanext.archiver.tasks import update_package


@pytest.mark.usefixtures("with_plugins", "clean_db")
@pytest.mark.ckan_config(
    "ckanext-archiver.cache_url_root", "http://localhost:50001/resources/"
)
@pytest.mark.ckan_config("ckanext-archiver.max_content_length", 1000000)
@pytest.mark.ckan_config("ckan.plugins", "archiver testipipe")
class TestApi(object):
    def test_package_show(self, client: str):
        url = client + "/?status=200&content=test&content-type=csv"
        testipipe = plugins.get_plugin("testipipe")
        testipipe.reset()

        pkg_dict = {
            "name": "test-package-api",
            "resources": [{"url": url, "format": "TXT", "description": "Test"}],
        }
        pkg = factories.Dataset(**pkg_dict)
        update_package(pkg["id"])

        result = helpers.call_action("package_show", id=pkg["id"])
        assert "archiver" in result.keys()
