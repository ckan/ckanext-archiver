from builtins import object
import ckanext.archiver.model as archiver_model
from ckan.tests import factories as ckan_factories
from ckan import model
import pytest

Archival = archiver_model.Archival


class TestArchival(object):

    @pytest.fixture(autouse=True)
    @pytest.mark.usefixtures(u"clean_db")
    def initial_data(self, clean_db):
        archiver_model.init_tables(model.meta.engine)

    def test_create(self):
        dataset = ckan_factories.Dataset()
        res = ckan_factories.Resource(package_id=dataset['id'])
        archival = Archival.create(res['id'])
        assert isinstance(archival, Archival)
        assert archival.package_id == dataset['id']
