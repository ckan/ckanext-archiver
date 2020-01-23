#!/bin/sh -e

echo "NO_START=0\nJETTY_HOST=127.0.0.1\nJETTY_PORT=8983\nJAVA_HOME=$JAVA_HOME" | sudo tee /etc/default/jetty
sudo cp ../ckan/config/solr/schema.xml /etc/solr/conf/schema.xml
sudo service jetty restart

if [ $CKANVERSION = 'master' ]
then
  pytest --ckan-ini=test-core.ini --cov=ckanext.archiver tests/
else
  nosetests --nologcapture --with-pylons=test-core.ini --with-coverage --cover-package=ckanext.archiver --cover-inclusive --cover-erase --cover-tests tests-py2
fi