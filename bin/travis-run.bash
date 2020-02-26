#!/bin/bash
set -e

if [ $CKANVERSION == 'master' ]
then
    export CKAN_MINOR_VERSION=100
else
    export CKAN_MINOR_VERSION=${CKANVERSION##*.}
fi


if (( $CKAN_MINOR_VERSION >= 9 ))
then
    pytest --ckan-ini=subdir/test.ini --cov=ckanext.archiver ckanext/archiver/tests
else
    nosetests --ckan --nologcapture --with-pylons=subdir/test-nose.ini --with-coverage --cover-package=ckanext.archiver --cover-inclusive --cover-erase --cover-tests ckanext/archiver/tests/nose
fi