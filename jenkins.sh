#! /usr/bin/env sh
# receive only two arguments
if [ $# = 2 ]; then
  # check existence of files
  if ! [ -f $1  ]; then
    echo First argement should refers to {virtualenv}/bin/activate file
    exit
  elif ! [ -f $2 ]; then
    echo Second argument should refers to existent CKAN config file
    exit
  fi
else 
  echo You should use full path to {virtualenv}/bin/activate as first argument and full path to CKAN config file as second argument
  exit 
fi
# activate virtualenv
. $1
echo Virtual environment activated

#install dependencies, etc.
# python setup.py develop

#clean db
# echo DB cleaning started..
# paster --plugin=ckan db clean -c $2
# echo DB cleaned. DB init started..
# paster --plugin=ckan db init -c $2
# echo DB initialized


default_test_admin_name='jenkins_test_admin'
default_test_admin_pass='040471'
default_test_admin_mail='jenkins_test_admin@testing.net'
default_test_admin_apikey='123123123123'

paster --plugin=ckan user add $default_test_admin_name email=$default_test_admin_mail password=$default_test_admin_pass apikey=$default_test_admin_apikey -c $2
paster --plugin=ckan sysadmin add $default_test_admin_name -c $2

nosetests --with-xunit --xunit-file=nosetests.xml -v --nologcapture --with-pylons=$2