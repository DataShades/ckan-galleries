# CKAN Galleries

A package of extensions for the [CKAN open data platform](http://ckan.org/) for storing and referencing image and video assets.

## Requirements

This extension was developed and tested under CKAN-

## Installation

To install CKAN Galleries:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the ckanext-dfmp Python package into your virtual environment::

     $ python setup.py install

3. Add ``dfmp`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     $ sudo service apache2 reload

## Config Settings

This extension doesn't define any additional config settings yet


## Development Installation

To install CKAN Galleries for development, activate your CKAN virtualenv and
do:
     $ python setup.py develop
     
All dependencies will be installed automatically 

## Running the Tests

To run the tests, do:

Download selenium server from http://www.seleniumhq.org/download/ . Start selenium by:
     $ java -jar selenium.jar
where selenium.jar - downloaded file

Activate your virtual environment and use command below to start tests:
     $ nosetests  --with-pylons=/etc/ckan/default/development.ini
where /etc/ckan/default/development.ini - path to ckan config file
If you don't want to perform browser testing or have some troubles with selenium then just add one additional param
     $ nosetests  --with-pylons=/etc/ckan/default/development.ini -e browser
in order to exclude browser testing

## Copying and License

This material is copyright &copy; 2015 Link Web Services Pty Ltd

It is open and licensed under the GNU Affero General Public License (AGPL) v3.0 whose full text may be found at:

[http://www.fsf.org/licensing/licenses/agpl-3.0.html](http://www.fsf.org/licensing/licenses/agpl-3.0.html)
