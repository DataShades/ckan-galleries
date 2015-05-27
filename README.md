# CKAN Galleries

A package of extensions for the [CKAN open data platform](http://ckan.org/) for storing and referencing image and video assets.

## Requirements

For example, you might want to mention here which versions of CKAN this
extension works with.

## Installation

To install CKAN Galleries:

1. Activate your CKAN virtual environment, for example::

     . /usr/lib/ckan/default/bin/activate

2. Install the {{ project }} Python package into your virtual environment::

     pip install {{ project }}

3. Add ``ckan-dfmp`` to the ``ckan.plugins`` setting in your CKAN
   config file (by default the config file is located at
   ``/etc/ckan/default/production.ini``).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu::

     sudo service apache2 reload

## Config Settings

Document any optional config settings here. For example:

    # The minimum number of hours to wait before re-checking a resource
    # (optional, default: 24).
    ckanext.{{ project_shortname }}.some_setting = some_default_value


## Development Installation

To install CKAN Galleries for development, activate your CKAN virtualenv and
do:


## Running the Tests

To run the tests, do:



## Copying and License

This material is copyright &copy; 2015 Link Web Services Pty Ltd

It is open and licensed under the GNU Affero General Public License (AGPL) v3.0 whose full text may be found at:

[http://www.fsf.org/licensing/licenses/agpl-3.0.html](http://www.fsf.org/licensing/licenses/agpl-3.0.html)
