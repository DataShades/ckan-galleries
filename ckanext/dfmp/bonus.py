import ckan.plugins.toolkit as toolkit
import hashlib
from pylons import config

import ckan.model as model
session = model.Session

def _get_index_id(id, assetID):
  return hashlib.md5('%s%s' % (id + assetID, config.get('ckan.site_id'))).hexdigest()


def _validate(data, *fields):
  for field in fields:
    if not field in data:

      raise toolkit.ValidationError('Parameter {%s} must be defined' % field)

def _unjson_base(string):
  return string\
    .replace('"("{','{')\
    .replace('("{','{')\
    .replace('}","")"','}')\
    .replace('}","")','}')\
    .replace('\\\\""','\\""')

def _unjson(string):
  return _unjson_base(string).replace('""','"')

def _get_package_id_by_res(id):
  return session.query(model.Resource).filter_by(id=id).first().get_package_id()
