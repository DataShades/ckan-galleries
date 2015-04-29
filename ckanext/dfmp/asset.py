import json
from copy import deepcopy
from datetime import datetime

import ckan.model as model
import ckan.plugins.toolkit as toolkit
from ckan.lib.helpers import url_for

import ckanext.dfmp.dfmp_solr as solr
from ckanext.dfmp.bonus import (
  _get_license_name, _make_uuid, _site_url, _asset_name_from_url, _sanitize
)

import logging
log = logging.getLogger(__name__)

session = model.Session

DEF_FIELDS = '_id, CAST("assetID" AS TEXT), CAST(url AS TEXT), CAST("lastModified" AS TEXT), metadata, name, spatial'
LIMIT = 21
OFFSET = 0

class AssetAbsentFieldsException(Exception):
  pass

# structure of default datastore
def _default_datastore_create(context, id):
  toolkit.get_action('datastore_create')(context, {
    'resource_id':id,
    'force': True,
    'fields':[
      {'id':'assetID', 'type':'text'},
      {'id':'lastModified', 'type':'text'},
      {'id':'name', 'type':'text'},
      {'id':'url', 'type':'text'},
      {'id':'spatial', 'type':'json'},
      {'id':'metadata', 'type':'json'},
    ],
    'primary_key':['assetID'],
    'indexes':['name', 'assetID']
  })

def _asset_to_solr(dict_to_solr, defer_commit=True):
  solr.DFMPSolr.index_asset(dict_to_solr, defer_commit=defer_commit)
  return True

def _filter_metadata(rec):
  # del rec['_full_text']
  _check_datastore_json(rec, 'metadata')
  _check_datastore_json(rec, 'spatial')
  return rec

# convert json string to python dict
def _check_datastore_json(rec, field):
  try:
    if type( rec[field] ) in (str, unicode) and rec[field]:
      rec[field] = json.loads( rec[field] )
  except ValueError, e:
    try:
      rec[field] = json.loads( _unjson(rec[field]) )
    except ValueError, e:
      try:
        rec[field] = json.loads( _unjson_base(rec[field]) )
      except ValueError, e:
        log.warn(e)
        log.warn(rec)
        log.warn('Wrong {0}'.format(field))
#  get one or many assets
def _get_asset_func(id, where, context):
  # get assets
  sql = "SELECT {fields} FROM \"{table}\"".format(fields=DEF_FIELDS, table=_sanitize(id) )
  try:
    result = toolkit.get_action('datastore_search_sql')(context, {'sql': sql + where })
  except toolkit.ValidationError, e:
    # resource not exists
    raise toolkit.ObjectNotFound('Resource not found')

  # convert metadata & spatial to correct python dict
  result['records'] = map(_filter_metadata, result['records'])
  # return result
  return result
  
class Asset:

  @staticmethod
  def get(id, assetID, context=None):
    where = " WHERE \"assetID\" = '{0}' ".format( _sanitize(assetID))
    return _get_asset_func(id, where, context)


  @staticmethod
  def get_all(id, limit=LIMIT, offset=OFFSET, context=None):
    if type(limit) != int:
      limit = limit if limit.isdigit() else LIMIT
    if type(offset) !=int:
      offset = offset if offset.isdigit() else OFFSET
      
    where = ' ORDER BY "lastModified" DESC LIMIT {0} OFFSET {1}'.format(limit, offset )
    return _get_asset_func(id, where, context)



  def __init__(self, data = None, source='user',  **additional):
    if data:
      # to prevent data mofifying
      local_data = deepcopy(data)
      # asset source route
      self.data = {
        'user': self._asset_from_user,
      }[source](local_data, additional)

    else:
      self.data = None

  # standart asset adding
  def _asset_from_user(self, data, additional):
    # check for image
    for field in ('url',):
        if not field in data:
          raise AssetAbsentFieldsException(field)

    if not 'name' in data:
      data['name'] = _asset_name_from_url(data['url'])

    # uploaded time
    data['lastModified'] = datetime.now().isoformat(' ')[:19]

    # correct spatial
    if 'spatial' in data: pass
    elif 'geoLocation' in data:
      geo = data['geoLocation']
      if geo and 'lng' in geo and 'lat' in geo:
        location =  { "type": "Point", "coordinates": [
            float(data_dict['geoLocation']['lng']),
            float(data_dict['geoLocation']['lat'])
          ]}
      else:
        location = None
      data['spatial'] = location
      del data['geoLocation']
    else: data['spatial'] = None

    # create resource if not exists
    if not additional['resources']:
      new_id = _make_uuid()
      parent = toolkit.get_action('resource_create')(additional['context'], {
        'package_id':additional['package_id'],
        'id':new_id,
        'url': _site_url() + '/datastore/dump/' + new_id,
        'name':'Assets',
        'resource_type':'asset',
      })
    else:
      parent = toolkit.get_action('resource_show')(additional['context'], {'id': additional['resources'][0].id})

    parent_id = parent['id']

    if not parent.get('datastore_active'):
      _default_datastore_create(additional['context'], parent_id)

    if not 'source' in data:
      data['source'] = 'user'

    #set license name and id
    if 'license' in data:
      data['license_id'] = data['license']
      data['license_name'] = _get_license_name(data['license'])
      del data['license']

    asset = {
      'assetID':_make_uuid(),
      'lastModified':data['lastModified'],
      'name':data['name'],
      'url':data['url'],
      'spatial':data['spatial'],
      'metadata':data,
    }

    datastore_item = toolkit.get_action('datastore_upsert')(additional['context'], {
      'resource_id': parent_id,
      'force': True,
      'records':[asset],
      'method':'insert'
    })

    result = datastore_item['records'][0]

    for field in ('metadata', 'spatial'):
      if type( result[field] ) == tuple:
        result[field] = json.loads(result[field][0])

    dict_to_solr = deepcopy(result)
    dict_to_solr.update(id = parent_id)

    _asset_to_solr(dict_to_solr, defer_commit = False)

    result['parent_id'] = parent_id
    return result