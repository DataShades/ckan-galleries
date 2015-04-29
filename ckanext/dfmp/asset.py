from copy import deepcopy
from datetime import datetime
import ckan.plugins.toolkit as toolkit

import json
import ckanext.dfmp.dfmp_solr as solr
from ckanext.dfmp.bonus import (
  _get_license_name, _make_uuid, _site_url, _asset_name_from_url, _sanitize
)

DEF_LIMIT = 21
DEF_FIELDS = '_id, CAST("assetID" AS TEXT), CAST(url AS TEXT), CAST("lastModified" AS TEXT), metadata, name, spatial'


class AssetAbsentFieldsException(Exception):
  pass

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

class Asset:

  @staticmethod
  def get(id, assetID): pass

  @staticmethod
  def get_all(id): pass



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