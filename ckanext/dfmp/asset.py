import json, logging
from copy import deepcopy
from datetime import datetime
import ckan.plugins.toolkit as toolkit
import ckan.model as model
import ckan.logic as logic

import ckanext.dfmp.dfmp_solr as solr
from ckanext.dfmp.bonus import (
  _get_license_name, _make_uuid, _site_url, _asset_name_from_url, _sanitize
)

session = model.Session
log = logging.getLogger(__name__)

DEF_LIMIT = 21
DEF_FIELDS = '_id, CAST("assetID" AS TEXT), CAST(url AS TEXT), CAST("lastModified" AS TEXT), metadata, name, spatial'


class AssetAbsentFieldsException(toolkit.ValidationError):
  pass
class AssetNotFound(toolkit.ObjectNotFound):
  def __init__(self, asset_id):
    self.extra_msg = 'Asset {asset_id} not found'.format(asset_id=asset_id)

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

  @staticmethod
  def update(data, **additional):
    # make convinient variables
    asset_id = data.get('asset_id')
    resource_id =data['resource_id']
    context = additional.get('context')
    defer_commit = additional.get('defer_commit', False)
    logic.check_access('resource_update', context, {'id':resource_id})
    #  get asset from datastore
    search = toolkit.get_action('datastore_search')(context, {
      'resource_id': resource_id,
      'filters':{'assetID':asset_id},
    })
    records, fields = search['records'], search['fields']
    #  if asset not exists
    if not records:
      raise AssetNotFound(asset_id)
    # fields available to change
    new_list = filter(
      lambda x:
        x in (
            'lastModified'
            'name'
            # 'url'
            'spatial'
            'metadata'
      ), data
    )
    #  get rid of other fields
    new_data = dict( [ (x, data[x]) for x in new_list ])
    #  add asset_id to updated fields
    new_data.update(assetID = asset_id)
    #  if added metadata, change its type to the one defined in datastore table
    for field in fields:
      if field['id'] in ('spatial', 'metadata'):
        if  field['type'] == 'text' and type(new_data.get(field['id'])) == dict:
          new_data[field['id']] = json.dumps(new_data[field['id']])
        elif  field['type'] == 'json' and type(new_data.get(field['id'])) in (str, unicode):
          new_data[field['id']] = json.loads(new_data[field['id']])
    #  make changes in asset dict
    records[0].update(new_data)
    #  delete old record
    Asset.delete({'resource_id': resource_id, 'assetID': asset_id}, context)
    #  create updated record
    updated = Asset(records[0], 'update', context=context, parent_id=resource_id, defer_commit=defer_commit)
    package_id = session.query(model.Resource).filter_by(id=resource_id).one().get_package_id()

    #  add activity to dataset
    activity_dict = {
      'user_id': context.get('user'),
      'object_id': package_id,
      'activity_type': 'changed package',
    }
    package = session.query(model.Package).get(package_id).as_dict()
    del package['resources']
    activity_dict['data'] = {'package': package}
    toolkit.get_action('activity_create')(context, activity_dict)

    return updated.data

  @staticmethod
  def delete(data, context=None):
    try:
      # delete from datastore 
      toolkit.get_action('datastore_delete')(context,{
        'resource_id': data['resource_id'],
        'force': True,
        'filters':{
          'assetID':data['assetID'],
        }
      })
      # delete from solr index
      solr.DFMPSolr.remove_dict({
        'id' : data['resource_id'],
        'assetID' : data['assetID']
      }, defer_commit=True)
      return True

    except toolkit.ObjectNotFound:
      return False


  def __init__(self, data = None, source='user',  **additional):
    if data:
      # to prevent data mofifying
      local_data = deepcopy(data)
      # asset source route
      self.data = {
        'user': self._asset_from_user,
        'update': self._add_new_asset,
      }[source](local_data, additional)

    else:
      self.data = None

  # standart asset adding
  def _asset_from_user(self, data, additional):
    # check for image
    for field in ('url',):
        if not field in data:
          raise AssetAbsentFieldsException(field)

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
    # get resource if exists
    else:
      parent = toolkit.get_action('resource_show')(additional['context'], {'id': additional['resources'][0].id})

    parent_id = parent['id']
    # create datastore if not exists
    if not parent.get('datastore_active'):
      _default_datastore_create(additional['context'], parent_id)
    additional['parent_id'] = parent_id
    # add asset to datastore
    return self._add_new_asset(data, additional)

  def _add_new_asset(self, data, additional):
    # get name/ convert url to name
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


    if not 'source' in data:
      data['source'] = 'user'

    #set license name and id
    if 'license' in data:
      data['license_id'] = data['license']
      data['license_name'] = _get_license_name(data['license'])
      del data['license']
    metadata = data.get('metadata', deepcopy(data))
    #  collect all previous data
    asset = {
      'assetID':data.get('assetID') or _make_uuid(),
      'lastModified':data['lastModified'],
      'name':data['name'],
      'url':data['url'],
      'spatial':data['spatial'],
      'metadata':data['metadata'],
    }
    # add record to datastore
    datastore_item = toolkit.get_action('datastore_upsert')(additional['context'], {
      'resource_id': additional['parent_id'],
      'force': True,
      'records':[asset],
      'method':'insert'
    })

    result = datastore_item['records'][0]
    # convert record's fields if they are serialized json
    for field in ('metadata', 'spatial'):
      if type( result[field] ) == tuple:
        result[field] = json.loads(result[field][0])
    # update solr index
    dict_to_solr = deepcopy(result)
    dict_to_solr.update(id = additional['parent_id'])

    _asset_to_solr(dict_to_solr, defer_commit = additional.get('defer_commit', True))

    result['parent_id'] = additional['parent_id']
    return result