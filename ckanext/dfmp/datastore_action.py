import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from ckanext.dfmp.action import _validate, _unjson
import json
from ckan.lib.helpers import url_for
import ckan.model as model
from random import shuffle, sample
import logging
log = logging.getLogger(__name__)

from ckanext.dfmp.dfmp_model import DFMPAssets
from sqlalchemy import func

@side_effect_free
def resource_items(context, data_dict):
  _validate(data_dict, 'id')
  sql = "SELECT * FROM \"%s\"" % data_dict['id']
  sql_search =  toolkit.get_action('datastore_search_sql')

  item = data_dict.get('item')
  if item:
    try:
      int(item)
      where = " WHERE _id = '{0}' OR \"assetID\" = '{0}'".format(item)
    except ValueError:
      where = " WHERE \"assetID\" = '{0}'".format(item)
    resp = sql_search(context, {'sql': sql + where })
  else:
    resp = sql_search(context, {'sql': sql + " LIMIT {0} OFFSET {1}" .format( data_dict.get('limit', 99999), data_dict.get('offset', 0) ) })
  resp['records'] = filter(_filter_metadata, resp['records'])
  resource = model.Session.query(model.Resource).filter_by(id=data_dict['id']).first().get_package_id()
  resp['backlink'] = url_for(controller='package', action='resource_read', resource_id=data_dict['id'], id=resource)[1:]
  return resp

def _filter_metadata(rec):
  del rec['_full_text']
  try:
    if type( rec['metadata'] ) in (str, unicode) and rec['metadata']:
      rec['metadata'] = json.loads( _unjson(rec['metadata']) )
    if type( rec['spatial'] ) in (str, unicode) and rec['spatial']:
      rec['spatial'] = json.loads( _unjson(rec['spatial']) )
  except ValueError:
    return False
  
  return True

@side_effect_free
def dfmp_gallery(context, data_dict):
  ds = [ item['name'] for item in toolkit.get_action('datastore_search')(context, {'resource_id':'_table_metadata', 'fields':'name', 'limit':int(data_dict.get('assets_limit', '1000')) })['records' ] ]
  resources = [ str(resource[0]) for resource in model.Session.query(model.Resource.id).filter(model.Resource.state=='active',model.Resource.id.in_(ds) ).all() ]
  sql_search =  toolkit.get_action('datastore_search_sql')
  shuffle(resources)
  result = []
  # scope = resources[:int(data_dict.get('per_asset', '121'))]
  # log.warn(scope)
  # sql = "(SELECT _id, url, '{0}' as parent FROM \"{0}\" ORDER BY RANDOM() LIMIT {1})".format( scope[-2], data_dict.get('per_asset', '1') )
  # sql += " UNION (SELECT _id, url, '{0}' as parent FROM \"{0}\" ORDER BY RANDOM() LIMIT {1}) ".format( scope[-1], data_dict.get('per_asset', '1') )
  # result.extend( [ item for item in sql_search(context,{'sql': sql} )['records'] ] )

  for res in resources:
    sql = "SELECT _id, url FROM \"%s\" ORDER BY RANDOM() LIMIT %s" % (res, data_dict.get('per_asset', '1') )
    try:
      result.extend( [ {'id':res, 'assetID':item['_id'], 'url':item['url']} for item in sql_search(context,{'sql': sql} )['records'] ] )
    except toolkit.ValidationError:
      continue
    if len(result) >= int(data_dict.get('limit', '21')): break
  shuffle(result)
  return result

@side_effect_free
def cbr_gallery(context, data_dict):
  res_id = data_dict.get('res_id')
  if not res_id:
    cbr = toolkit.get_action('resource_search')(context, {'query':'description:#CBR'})
    if cbr['count'] != 1:
      raise toolkit.ValidationError('Can\'t find unique resouce or any resource at all. Please specify param {res_id}')
    res_id = cbr['results'][0]['id']
  items = [{'url':item['url'], 'id':item['_id'], 'parent':res_id} for item in toolkit.get_action('datastore_search')(context,
          {'resource_id':res_id, 'fields':'url, _id', 'sort':'_id desc', 'limit':int(data_dict.get('limit',1000)), 'offset':int(data_dict.get('offset','0')) }
        )['records'] ]
  return items

@side_effect_free
def static_gallery_reset(context, data_dict):  
  ds = [ item['name'] for item in toolkit.get_action('datastore_search')(context, {'resource_id':'_table_metadata', 'fields':'name', 'limit':int(data_dict.get('assets_limit', '1000')) })['records' ] ]
  resources = [ str(resource[0]) for resource in model.Session.query(model.Resource.id).filter(model.Resource.state=='active',model.Resource.id.in_(ds) ).all() ]
  sql_search =  toolkit.get_action('datastore_search_sql')
  result = []

  for res in resources:
    sql = "SELECT _id, url, name, metadata, \"lastModified\" FROM \"{0}\"".format(res)
    try:
      result.extend( [ DFMPAssets(parent=res, 
                                  item=item['_id'], 
                                  url=item['url'], 
                                  name=item['name'], 
                                  asset_metadata=_unjson(item['metadata']) if type( item['metadata'] ) in (str, unicode) and item['metadata'] else json.dumps(item['metadata']),
                                  lastModified=item['lastModified']
                                  ) 
                          for item in sql_search(context,{'sql': sql} )['records'] 
                      ] )
    except toolkit.ValidationError:
      continue
  model.Session.execute('TRUNCATE {0}; ALTER SEQUENCE {0}_id_seq RESTART;'.format( str(DFMPAssets.__table__) ))
  model.Session.add_all(result)
  model.Session.commit()
  return len(result)

@side_effect_free
def dfmp_static_gallery(context, data_dict):
  limit = int( data_dict.get('limit', 21) )
  total = model.Session.query(func.count('*')).select_from(DFMPAssets).scalar()
  variety = range(1, total+1 if total > limit else limit+1)
  ids = sample(variety, limit)

  result = [{'id':item.parent, 
              'assetID':item.item, 
              'url': item.url, 
              'name': item.name,
              'lastModified':item.lastModified,
              'metadata':json.loads(item.asset_metadata)} for item in model.Session.query(DFMPAssets).filter(DFMPAssets.id.in_(ids)).all() ]
  shuffle(result)
  return result

@side_effect_free
def search_item(context, data_dict):
  limit = int( data_dict.get('limit', 21) )
  offset = int( data_dict.get('offset', 0) )

  name = data_dict.get('query_string','')

  result = [{'id':item.parent, 
              'assetID':item.item, 
              'url': item.url, 
              'name': item.name,
              'lastModified':item.lastModified,
              'metadata':json.loads(item.asset_metadata)} for item in model.Session.query(DFMPAssets).filter(DFMPAssets.name.like('%{0}%'.format(name))).offset(offset).limit(limit+1).all() ]
  shuffle(result)
  has_more = False
  if len(result) > limit:
    has_more = True
    result = result[:-1]
  return {'records':result, 'limit':limit, 'offset':offset, 'has_more':has_more}