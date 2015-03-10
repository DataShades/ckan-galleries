import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from ckanext.dfmp.actions.action import _validate, _unjson
import json
from ckan.lib.helpers import url_for
import ckan.model as model
from random import shuffle, sample
import logging
log = logging.getLogger(__name__)

from ckanext.dfmp.dfmp_model import DFMPAssets
from sqlalchemy import func

from ckanext.dfmp.actions.action import indexer

DEF_LIMIT = 21
DEF_FIELDS = '_id, CAST("assetID" AS TEXT), CAST(url AS TEXT), CAST("lastModified" AS TEXT), CAST(metadata  AS TEXT), name, CAST(spatial  AS TEXT)'
session = model.Session

@side_effect_free
def resource_items(context, data_dict):
  '''Returns items from asset if only {id} specified or single item if {item} specified as well. Also you can use {limit} and {offset} for global search'''
  _validate(data_dict, 'id')
  sql = "SELECT {fields} FROM \"{table}\"".format(fields=DEF_FIELDS, table=_sanitize(data_dict['id']) )
  sql_search =  toolkit.get_action('datastore_search_sql')

  item = data_dict.get('item')
  if item:
    try:
      # check PostgreSQL integer top bound
      if int(item) > 2147483647: raise ValueError
      fields_filter = " _id = '{0}' OR \"assetID\" = '{0}'".format(item)
    except ValueError:
      fields_filter = " \"assetID\" = '{0}' ".format( _sanitize(item) )
    where = " WHERE {0}".format(fields_filter)

  else:
    where = ' ORDER BY "lastModified" DESC LIMIT {0} OFFSET {1}'.format( int(data_dict.get('limit', DEF_LIMIT)), int(data_dict.get('offset', 0)) )


  result = sql_search(context, {'sql': sql + where })
  result['records'] = map(_filter_metadata, result['records'])

  package_id = session.query(model.Resource).filter_by(id=data_dict['id']).first().get_package_id()
  result['backlink'] = url_for(controller='package', action='resource_read', resource_id=data_dict['id'], id=package_id)[1:]
  package = toolkit.get_action('package_show')(context, {'id':package_id})

  result['title'] = package.get('title')
  result['description'] = package.get('notes')
  result['tags'] = ','.join([item['display_name'] for item in package.get('tags')])
  return result

@side_effect_free
def static_gallery_reset(context, data_dict):
  '''Recreate table with assets list'''
  if not data_dict.get('real') :
    return 0
  ds = [ item['name']

          for item

          in toolkit.get_action('datastore_search')(context, {'resource_id':'_table_metadata', 'fields':'name', 'limit':int(data_dict.get('assets_limit', '1000')) })['records' ] ]
  resources = [ str(resource[0])

                for resource

                in session.query(model.Resource.id).filter( model.Resource.state=='active',model.Resource.id.in_(ds) ).all() ]
  sql_search =  toolkit.get_action('datastore_search_sql')
  result = []

  session.execute('TRUNCATE {0}; ALTER SEQUENCE {0}_id_seq RESTART;'.format( str(DFMPAssets.__table__) ))

  for res in resources:
    sql = 'SELECT "assetID", name FROM "{table}" ORDER BY _id DESC LIMIT {limit}'.format(fields=DEF_FIELDS, table=res, limit=1000)
    try:
      result.extend([ DFMPAssets(parent=res,  asset_id=item['assetID'], name=item['name']) for item in sql_search(context,{'sql': sql} )['records']   ])
      
    except toolkit.ValidationError:
      continue
  
  seek = 0
  step = 1000
  total = len(result)
  while seek <= total:
    piece = result[seek : seek+step]
    seek += step


    session.add_all(piece)
    session.commit()
  return total

@side_effect_free
def dfmp_static_gallery(context, data_dict):
  '''Returns random items from gallery'''
  limit = int( data_dict.get('limit', 21) )

  total = session.query(func.max(DFMPAssets.id)).first()[0]

  ids = sample( range(1, total+1), limit if limit < total else total )
  
  result = {}
  assets = session.query(DFMPAssets.parent, DFMPAssets.asset_id).filter(DFMPAssets.id.in_(ids)).all()
  for item in assets:
    _concat_items(result, item)

  sql = _concat_sql(result)

  result = toolkit.get_action('datastore_search_sql')(context, {'sql': sql})['records']
  result = map(_filter_metadata, result)
  shuffle(result)
  return result

@side_effect_free
def search_item(context, data_dict):
  '''Search by name'''
  try:
    limit =int( data_dict.get('limit') ) + 1
  except Exception:
    limit=22
  try:
    offset = int( data_dict.get('offset') )
  except Exception:
    offset=0

  name = data_dict.get('query_string','')
  
  result = {}
  assets = session.query(DFMPAssets.parent, DFMPAssets.asset_id).filter(DFMPAssets.name.like('%{name}%'.format(name=name))).limit(limit).offset(offset).all()
  limit -=1
  for item in assets:
    _concat_items(result, item)
  if result:
    sql = _concat_sql(result)

    result = toolkit.get_action('datastore_search_sql')(context, {'sql': sql})
    result['records'] = map(_filter_metadata, result['records'])
  has_more = False
  if len(result['records']) > limit:
    has_more = True
    result['records'] = result['records'][:-1]
  result.update(limit=limit, offset=offset, has_more=has_more)
  return result


def _filter_metadata(rec):
  # del rec['_full_text']
  _check_datastore_json(rec, 'metadata')
  _check_datastore_json(rec, 'spatial')
  return rec

def _check_datastore_json(rec, field):
  try:
    if type( rec[field] ) in (str, unicode) and rec[field]:
      rec[field] = json.loads( _unjson(rec[field]) )
  except ValueError, e:
    try:
      rec[field] = json.loads( _unjson(rec[field]).replace('""','"') )
    except ValueError, e:
      log.warn(e)
      log.warn(rec)
      log.warn('Wrong {0}'.format(field))

def _sanitize(s):
  return s.replace(';','').replace('"','').replace("'","")

def _concat_items(result, item):
  if item.parent in result:
    result[item.parent] += "', '{0}".format(item.asset_id)
  else:
    result[item.parent] = item.asset_id

def _concat_sql(result, limit=''):
  sql = [ '( SELECT {fields} FROM "{id}" WHERE  "assetID" IN ( \'{assetID}\' {limit}) ) '.format(fields=DEF_FIELDS, id=item[0], assetID=item[1], limit=limit) for item in result.items() ]
  if len(sql) > 1:
    sql = ' UNION '.join(sql)
  else:
    sql = sql[0]
  return sql