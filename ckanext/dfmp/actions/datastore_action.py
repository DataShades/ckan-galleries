import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from ckanext.dfmp.bonus import _validate, _unjson, _unjson_base
import json
from ckan.lib.helpers import url_for
import ckan.model as model
from random import shuffle, sample, randint
import logging
log = logging.getLogger(__name__)
from dateutil.parser import parse
from ckanext.dfmp.dfmp_model import DFMPAssets
from sqlalchemy import func

from pylons import config

from ckanext.dfmp.actions.action import _get_package_id_by_res
from ckanext.dfmp.actions.action import indexer, searcher

DEF_LIMIT = 21
DEF_FIELDS = '_id, CAST("assetID" AS TEXT), CAST(url AS TEXT), CAST("lastModified" AS TEXT), metadata, name, spatial'
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

  organization = package['organization']
  if organization:
    organization['dfmp_link'] = config.get('ckan.site_url') + '/organization/{name}'.format(name=organization['name'])

    pkgs = session.query(model.Group).get(organization['id']).packages()
    all_res = []
    for pkg in pkgs:
      for res in pkg.resources:
        all_res.append(res.id)
    ids = ' OR '.join(all_res)
    ammount = searcher({
      'q':'id:({ids})'.format(ids=ids),
      'fq':'-state:hidden',
      'rows':0,
    })['count']
    organization['dfmp_assets'] = ammount
  result['organization'] = organization
  if result['records']:
    result['records'][0]['organization'] = organization
  result['title'] = package.get('title')
  result['description'] = package.get('notes')
  result['tags'] = ','.join([item['display_name'] for item in package.get('tags')])
  return result

@side_effect_free
def static_gallery_reset(context, data_dict):
  '''Deprecated'''
  return

@side_effect_free
def dfmp_static_gallery(context, data_dict):
  '''Returns random items from gallery'''
  ammount = searcher({
    'q':'',
    'fq':'-state:hidden',
    'rows':0,
  })['count']
  limit = int( data_dict.get('limit', 21) )

  offset = randint (0, ammount - limit - 1)

  result = searcher({
    'q':'',
    'fl':'data_dict',
    'fq':'-state:hidden',
    'rows':limit,
    'start':offset,
  })
  records = []
  for item in result['results']:
    try:
      json_str = item['data_dict']
      json_dict = json.loads(json_str)
      records.append(json_dict)
    except:
      pass
  # del result['results']
  # result.update(records=records, limit=limit, has_more=None)
  return records

@side_effect_free
def dfmp_all_assets(context, data_dict):
  limit = int(data_dict.get('limit', 8))
  offset = int(data_dict.get('offset', 0))
  result = searcher({
    'q':'',
    'facet.field':'id',
    'rows':0,
  })
  log.warn(result['facets']['id'].keys())
  ids = result['facets']['id'].keys()[offset:]
  log.warn(ids)
  response = []
  for item in ids:
    try:
      package_id = _get_package_id_by_res(item)
    except AttributeError:
      continue
    log.warn(package_id)
    package = toolkit.get_action('package_show')( 
      context,
      {'id':package_id}
    )
    package['asset'] = filter(lambda x: x['id'] == item, package['resources'])[0]
    del package['resources']

    package['tags'] = [tag['display_name'] for tag in package['tags']]

    package['dataset_link']=url_for(controller='package', action='read', id=package_id)
    package['asset_link'] = package['dataset_link'] + '/resource/{res}'.format(res=item)

    response.append(package)
    if len(response) >= limit:
      break

  return response

@side_effect_free
def search_item(context, data_dict):
  '''Search by name'''
  log.warn(data_dict)
  # {'query_string': {'date': u'2015-03-11', 'name': u'f', 'tags': u'awesome'}, 'limit': 12}
  _validate(data_dict, 'query_string')

  # data_dict['query_string'] = json.loads(data_dict['query_string'])

  limit = data_dict.get('limit', 21)
  offset = data_dict.get('offset', 0)
  atype = data_dict['query_string'].get('type', '')
  if atype:
    atype = '+(extras_mimetype:{type}* OR extras_type:{type}*)'.format(type=atype)

  tags = data_dict['query_string'].get('tags')
  if type(tags) in (str, unicode):
    tags = [tag.strip() for tag in tags.split(',') if tag]
  tags = '+tags:({tags})'.format(tags = ' OR '.join(tags)) if tags else ''

  name = data_dict['query_string'].get('name', '')
  if name:
    name = '+name:"{name}"'.format(name = name)
  
  date = data_dict['query_string'].get('date') or '1111-11-11'
  try:
    date = '+metadata_modified:[{start} TO *]'.format(
      start= parse(date).isoformat() + 'Z'
    )
  except ValueError:
    date = ''
  log.warn(date)
  result = searcher({
    'q':'{name} {tags} {date} {type}'.format(
      name = name,
      tags = tags,
      date = date,
      type = atype
    ),
    'fl':'data_dict',
    'fq':'-state:hidden',
    'rows':limit,
    'start':offset,
  })
  log.warn(result)
  records = []
  for item in result['results']:
    try:
      json_str = item['data_dict']
      json_dict = json.loads(json_str)
      records.append(json_dict)
    except:
      pass
  del result['results']
  log.warn(records)
  result.update(records=records, limit=limit, offset=offset)
  return result


def _filter_metadata(rec):
  # del rec['_full_text']
  _check_datastore_json(rec, 'metadata')
  _check_datastore_json(rec, 'spatial')
  return rec

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