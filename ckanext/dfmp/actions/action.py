import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from datetime import datetime
import logging, copy, uuid, json, Polygon
from ckanext.dfmp.dfmp_solr import DFMPSolr, DFMPSearchQuery
import ckan.model as model
from pylons import config
from ckanext.dfmp.bonus import _validate, _get_index_id, _name_normalize
log = logging.getLogger(__name__)
session = model.Session
indexer = DFMPSolr()
searcher = DFMPSearchQuery()
#GET functions
@side_effect_free
def solr(context, data_dict):
  result = searcher(data_dict)
  for item in result.get('results', []):
    try:
      json_str = item['data_dict']
      json_dict = json.loads(json_str)
      item['data_dict'] = json_dict
    except:
      pass
  return result

def solr_add_assets(context, data_dict):
  for item in data_dict['items']:
    _asset_to_solr(item)
  indexer.commit()
  
@side_effect_free
def all_user_list(context, data_dict):
  U = context['model'].User
  users = [

    dict(name=user.name,
      api_key=user.apikey,
      organization=_organization_from_list(_user_get_groups(user))[1]
    ) for user
      in context['session'].query(U).filter(
        ~U.name.in_( ['default', 'visitor', 'logged_in'] ), U.state!='deleted'
    ).all()
  ]

  return users

@side_effect_free
def user_get_organization(context, data_dict):
  _validate(data_dict, 'user')
  user = _user_by_apikey(context, data_dict['user']).first()
  groups = _user_get_groups(user)
  org = _organization_from_list( groups )[1]
  return org

@side_effect_free
def all_organization_list(context, data_dict):
  orgs = [ [key, value['title'], value['id']]
    for key, value
    in enumerate(toolkit.get_action('organization_list')(context, {'all_fields':True}), 1)
  ]
  return orgs

@side_effect_free
def user_get_assets(context, data_dict):
  """Get all assets of user"""
  if not context['auth_user_obj']:

    log.warn('User not authorized to get assets!')
    raise toolkit.NotAuthorized
  try:

    package_id, resources = _get_pkid_and_resource(context)
    if not resources:
      return {}
    else:
      log.warn(resources)
      parent_id = resources[0].id
      assets = toolkit.get_action('resource_items')(context, {
        'id':parent_id,
        'limit':data_dict.get('limit',21),
        'offset':data_dict.get('offset', 0)
      })
      for record in assets['records']:
        record['parent_id'] = parent_id
        record['total'] = assets['count']

    return assets['records']
  except Exception, e:
    log.warn(_get_assets_container_name(context['auth_user_obj'].name))
    log.warn(e)
    return {}

@side_effect_free
def dfmp_tags(context, data_dict):
  q = data_dict.get('query', '')
  log.warn(q)
  tags = toolkit.get_action('tag_list')(context,{'query':q})
  asset_tags = searcher({
    'q':'*:*',
    'facet.field':'tags',
    'rows':0
  })['facets']['tags'].keys()
  for tag in asset_tags:
    if tag.lower().find(q.lower()) != -1:
      tags.append(tag)
  return dict(zip(tags,tags))

@side_effect_free
def flag_asset(context, data_dict):
  _validate(data_dict, 'id', 'assetID')
  index_id = _get_index_id(data_dict['id'], data_dict['assetID'])
  dd = searcher({
    'q':'+index_id:{index_id}'.format(index_id=index_id)
  })['results']
  if len(dd):
    asset = json.loads(dd[0]['data_dict'])
    asset['metadata']['flag'] = data_dict.get('flag','warning')
    _asset_to_solr(asset, defer_commit=False)
    return True
  return False

# ASSET functions
def user_add_asset_inner(context, data_dict, package_id, resources):
  organization = _organization_from_list(context['auth_user_obj'].get_groups())[2]
  data_dict['owner_name'] = organization.title if organization else context['auth_user_obj'].name
  data_dict['organization'] = organization.name if organization else None

  if not data_dict.get('spatial'):
    if data_dict.get('geoLocation'):
      location =  { "type": "Point", "coordinates": [
          float(data_dict['geoLocation']['lng']),
          float(data_dict['geoLocation']['lat'])
        ]}
    else:
      location = None
  
  if data_dict.get('license'):
    data_dict['license_id']   = data_dict['license']
    data_dict['license_name'] = _get_license_name(data_dict['license'])
  
  if not resources:
    new_id = make_uuid()
    parent = toolkit.get_action('resource_create')(context, {
      'package_id':package_id,
      'id':new_id,
      'url': config.get('ckan.site_url') + '/datastore/dump/' + new_id,
      'name':'Assets',
      'resource_type':'asset',
    })

  else:
    parent = toolkit.get_action('resource_show')(context, {'id': resources[0].id})

  if not parent.get('datastore_active'):
    _default_datastore_create(context, parent['id'])
  metadata = {'source':'user'}
  metadata.update(data_dict)
  datastore_item = toolkit.get_action('datastore_upsert')(context, {
    'resource_id':parent['id'],
    'force': True,
    'records':[{
      'assetID':make_uuid(),
      'lastModified':datetime.now().isoformat(' '),
      'name':data_dict['name'],
      'url':data_dict['url'],
      'spatial':location,
      'metadata':metadata,
      }
    ],
    'method':'upsert'
  })

  result = datastore_item['records'][0]
  log.warn(result)
  if type( result['metadata'] ) == tuple:
    result['metadata'] = json.loads(result['metadata'][0])
  if type( result['spatial'] ) == tuple:
    result['spatial'] = json.loads(result['spatial'][0])

  ind = {'id': parent['id']}
  ind.update(copy.deepcopy(result))
  _asset_to_solr(ind)

  result['parent_id'] = parent['id']
  indexer.commit()
  return result

def user_update_asset_inner(context, data_dict):
  """Update assets"""
  return _changes_route(context, data_dict, _update_generator)

def user_remove_asset_inner(context, data_dict):
  """Remove assets"""
  return _changes_route(context, data_dict, _delete_generator)

def _changes_route(context, data_dict, generator):
  if not 'items' in data_dict:
    data_dict['items'] = [data_dict.copy()]

  changer = generator(context, data_dict['items'])
  resources = [resource for resource in changer]
  indexer.commit()
  return resources

def _update_generator(context, data_dict):
  for item in data_dict:
    try:
      res = toolkit.get_action('datastore_search')(context, {
        'resource_id' : item['id'],
        'filters':{
          'assetID':item['assetID'],
        }
      })['records'][0]
      del res['_id']
      name = item.get('name')
      if name:
        res.update(name=name)
      res['metadata'].update(item)
      if item.get('license'):
        res['metadata']['license_id']   = res['metadata']['license'] = item['license']
        res['metadata']['license_name'] = _get_license_name(item['license'])

      ind_res = copy.deepcopy(res)
      result = toolkit.get_action('datastore_upsert')(context,{
        'resource_id' : item['id'],
        'force':True,
        'method': 'update',
        'records':[res]}
      )['records'][0]
      ind = {'id': item['id']}
      
      ind.update(ind_res)
      _asset_to_solr(ind)
      yield res

    except toolkit.ObjectNotFound:
      log.warn('res not found in update')
      yield {}

def _delete_generator(context, data_dict):
  for item in data_dict:
    try:
      log.warn(toolkit.get_action('datastore_delete')(context,{
        'resource_id': item['id'],
        'force': True,
        'filters':{
          'assetID':item['assetID'],
        }
      }))

      indexer.remove_dict({
        'id' : item['id'],
        'assetID' : item['assetID']
      }, defer_commit=True)
      yield {item['id']:True}

    except toolkit.ObjectNotFound:
      yield {item['id']:False}

# USER functions

def user_update_dataset(context, data_dict):
  log.warn('USER UPDATE DATASET')
  log.warn(data_dict)
  _validate(data_dict, 'title', 'description', 'tags' )
  dataset = toolkit.get_action('package_show')(context,{
    'id' : _get_assets_container_name(context['auth_user_obj'].name)
  })

  data_dict['title'] and dataset.update(title=data_dict['title'])
  data_dict['description'] and dataset.update(notes=data_dict['description'])
  if 'tags' in data_dict and type(data_dict['tags']) in (str, unicode):
    tags = [{'name':name}
      for name
      in data_dict.get('tags', '').split(',')
      if name
    ]
    dataset.update(tags=tags)

  toolkit.get_action('package_update')(context, dataset)

def user_create_with_dataset(context, data_dict):
  log.warn('USER CREATE WITH DATASET')
  log.warn(data_dict)
  _validate(data_dict, 'password', 'name', 'email' )
  title = data_dict.get('title', data_dict['name'])
  notes = data_dict.get('description', '')
  tags = []
  if 'tags' in data_dict and type(data_dict['tags']) in (str, unicode):
    tags = [{'name':name}
      for name
      in data_dict.get('tags', '').split(',')
      if name
    ]

  data_dict['name'] = _name_normalize(data_dict['name']).lower()

  try:
    user = toolkit.get_action('user_create')(context, data_dict)
  except toolkit.ValidationError, e:
    raise e

  try:
    package = toolkit.get_action('package_create')(context, {
      'name' : _get_assets_container_name(data_dict['name']),
      'title':title,
      'notes':notes,
      'tags':tags
    })

    try:
      toolkit.get_action('package_owner_org_update')(context,{
        'id':package['id'],
        'organization_id':'brand-cbr'
      })
    except Exception:
      log.warn('Error during adding user to organization ')
    try:
      toolkit.get_action('organization_member_create')(context, {
        'id':data_dict.get('organization_id','brand-cbr'),
        'username': data_dict['name'],
        'role':'editor'
      })
    except Exception:
      log.warn('Error during adding permissions to user')
  except toolkit.ValidationError, e:
    log.warn(e)

  return user

def delete_user_test(context, data_dict):
  user = _user_by_apikey(context, data_dict['user'])
  if not user.count(): return

  try:
    toolkit.get_action('package_delete')(context, { 'id' : _get_assets_container_name(user.first().name) })
    context['session']\
      .query(context['model'].Package)\
      .filter_by(name=_get_assets_container_name(user.first().name))\
      .first()\
      .delete()
  except:
    pass

  user.delete()
  context['session'].commit()

# ORGANIZATION functions

def create_organization(context, data_dict):
  _validate(data_dict, 'name')
  name = _transform_org_name(data_dict['name'])
  title = data_dict['name']

  org = toolkit.get_action('organization_create')(context, {
    'name':name,
    'title':title
  })
  return org

@side_effect_free
def organization_add_user(context, data_dict):
  _validate(data_dict, 'user', 'organization')
  user = _user_by_apikey(context, data_dict['user']).first()
  username = user.id
  user_current_org = _organization_from_list(_user_get_groups(user))[0]
  if user_current_org and not data_dict.get('only_update', False):
    toolkit.get_action('organization_member_delete')(context, {
      'id':user_current_org,
      'username':username
    })
  try:
    res = toolkit.get_action('organization_member_create')(context, {
      'id':data_dict['organization'],
      'username': username,
      'role':data_dict.get('role','editor')
    })
  except Exception, e:
    log.warn(e)
    return {}
  return res

def organization_remove_user(context, data_dict):
  _validate(data_dict, 'user', 'organization')
  toolkit.get_action('organization_member_delete')(context, {
    'id':data_dict['organization'],
    'username':_user_by_apikey(context,data_dict['user']).first().id
  })
  return True

# ADDITIONAL functions

def _organization_from_list(groups):
  if not len(groups):
    return (None, '', None)

  else:
    for group in groups:
      if group.type == 'organization':
        return (group.name, group.title, group)

    return (None, '', None)

def _user_get_groups(user):
  if not user:
    raise toolkit.ObjectNotFound('User does not exists')
  return user.get_groups()

def _get_assets_container_name(name):
  return 'dfmp_assets_' + name.lower()


def _user_by_apikey(context, key):
  return context['session']\
          .query(context['model'].User)\
          .filter_by(apikey=key)

def _transform_org_name(title):
  return title.replace(' ','_').lower()

def _get_license_name(id):
  license = filter(lambda x: x['id']==id, toolkit.get_action('license_list')(None,None) )
  if license:
    return license[0]['title']
  return ''

def _get_pkid_and_resource(context):
  package_id = _get_assets_container_name(context['auth_user_obj'].name)
  package = session.query(model.Package).filter(
    model.Package.name == package_id
  ).first()
  resources = filter(lambda x: x.state == 'active' and x.resource_type == 'asset', package.resources)
  return package_id, resources

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

def _asset_to_solr(data_dict, defer_commit=True):
  _validate(data_dict,'name', 'lastModified', 'id', 'assetID')
  indexer.index_asset(data_dict, defer_commit=defer_commit)
  return True

def make_uuid():
  return unicode(uuid.uuid4())

@side_effect_free
def get_last_geo_asset(context, data_dict):
  last_added = {}

  # search iteration start
  start = 0

  # define possible polygon coords
  min_lat = -35.47779
  max_lat = -35.12090
  min_lng = 148.76224
  max_lng = 149.42004

  # polygon validation
  def __not_in_polygon(lat, lng):
    return (lat > max_lat and (lng < min_lng or lng > max_lng)) or\
          (lat < min_lat and (lng < min_lng or lng > max_lng))

  # gets latest valid image
  while True:
    # requests the latest image
    last_added = DFMPSearchQuery()({
      'q': ' +entity_type:asset +type:image* +spatial:["" TO *] ',
      'sort': 'metadata_created desc',
      'start': start,
      'rows': 1
    })['results']
    start += 1

    # if there is no any assets with coordinates
    if not last_added: break
    
    # gets dict
    last_added = json.loads(last_added[0]['data_dict'])

    # Validates coordinates
    valid = True
    if last_added['spatial']['type'] == 'Polygon':
      # finds center of polygon
      polygon = Polygon.Polygon(last_added['spatial']['coordinates'][0])
      center = polygon.center()
      # center's lat and lng
      lat = center[1]
      lng = center[0]
      last_added['spatial']['coordinates'] = [lng, lat]
      last_added['spatial']['type'] = 'Point'
      if __not_in_polygon(lat, lng):
        valid = False

    else:
      # point's lat and lng
      lat = last_added['spatial']['coordinates'][1]
      lng = last_added['spatial']['coordinates'][0]
      if __not_in_polygon(lat, lng):
          valid = False
    # stops loop once valid asset is found
    if valid:
      break

  return json.dumps(last_added)
