import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from datetime import datetime

import logging
log = logging.getLogger(__name__)

#GET functions

@side_effect_free
def all_user_list(context, data_dict):
  U = context['model'].User
  users = [dict(name=user.name, api_key=user.apikey, organization=_organization_from_list(_user_get_groups(user))[1] ) 
      for user in context['session'].query(U).filter(
        ~U.name.in_(['default', 'visitor', 'logged_in']
      ), U.state!='deleted' ).all()]

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
  orgs = [ [key, value['title'], value['id']] for key, value in enumerate(toolkit.get_action('organization_list')(context, {'all_fields':True}), 1) ]
  return orgs

@side_effect_free
def user_get_assets(context, data_dict):
  """Get all assets of user"""
  try:
    dataset = toolkit.get_action('package_show')(context,{'id' : _get_assets_container_name(context['auth_user_obj'].name) })

    for resource in dataset['resources']:
      try:
        resource.update( datastore = toolkit.get_action('datastore_search')(context,{'resource_id': resource['id']}).get('records') )
      except toolkit.ObjectNotFound:
        resource.update(datastore = [])
    return dataset
  except toolkit.ObjectNotFound, e:
    log.warn(_get_assets_container_name(context['auth_user_obj'].name))
    log.warn(e)
    return {}

@side_effect_free
def my_packages_list(context, data_dict):
  user = context['auth_user_obj'].id
  packages = toolkit.get_action('package_search')(context, {'q':'creator_user_id:{0}'.format(user)})
  return packages


# ASSET functions
def user_add_asset_inner(context, data_dict):
  res = _res_init(data_dict)
  res['package_id'] = _get_assets_container_name(context['auth_user_obj'].name)
  resource = toolkit.get_action('resource_create')(context, res )
  datastore = toolkit.get_action('datastore_create')(context,{'force':True,
                                                              'resource_id': resource['id'],
                                                              'fields':[
                                                                {'id':'date', 'type':'text'},
                                                                {'id':'creator_id', 'type':'text'},
                                                                {'id':'creator_name', 'type':'text'},
                                                                {'id':'owner_id', 'type':'text'},
                                                                {'id':'owner_name', 'type':'text'},
                                                                {'id':'license_id', 'type':'text'},
                                                                {'id':'type', 'type':'text'},
                                                              ],
                                                              'records': [
                                                                _init_records(context, data_dict),
                                                              ]})
  resource.update(datastore=datastore.get('records'))
  return resource

def user_update_asset_inner(context, data_dict):
  """Update assets"""
  updater = _update_generator(context, data_dict['items'])
  resources = [resource for resource in updater]
  return resources

def user_remove_asset_inner(context, data_dict):
  """Remove assets"""
  if not 'items' in data_dict:
    data_dict['items'] = [{'id': data_dict.get('id')}]
  deleter = _delete_generator(context, data_dict['items'])
  resources = [resource for resource in deleter]
  return resources



# USER functions

def user_create_with_dataset(context, data_dict):
  data_dict['name'] = data_dict['name'].lower()

  try:
    user = toolkit.get_action('user_create')(context, data_dict)
  except Exception, e:
    log.warn(e)
    user = toolkit.get_action('user_show')(context, {'id':data_dict['name']})

  try:
    toolkit.get_action('package_create')(context, { 'name' : _get_assets_container_name(data_dict['name']) })
  except toolkit.ValidationError, e:
    log.warn(e)
  return user

def delete_user_test(context, data_dict):
  # toolkit.get_action('package_show')(context, { 'id' : _get_assets_container_name(data_dict['name']) })
  user = _user_by_apikey(context, data_dict['user'])
  if not user.count(): return
  try:
    toolkit.get_action('package_delete')(context, { 'id' : _get_assets_container_name(user.first().name) })
    context['session'].query(context['model'].Package).filter_by(name=_get_assets_container_name(user.first().name)).first().delete()
  except:
    pass
  user.delete()
  context['session'].commit()


# ORGANIZATION functions

def create_organization(context, data_dict):
  _validate(data_dict, 'name')
  name = _transform_org_name(data_dict['name'])
  title = data_dict['name']
  org = toolkit.get_action('organization_create')(context, {'name':name,'title':title})
  return org

def organization_add_user(context, data_dict):
  _validate(data_dict, 'user', 'organization')
  log.warn(data_dict)
  user = _user_by_apikey(context, data_dict['user']).first()
  username = user.id
  user_current_org = _organization_from_list(_user_get_groups(user))[0]
  if user_current_org:
    toolkit.get_action('organization_member_delete')(context, {'id':user_current_org,'username':username})
  try:
    res = toolkit.get_action('organization_member_create')(context, {'id':data_dict['organization'],'username': username,'role':'editor'} )
  except Exception, e:
    log.warn(e)
    return {}
  return res

def organization_remove_user(context, data_dict):
  _validate(data_dict, 'user', 'organization')
  toolkit.get_action('organization_member_delete')(context, {'id':data_dict['organization'],'username':_user_by_apikey(context, data_dict['user']).first().id})
  return True


# ADDITIONAL functions 

def _validate(data, *fields):
  for field in fields:
    if not field in data: 
      raise toolkit.ValidationError('Parameter {%s} must be defined' % field)

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
  return 'dfmp_assets_'+name

def _res_init(data_dict):
  return dict(url      = data_dict['url'],
              name     = data_dict['name'],
              size     = data_dict['size'],
              mimetype = data_dict['type'])

def _init_records(context, data_dict):
  organization = _organization_from_list(context['auth_user_obj'].get_groups())[2] 

  owner_id    = organization.id     if organization else context['auth_user_obj'].id
  owner_name  = organization.title  if organization else context['auth_user_obj'].name

  return dict(creator_id = context['auth_user_obj'].id,
              creator_name = context['auth_user_obj'].name,
              date = datetime.now().isoformat(),
              owner_id = owner_id,
              owner_name = owner_name,
              license_id = data_dict['license'],
              type = data_dict['type'],
              thumb = data_dict['thumbnailUrl'])

def _update_generator(context, data_dict):
  for item in data_dict:
    try:
      res = toolkit.get_action('resource_show')(context, { 'id' : item['id'] })
      records = toolkit.get_action('datastore_search')(context,{'resource_id': item['id']})['records'][-1]
      del records['_id']
      records['license_id'] = item['license']

      datastore = toolkit.get_action('datastore_upsert')(context,{
                                              'force':True,
                                              'resource_id': item['id'],
                                              'method':'insert',
                                              'records': [
                                                records,
                                              ]
                                         })
      res.update(datastore=datastore.get('records'))
      yield res
    except toolkit.ObjectNotFound:
      yield {}

def _delete_generator(context, data_dict):
  for item in data_dict:
    try:
      toolkit.get_action('resource_delete')(context,{'id': item['id']})
      yield {item['id']:True}
    except toolkit.ObjectNotFound:
      yield {item['id']:False}

def _user_by_apikey(context, key):
  return context['session'].query(context['model'].User).filter_by(apikey=key)

def _transform_org_name(title):
  return title.replace(' ','_').lower()