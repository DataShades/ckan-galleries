import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from datetime import datetime
from time import sleep

import logging
log = logging.getLogger(__name__)

class DFMPPlugin(plugins.SingletonPlugin):
  plugins.implements(plugins.IActions)
  inProgress = 0
  def get_actions(self):
      return {
        'user_add_asset': user_add_asset,
        'user_get_assets': user_get_assets,
        'user_create_with_dataset': user_create_with_dataset,
        'user_remove_asset':user_remove_asset,
        'user_update_asset': user_update_asset,
      }




def user_add_asset(context, data_dict):
  """Add new asset"""
  while DFMPPlugin.inProgress:
    sleep(.1)
  DFMPPlugin.inProgress += 1
  try:
    res = _res_init(data_dict)
    res['package_id'] = _get_assets_container_name(context['auth_user_obj'].name)
    resource = toolkit.get_action('resource_create')(context, res )
  except Exception, e:
    DFMPPlugin.inProgress -= 1
    return e
  DFMPPlugin.inProgress -= 1
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

def user_update_asset(context, data_dict):
  """Update asset"""
  res = toolkit.get_action('resource_show')(context, { 'id' : data_dict['id'] })
  records = toolkit.get_action('datastore_search')(context,{'resource_id': data_dict['id']})['records'][-1]
  del records['_id']
  records['license_id'] = data_dict['license']


  datastore = toolkit.get_action('datastore_upsert')(context,{
                                          'force':True,
                                          'resource_id': data_dict['id'],
                                          'method':'insert',
                                          'records': [
                                            records,
                                          ]
                                     })
  res.update(datastore=datastore.get('records'))
  return res



@side_effect_free
def user_get_assets(context, data_dict):
  """Get all assets of user"""
  try:
    dataset = toolkit.get_action('package_show')(context,{'id' : _get_assets_container_name(context['auth_user_obj'].name) })
    for resource in dataset['resources']:
      resource.update( datastore = toolkit.get_action('datastore_search')(context,{'resource_id': resource['id']}).get('records') )
    return dataset
  except toolkit.ObjectNotFound, e:
    return {}


def user_remove_asset(context, data_dict):
  """Remove one asset"""
  try:
    result = toolkit.get_action('datastore_delete')(context,{
                                          'force':True,
                                          'resource_id': data_dict['id'],
                                          })
  except toolkit.ObjectNotFound:
    pass
  toolkit.get_action('resource_delete')(context,{'id': data_dict['id']})


def user_create_with_dataset(context, data_dict):
  user = toolkit.get_action('user_create')(context, data_dict)

  try:
    toolkit.get_action('package_create')(context, { 'name' : _get_assets_container_name(data_dict['name']) })
  except toolkit.ValidationError, e:
    log.warn(e)

  return user


def _get_assets_container_name(name):
  return 'dfmp_assets_'+name

def _res_init(data_dict):
  return dict(url      = data_dict['url'],
              name     = data_dict['name'],
              size     = data_dict['size'],
              mimetype = data_dict['type'])

def _init_records(context, data_dict):
  orgs = toolkit.get_action('organization_list_for_user')(context, {'permission':'read'})
  owner_id    = orgs[0]['id']     if orgs else context['auth_user_obj'].id
  owner_name  = orgs[0]['title']  if orgs else context['auth_user_obj'].name

  return dict(creator_id = context['auth_user_obj'].id,
              creator_name = context['auth_user_obj'].name,
              date = datetime.now().isoformat(),
              owner_id = owner_id,
              owner_name = owner_name,
              license_id = data_dict['license'],
              type = data_dict['type'],
              thumb = data_dict['thumbnailUrl'])