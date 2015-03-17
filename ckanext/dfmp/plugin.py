import ckan.plugins as plugins
from time import sleep
from ckanext.dfmp.actions.action import *
from ckanext.dfmp.actions.action import _get_pkid_and_resource

from ckanext.dfmp.actions.datastore_action import *
from ckan.logic import side_effect_free
from ckanext.dfmp.actions.background import *
from ckanext.dfmp.actions.social import *
import datetime
from dateutil.parser import parse
from ckan.common import c

import ckan.model as model
session = model.Session

import logging
log = logging.getLogger(__name__)

class DFMPPlugin(plugins.SingletonPlugin, toolkit.DefaultDatasetForm):
  plugins.implements(plugins.IConfigurer)
  plugins.implements(plugins.IActions)
  plugins.implements(plugins.ITemplateHelpers)
  plugins.implements(plugins.IRoutes, inherit=True)

  inProgress = 0

  def before_map(self, map):
    map.connect(
        'getting_tweets', '/dataset/{id}/pull_tweets/{resource_id}',
        controller='ckanext.dfmp.controller:DFMPController',
        action='getting_tweets', ckan_icon='twitter-sign')
    map.connect(
        'terminate_listener', '/dataset/{id}/terminate_listener/{resource_id}',
        controller='ckanext.dfmp.controller:DFMPController',
        action='terminate_listener', ckan_icon='twitter-sign')
    map.connect(
        'start_listener', '/dataset/{id}/start_listener/{resource_id}',
        controller='ckanext.dfmp.controller:DFMPController',
        action='start_listener', ckan_icon='twitter-sign')
    map.connect(
        'get_flickr', '/dataset/new_from_flickr',
        controller='ckanext.dfmp.controller:DFMPController',
        action='get_flickr')
    map.connect(
        'ckanadmin_twitter_streamers', '/ckan-admin/twitter-listeners',
        controller='ckanext.dfmp.controller:DFMPController',
        action='twitter_listeners', ckan_icon='twitter-sign')
    map.connect(
        'solr_commit', '/ckan-admin/solr_commit',
        controller='ckanext.dfmp.controller:DFMPController',
        action='solr_commit', ckan_icon='twitter-sign')
    map.connect(
        'search_assets', '/search_assets',
        controller='ckanext.dfmp.controller:DFMPController',
        action='search_assets', ckan_icon='')
    
    return map

  def get_helpers(self):
    return {'dfmp_with_gallery':dfmp_with_gallery,
            'is_sysadmin':is_sysadmin,
            }

  def update_config(self, config):
    toolkit.add_template_directory(config, 'templates')
    toolkit.add_resource('fanstatic', 'dfmp')

  def get_actions(self):
      return {
        'user_get_assets': user_get_assets,
        'user_add_asset': user_add_asset,
        'user_update_asset': user_update_asset,
        'user_remove_asset':user_remove_asset,
        'user_create_with_dataset': user_create_with_dataset,
        'all_user_list':all_user_list,
        'delete_user_test':delete_user_test,
        'create_organization':create_organization,
        'organization_add_user':organization_add_user,
        'organization_remove_user':organization_remove_user,
        'all_organization_list':all_organization_list,
        'user_get_organization':user_get_organization,
        'resource_items':resource_items,
        'static_gallery_reset':static_gallery_reset,
        'dfmp_static_gallery':dfmp_static_gallery,
        'search_item':search_item,
        'dfmp_tags':dfmp_tags,
        'celery_cleaning':celery_cleaning,
        'celery_getting_tweets': celery_getting_tweets,
        'celery_streaming_tweets':celery_streaming_tweets,
        'celery_revoke':celery_revoke,
        'celery_flickr_import':celery_flickr_import,
        'flickr_import_group_pool':flickr_import_group_pool,
        'solr':solr,
        'solr_add_assets':solr_add_assets,
        'user_update_dataset':user_update_dataset,

      }


def custom_stack(func):
  """Execute actions in queue"""
  def waiter(a, b):
    while DFMPPlugin.inProgress: sleep(.5)
    DFMPPlugin.inProgress += 1
    log.warn('in')
    try:
      result = func(a,b)
      log.warn('out')
      DFMPPlugin.inProgress -= 1
      return result
    except Exception, e:
      log.warn(e)
      log.warn('out with error')
      DFMPPlugin.inProgress -= 1
      return e
  return waiter

@custom_stack
def user_add_asset(context, data_dict):
  """Add new assets"""
  package_id, resources = _get_pkid_and_resource(context)
  return user_add_asset_inner(context, data_dict, package_id, resources)

@custom_stack
def user_update_asset(context, data_dict):
  """Update assets"""
  return user_update_asset_inner(context, data_dict)

@custom_stack
def user_remove_asset(context, data_dict):
  """Remove assets"""
  return user_remove_asset_inner(context, data_dict)

def dfmp_with_gallery(id):
  res = toolkit.get_action('resource_show')(None, {'id':id})
  result = res.get('datastore_active', False)
  return result

def is_sysadmin():
  if c.userobj:
    return c.userobj.sysadmin
  return False
