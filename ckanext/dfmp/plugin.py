import ckan.plugins as plugins
from time import sleep
from ckanext.dfmp.action import *

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
        'delete_user_test':delete_user_test,
        'create_organization':create_organization,
        'all_organization_list':all_organization_list,
        'organization_add_user':organization_add_user,
        'organization_remove_user':organization_remove_user,
        'all_user_list':all_user_list,
        'user_get_organization':user_get_organization,
      }


def custom_stack(func):
  """Execute actions in queue"""
  def waiter(a, b):
    while DFMPPlugin.inProgress:
      sleep(.1)
    DFMPPlugin.inProgress += 1
    log.warn('in')
    try:
      result = func(a,b)
      log.warn('out')
      DFMPPlugin.inProgress -= 1
      return result
    except Exception, e:
      log.warn('out with error')
      DFMPPlugin.inProgress -= 1
      return e
  return waiter

@custom_stack
def user_add_asset(context, data_dict):
  """Add new assets"""
  return user_add_asset_inner(context, data_dict)

@custom_stack
def user_update_asset(context, data_dict):
  """Update assets"""
  return user_update_asset_inner(context, data_dict)

@custom_stack
def user_remove_asset(context, data_dict):
  """Remove assets"""
  return user_remove_asset_inner(context, data_dict)

