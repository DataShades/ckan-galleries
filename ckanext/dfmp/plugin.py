import ckan.plugins as plugins
from time import sleep
from ckanext.dfmp.action import *

import logging
log = logging.getLogger(__name__)

class DFMPPlugin(plugins.SingletonPlugin, toolkit.DefaultDatasetForm):
  plugins.implements(plugins.IConfigurer)
  plugins.implements(plugins.IActions)
  plugins.implements(plugins.IDatasetForm)

  inProgress = 0

  def is_fallback(self):
    return True
  def package_types(self):
    return []

  def show_package_schema(self):
    schema = super(DFMPPlugin, self).show_package_schema()
    # schema.update({
    #     'custom_text': [tk.get_converter('convert_from_extras'),
    #                     tk.get_validator('ignore_missing')]
    # })
    schema['resources'].update({
                'license_id' : [ toolkit.get_validator('ignore_missing') ],
                'license_name':[ toolkit.get_validator('ignore_missing') ],
                'thumb' : [ toolkit.get_validator('ignore_missing') ],
                'something_else' : [ toolkit.get_validator('ignore_missing') ],
                'spatial': [toolkit.get_validator('ignore_missing')],
            })
    return schema

  def _modify_package_schema(self, schema):
    # schema.update({
    #     'custom_text': [tk.get_validator('ignore_missing'),
    #                     tk.get_converter('convert_to_extras')]
    # })
    schema['resources'].update({
                'license_id' : [ toolkit.get_validator('ignore_missing') ],
                'license_name':[ toolkit.get_validator('ignore_missing') ],
                'thumb' : [ toolkit.get_validator('ignore_missing') ],
                'something_else' : [ toolkit.get_validator('ignore_missing') ],
                'spatial': [toolkit.get_validator('ignore_missing')],
                })
    return schema

  def create_package_schema(self):
    schema = super(DFMPPlugin, self).create_package_schema()
    schema = self._modify_package_schema(schema)
    return schema

  def update_package_schema(self):
    schema = super(DFMPPlugin, self).update_package_schema()
    schema = self._modify_package_schema(schema)
    return schema

  def update_config(self, config):
    toolkit.add_template_directory(config, 'templates')
    toolkit.add_resource('fanstatic', 'dfmp')





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
        'my_packages_list':my_packages_list,
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

