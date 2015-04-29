# import ckan.plugins as plugins
# import ckan.plugins.toolkit as toolkit
import logging

from ckan.common import c
import ckan.model as model
from ckan.logic import side_effect_free

from ckanext.dfmp.bonus import _validate, _only_admin, _name_normalize
from ckanext.dfmp.asset import Asset

session = model.Session

log = logging.getLogger(__name__)


@side_effect_free
@_only_admin
def dfmp_user_info(context, data_dict):
  def _get_info(id):
    user = model.User.by_name(id)
    if not user: return {}
    groups = [
      {'id':group.id,
       'name':group.name,
       'title':group.title
      } for group in user.get_groups()
      if group.is_organization
    ]
    return {
      'display_name': user.display_name,
      'name': user.name,
      'email': user.email,
      'groups':groups,
    }

  _validate(data_dict, 'name')
  name = data_dict['name']
  if not isinstance(name, list): name = [name]
  res = dict( [
    (item, _get_info(_name_normalize(item).lower()) ) 
    for item  in name
  ])
  return res

@side_effect_free
def dfmp_get_asset(context, data_dict):
  ''' Returns single asset by resource_id and asset_id'''
  _validate(data_dict, 'resource_id', 'asset_id')
  asset = Asset.get(data_dict['resource_id'], data_dict['asset_id'], context)['records']
  if asset:
    return asset.pop()

@side_effect_free
def dfmp_get_asset_list(context, data_dict):
  ''' Returns all assets of resource by resource_id and asset_id'''
  _validate(data_dict, 'resource_id')
  assets = Asset.get_all(data_dict['resource_id'], data_dict.get('limit', ''), data_dict.get('offset', ''), context=context)['records']
  return assets