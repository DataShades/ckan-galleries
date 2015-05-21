# import ckan.plugins as plugins
# import ckan.plugins.toolkit as toolkit
import ckan.logic as logic
from ckan.common import c
import ckan.model as model

from ckanext.dfmp.bonus import _validate, _only_admin, _name_normalize
from ckanext.dfmp.asset import Asset

session = model.Session

import logging

log = logging.getLogger(__name__)

@logic.side_effect_free
def dfmp_update_asset(context, data_dict):
  _validate(data_dict, 'resource_id', 'asset_id')
  resource_id = data_dict['resource_id']
  return Asset.update(data_dict, context=context, defer_commit=False)
