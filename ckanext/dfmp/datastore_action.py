import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from datetime import datetime
from ckanext.dfmp.action import _validate

import logging
log = logging.getLogger(__name__)

@side_effect_free
def resource_items(context, data_dict):
  _validate(data_dict, 'id')
  sql = "SELECT * FROM \"%s\"" % data_dict['id']
  sql_search =  toolkit.get_action('datastore_search_sql')

  item = data_dict.get('item')
  if item:
    resp = sql_search(context, {'sql': sql + " WHERE _id = '%s'"  % (  item ) })
  else:
    resp = sql_search(context, {'sql': sql + " LIMIT {0} OFFSET {1}" .format( data_dict.get('limit', 20), data_dict.get('offset', 0) ) })

  return resp
