import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from datetime import datetime
from ckanext.dfmp.action import _validate
import json

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
    resp = sql_search(context, {'sql': sql + " LIMIT {0} OFFSET {1}" .format( data_dict.get('limit', 99999), data_dict.get('offset', 0) ) })
  resp['records'] = filter(_filter_metadata, resp['records'])
  return resp

def _filter_metadata(rec):
  if type( rec['metadata'] ) in (str, unicode):
    try:
      rec['metadata'] = json.loads( rec['metadata'].replace('("{','{').replace('}","")','}').replace('""','"') )
    except ValueError:
      return False
  return True
