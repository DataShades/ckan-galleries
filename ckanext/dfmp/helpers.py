from ckan.common import c
from ckanext.dfmp.dfmp_solr import DFMPSearchQuery
import ckan.plugins.toolkit as toolkit 

def dfmp_with_gallery(id):
  res = toolkit.get_action('resource_show')(None, {'id':id})
  result = res.get('datastore_active', False)
  return result

def is_sysadmin():
  if c.userobj:
    return c.userobj.sysadmin
  return False

def total_ammount_of_assets():
  return DFMPSearchQuery()({
    'rows':0,
    'q':'*:*'
    })['count']