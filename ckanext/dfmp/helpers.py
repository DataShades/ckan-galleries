from ckan.common import c
from ckanext.dfmp.dfmp_solr import DFMPSearchQuery
import ckan.plugins.toolkit as toolkit 
from ckanext.dfmp.bonus import _count_literal

def dfmp_with_gallery(id):
  res = toolkit.get_action('resource_show')(None, {'id':id})
  result = res.get('datastore_active', False)
  return result

def is_sysadmin():
  if c.userobj:
    return c.userobj.sysadmin
  return False

def dfmp_total_ammount_of_assets():
  ammount = DFMPSearchQuery()({
    'rows':0,
    'q':'*:*'
    })['count']
  return _count_literal(ammount)

def dfmp_total_ammount_of_datasets():
  ammount = toolkit.get_action('package_search')(None,{'q':'entity_type:package'})['count']
  return _count_literal(ammount)