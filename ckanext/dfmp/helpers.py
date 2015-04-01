from ckan.common import c
from ckanext.dfmp.dfmp_solr import DFMPSearchQuery
import ckan.plugins.toolkit as toolkit 
from ckanext.dfmp.bonus import _count_literal
from ckanext.dfmp.dfmp_solr import DFMPSearchQuery


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

def dfmp_last_added_assets_with_spatial_data():
  twitter_items = DFMPSearchQuery()({
    'q': '+entity_type:asset +type:image* +extras_retweeted:[* TO *]',
    'rows': 0
  })['count']

  flickr_items = DFMPSearchQuery()({
    'q': '+entity_type:asset +type:image* +extras_source:flickr',
    'rows': 0
  })['count']

  last_added = DFMPSearchQuery()({
    'q': '+entity_type:asset +type:image* +spatial:[* TO *]',
    'sort': 'metadata_created desc',
    'rows': 1
  })

  return {
    'twitter': twitter_items,
    'flickr': flickr_items,
    'last_added': last_added,
  }
