from ckan.common import c
import ckan.plugins.toolkit as toolkit 
from ckanext.dfmp.bonus import _count_literal, _get_rel_members
from ckanext.dfmp.dfmp_solr import DFMPSearchQuery
import datetime
from dateutil.parser import parse
import ckan.model as model
from pylons import config
import ckan.logic
NotFound = ckan.logic.NotFound
session = model.Session

REQUIRED_DATASTORE_COLS = [
  '_id',
  'assetID',
  'lastModified',
  'name',
  'url',
  'spatial',
  'metadata',
]


def dfmp_recently_added():
  import logging as log 
  q = session.query(model.Resource.name, model.Resource.id)\
    .join(model.ResourceGroup).join(model.Package)\
    .filter(model.Package.state == 'active',
            model.Package.private == False,
            model.Resource.state == 'active')\
    .order_by('created desc')\
    .limit(4)\
    .all()
  for item in q:
    result = DFMPSearchQuery.run({
      'q':'*:*',
      'rows':1,
      'fq':'id:%s' % item.id,
      'fl':'url,extras_thumb',
      'sort':'metadata_modified asc',
      'facet.field':'id'
      })
    count = result['facets']['id'].values()
    item.count = count[0] if count else 0
    asset = result['results']
    item.image = asset[0] if asset else {'extras':{}}
  return q

def dfmp_with_gallery(id):
  with_gallery = False
  total_in_ds = 0
  ammount = 0
  try:
    ds = toolkit.get_action('datastore_search')(None, {
      'resource_id':id,
      'limit': 1,
    })
    field_names = map(lambda x: x['id'], ds['fields'])
    for field in REQUIRED_DATASTORE_COLS:
      if not field in field_names:
        raise Exception
    with_gallery = True

    total_in_ds = ds.get('total', 0)
    ammount = DFMPSearchQuery.run({
    'rows':0,
    'q':'*:*',
    'fq':'+id:{0}'.format(id)
    })['count']

  except Exception, e:
    pass

  return with_gallery, total_in_ds, ammount

def is_sysadmin():
  if c.userobj:
    return c.userobj.sysadmin
  return False

def dfmp_total_ammount_of_assets():
  ammount = DFMPSearchQuery.run({
    'rows':0,
    'q':'*:*'
    })['count']
  return _count_literal(ammount)

def dfmp_total_ammount_of_datasets():
  ammount = toolkit.get_action('package_search')(None,{'q':'entity_type:package'})['count']
  return _count_literal(ammount)


def dfmp_last_added_assets_with_spatial_data():
  # twitter_items = DFMPSearchQuery.run({
  #   'q': '+entity_type:asset +type:image* +extras_retweeted:[* TO *] +metadata_created[' + datetime.datetime.now().replace(hour=0, minute=0, second=0).isoformat()[0:19] + 'Z' + ' TO *]',
  #   'rows': 0,
  # })['count']
  #
  # flickr_items = DFMPSearchQuery.run({
  #   'q': '+entity_type:asset +type:image* +extras_source:flickr +metadata_created[' + datetime.datetime.now().replace(hour=0, minute=0, second=0).isoformat()[0:19] + 'Z' + ' TO *]',
  #   'rows': 0
  # })['count']
  return toolkit.get_action('get_last_geo_asset')()

def dfmp_current_server_time():
  return datetime.datetime.now()

def dfmp_nice_date(date):
  try:
    result = parse(date).strftime('%d %b %Y')
  except:
    result = ''
  return result

def dfmp_relationship(org):
  from logging import warn
  org = session.query(model.Group).get(org)
  children = _get_rel_members(org, 'child_organization')

  parents = _get_rel_members(org, 'parent_organization')

  friends = []
  parent_orgs = model.Session.query(model.Group)\
            .filter(model.Group.id.in_(parents)).all()
  for parent in parent_orgs:
            friends.extend(_get_rel_members(parent, 'child_organization'))
  org_ids =friends + children + parents
  orgs = dict(model.Session.query(model.Group.id, model.Group)\
            .filter(model.Group.id.in_(org_ids))\
            .all())
  return parents, children, friends, orgs

def dfmp_relative_time(time):
  try:
    parsed_time = parse(time).replace(tzinfo=None)
    diff = datetime.datetime.now() - parsed_time
    sec = diff.seconds
    if sec < 60:
      di = sec
      ending = 's' if di > 1 else ''
      return "{0} second{1} ago".format(di, ending)
    elif sec / 60 < 60:
      di = sec / 60
      ending = 's' if di > 1 else ''
      return "{0} minute{1} ago".format(di, ending)
    elif sec / 60 / 24 < 24:
      di = sec / 60 / 24
      ending = 's' if di > 1 else ''
      return "{0} hour{1} ago".format(di, ending)
    elif sec / 60 / 24 / 7 < 2:
      time = parsed_time.strftime('%I:%M %p')
      return "{0} yesterday".format(time)
    else:
      time = parsed_time.strftime('%d %b %Y %H:%M')
      return time
    return time
  except Exception, e:
    return time

def dfmp_get_thumbnail(path, width, height):
  ''' Gets thumbnail url '''
  try:
    thumbnail = toolkit.get_action(
      'dfmp_get_thumbnail_url')({}, {
      'image_url': path,
      'width': width,
      'height': height
    })
  # returns resized default image if image not found
  except NotFound:
    thumbnail = toolkit.get_action(
      'dfmp_get_thumbnail_url')({}, {
      'image_url': config.get('ckan.site_url') + '/dfmp/images/default_thumb.png',
      'width': width,
      'height': height
    })
  return ckan.lib.helpers.url_for(
    controller='ckanext.dfmp.controller:DFMPController',
    action='get_thumbnail',
    resolution=thumbnail['resolution'],
    image=thumbnail['image'])
