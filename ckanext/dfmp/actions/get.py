# import ckan.plugins as plugins
# import ckan.plugins.toolkit as toolkit
import logging

from ckan.common import c
import ckan.model as model
import ckan.logic as logic
from pylons import config
side_effect_free = logic.side_effect_free
NotFound = logic.NotFound
ValidationError = logic.ValidationError

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

@side_effect_free
def dfmp_get_thumbnail_url(context, data_dict):
  ''' Generates thumbnail and returns url. '''
  _validate(data_dict, 'image_url', 'width', 'height')

  import os
  import ckan.lib.uploader as uploader

  upload_path = uploader.get_storage_path()

  # check if we can write a file somewhere
  if not upload_path or not os.access(upload_path, os.W_OK):
    raise ValidationError('Storage path directory should be specified. It should be writable.')

  from PIL import Image, ImageOps
  import requests
  import io
  import hashlib

  size = (data_dict['width'], data_dict['height'])
  file = io.BytesIO(requests.get(data_dict['image_url']).content)
  try:
    im = Image.open(file)
    thumbnail = ImageOps.fit(
        im,
        size,
        Image.ANTIALIAS
    )
  except IOError, e:
    log.warn(e)
    raise NotFound

  # create thumbnail folder
  if not os.path.isdir(upload_path + '/thumbnails'):
    os.makedirs(upload_path + '/thumbnails')

  # create certain dimension folder
  resolution_dir = '_'.join(str(v) for v in size)
  if not os.path.isdir(upload_path + '/thumbnails/' + resolution_dir):
    os.makedirs(upload_path + '/thumbnails/' + resolution_dir)

  # generate thumbnail if not exist
  m = hashlib.md5()
  m.update(data_dict['image_url'])
  path = upload_path + '/thumbnails/' + resolution_dir + '/' + m.hexdigest() + '.jpg'
  if not os.path.exists(path):
    thumbnail.save(path, "JPEG")

  return {'resolution': resolution_dir, 'image': path.split('/')[-1]}