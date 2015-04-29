import ckan.plugins.toolkit as toolkit
import hashlib, string, json, uuid
from pylons import config
from ckan.common import c
import ckan.model as model

session = model.Session

KEY_CHARS = string.digits + string.letters + "_-"


def _get_index_id(id, assetID):
  return hashlib.md5('%s%s' % (id + assetID, config.get('ckan.site_id'))).hexdigest()

def _make_uuid():
  return unicode(uuid.uuid4())

def _site_url():
  return config.get('ckan.site_url')

def _asset_name_from_url(url):
  slash_index = url.rfind('/')
  if slash_index == -1:
    name = url
  else:
    name = url[slash_index+1:]
  return name

def _validate(data, *fields):
  for field in fields:
    if not field in data:
      raise toolkit.ValidationError('Parameter {%s} must be defined' % field)

def _unjson_base(string):
  return string\
    .replace('"("{','{')\
    .replace('("{','{')\
    .replace('}","")"','}')\
    .replace('}","")','}')\
    .replace('\\\\""','\\""')

def _unjson(string):
  try:
    return _unjson_base(string).replace('""','"')
  except:
    return json.loads(string)


def _get_package_id_by_res(id):
  return session.query(model.Resource).filter_by(id=id).first().get_package_id()

def _unique_list(array):
  doppelganger = set()
  doppelganger_add = doppelganger.add
  return [ x for x in array if not (x in doppelganger or doppelganger_add(x))]

def _only_admin(func):
  def function(*pargs, **kargs):
    if not c.userobj or not c.userobj.sysadmin:
      raise toolkit.NotAuthorized
    return func(*pargs, **kargs)
  return function

def _name_normalize(name):
  return ''.join([
      c
      for c
      in name
      if c in KEY_CHARS
    ])

def _count_literal(ammount):
  if ammount < 1e3:
    value = ammount
  elif ammount < 1e6:
    value = '%sK' % int(ammount/1e3)
  elif ammount < 1e9:
    value = '%sM' % int(ammount/1e6)
  elif ammount >= 1e9:
    value = '%sB' % int(ammount/1e9)
  return value

def _get_rel_members(collection, capacity):
  return [item.table_id for item in filter(
      lambda x: x.capacity==capacity and x.state == 'active',
      collection.member_all
    )]

def _get_license_name(id):
  license = filter(lambda x: x['id']==id, toolkit.get_action('license_list')(None,None) )
  if license:
    return license[0]['title']
  return ''