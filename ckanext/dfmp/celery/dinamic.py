import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from datetime import datetime

import ckan.model as model
import logging, urlparse, json, requests, urllib2
log = logging.getLogger(__name__)


def datastore_mass(context, data, workflow):
  # try:
  context = json.loads(context)
  offlim= [0, 10]
  try:
    while True:
      current_status = _get_status(context, data, task_type=workflow)
      if current_status == 'stop':
        print 'Terminated by API'
        raise Exception('Stop')
      post_data = {'resource_id':data['resource'],

                   'offset':offlim[0],

                   'limit':offlim[1]}
      print(workflow)
      result = {
        'clearing':clearing,
      }[workflow](data, context, post_data, offlim)

      if result and result.get('done'):
        break
    _change_status(context, data, status='Done', task_type=workflow)
    print 'Done'
  except toolkit.ObjectNotFound:
    _change_status(context, data, status='Error: Resource not found', task_type=workflow)
  # except Exception, e:
  #   log.warn(e)

def _celery_api_request(action, data, context, post_data):
  # api_url = urlparse.urljoin(context['site_url'], '/data/api/action/') + action
  api_url = urlparse.urljoin(context['site_url'], '/api/action/') + action
  
  res = requests.post(
      api_url, json.dumps(post_data),
      headers = {'Authorization': context['apikey'],
                 'Content-Type': 'application/json'}
  )
  return res.content

def clearing(data, context, post_data, offlim):

  response = _celery_api_request('datastore_search', data, context, post_data)
  try:
    datastore = json.loads(response)
  except:
    _change_status(context, data, status='Error: Wrong response', task_type='clearing')
    return {'done':True}
  if not datastore['success']:
    log.error(datastore['error'])
    return {'done':True}

  items = []
  for record in datastore['result']['records']:
    if not record['url'].startswith('http'):
      log.warn('URL without schema {url}'.format(url=record['url']))
      continue
    resp = requests.head(record['url'])
    if resp.status_code > 310:
      items.append({'id':data['resource'],

                    'assetID':record['assetID']})
  response = _celery_api_request('user_remove_asset', data, context, {'items':items})
  print response

  offlim[0] += offlim[1]
  _change_status(context, data, status='Checked first {0} rows'.format(offlim[0]), task_type='clearing')

  if datastore['result'].get('total', -1) < offlim[0]:
    return {'done':True}

def _change_status(context, data, status, task_type):
  task_status = {
        'entity_id': data['resource'],
        'task_type': task_type,
        'key': u'celery_task_id',
        'value': status,
        'error': u'',
        'last_updated': datetime.now().isoformat(),
        'entity_type': 'resource'
    }
  _celery_api_request('task_status_update', data, context, task_status)

def _get_status(context, data, task_type):
  task_status = {
        'entity_id': data['resource'],
        'task_type': task_type,
        'key': u'celery_task_id',
    }
  response = _celery_api_request('task_status_show', data, context, task_status)
  try:
    status = json.loads(response)
  except:
    return

  if status['success']:
    return status['result'].get('value')