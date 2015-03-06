import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from datetime import datetime

import ckan.model as model
import logging, urlparse, json, requests, urllib2
log = logging.getLogger(__name__)


def clearing(context, data):
  # try:
  context = json.loads(context)
  offset=0
  limit=1000
  try:
    while True:
      current_status = _get_status(context, data)
      if current_status == 'stop':
        print 'Terminated by API'
        raise Exception('Stop')
      post_data = {'resource_id':data['resource'], 
                   'offset':offset, 
                   'limit':limit}
      response = _celery_api_request('datastore_search', data, context, post_data)
      datastore = json.loads(response)

      if not datastore['success']:
        log.error(datastore['error'])
        break

      items = []
      for record in datastore['result']['records']:
        resp = requests.head(record['url'])
        if resp.status_code > 310:
          items.append({'id':data['resource'], 
                        'assetID':record['assetID']})

      response = _celery_api_request('user_remove_asset', data, context, {'items':items})
      print response

      offset += limit
      _change_status(context, data, 'Checked first {0} rows'.format(offset))

      if datastore['result'].get('total', -1) < offset:
        break
    _change_status(context, data, 'Done')
    print 'Done'
  except toolkit.ObjectNotFound:
    _change_status(context, data, 'Error: Resource not found')
  # except Exception, e:
  #   log.warn(e)

def _celery_api_request(action, data, context, post_data):
  api_url = urlparse.urljoin(context['site_url'], 'api/action/') + action
  
  res = requests.post(
      api_url, json.dumps(post_data),
      headers = {'Authorization': context['apikey'],
                 'Content-Type': 'application/json'}
  )
  return res.content


def _change_status(context, data, status):
  task_status = {
        'entity_id': data['resource'],
        'task_type': u'clearing',
        'key': u'celery_task_id',
        'value': status,
        'error': u'',
        'last_updated': datetime.now().isoformat(),
        'entity_type': 'resource'
    }
  _celery_api_request('task_status_update', data, context, task_status)

def _get_status(context, data):
  task_status = {
        'entity_id': data['resource'],
        'task_type': u'clearing',
        'key': u'celery_task_id',
    }
  response = _celery_api_request('task_status_show', data, context, task_status)
  status = json.loads(response)

  if status['success']:
    return status['result'].get('value')