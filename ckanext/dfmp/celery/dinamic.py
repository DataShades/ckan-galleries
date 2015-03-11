import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free
from datetime import datetime
from ckanext.dfmp.celery import twitter
import ckanapi
import ckan.model as model
import logging, urlparse, json, requests, urllib2
log = logging.getLogger(__name__)


from tweepy import Stream
from tweepy.streaming import StreamListener
from time import sleep
from os import getpid



def datastore_mass(context, data, workflow):
  context = json.loads(context)
  offlim = [0, 1000]
  try:
    while True:
      current_status = _get_status(context, data, task_type=workflow)
      print current_status
      if current_status == 'stop':
        print 'Terminated by API'
        raise Exception('Stop')

      post_data = {
        'resource_id':data['resource'],
        'offset':offlim[0],
        'limit':offlim[1]
      }
      print(workflow)

      result = {
        'clearing' : clearing,
        'getting_tweets' : getting_tweets,
        'streaming_tweets' : streaming_tweets,
      }[workflow](
        data,
        context,
        post_data,
        offlim
      )

      if result and result.get('done'):
        break
    _change_status(context, data, status='Done', task_type=workflow)
    print 'Done'
  except toolkit.ObjectNotFound:
    _change_status(
      context,
      data,
      status='Error: Resource not found',
      task_type=workflow
    )

def _celery_api_request(action, context, post_data):
  print context['site_url']

  ckan = ckanapi.RemoteCKAN(
    context['site_url']
    # + '/data'
    ,
    context['apikey'])
  result = ckan.call_action(action, post_data)
  return result

def clearing(data, context, post_data, offlim):
  try:
    datastore = _celery_api_request(
      'datastore_search',
      context,
      post_data
    )
  except ckanapi.errors.NotFound, e:
    print e
    return {'done':True}

  items = []
  for record in datastore['records']:
    if not record['url'].startswith('http'):
      log.warn(
        'URL without schema {url}'.format(url=record['url'])
      )
      continue

    resp = requests.head( record['url'] )
    if resp.status_code > 310:
      items.append({
        'id':data['resource'],
        'assetID':record['assetID']
      })

  response = _celery_api_request(
    'user_remove_asset',
    context,
    { 'items' : items }
  )
  print response
  # print items
  # print 'removed'

  offlim[0] += offlim[1]
  _change_status(
    context,
    data,
    status='Checked first {0} rows' . format(offlim[0]),
    task_type='clearing'
  )

  if datastore.get('total', -1) < offlim[0]:
    return {'done':True}

def getting_tweets(data, context, post_data, offlim):
  twitter_api = twitter.init_api()
  resource = data['resource']
  word = data.pop('word')

  searcher = twitter.search_tweets(twitter_api, word, **data)
  total = 0
  _create_datastore(data['resource'], context)
  for piece in searcher:
    if not piece:
      continue
    records = []
    for item in piece:
      item_json = item._json
      item_json.update(mimetype = 'image/jpeg', type = 'image/jpeg')
      records.append({
        'assetID': item.entities['media'][0]['id_str'],
        'lastModified': item.created_at.isoformat(' '),
        'name': item.user.screen_name,
        'url': item.entities['media'][0]['media_url'],
        'metadata': item_json,
        'spatial': {
          'type' : item.place.bounding_box.type,
          'coordinates' : item.place.bounding_box.coordinates
        } if item.place else None,
      })

    post_data = {
      'resource_id':resource,
      'force':True,
      'records': records,
      'method': 'upsert'
    }
    _celery_api_request(
      'datastore_upsert',
      context,
      post_data
    )
    total += len(records)
    status = 'Added {0} tweets, from {1} to {2}'.format(total, records[0]['lastModified'], records[-1]['lastModified'])
    _change_status(
      context,
      data,
      status=status,
      task_type='getting_tweets'
    )
    print status
  return {'done':True}

def streaming_tweets(data, context, post_data, offlim):
  _create_datastore(data['resource'], context)
  _change_status(
    context,
    data,
    'Started, process %s' % ( getpid()),
    'streaming_tweets',
    state='Listening'
  )
  print 'Starting %s...' % getpid()
  while True:
    try:
      init_twitter_stream(TwitterListener, context, data).filter(track=[data['word']])
    except Exception, e:
      print e
      print 'Restart in 60 seconds'
      sleep(60)
      print 'Restarting...'



def _change_status(context, data, status, task_type, state=''):
  task_status = {
      'entity_id': data['resource'],
      'task_type': task_type,
      'key': u'celery_task_id',
      'value': status,
      'state':state,
      'error': u'',
      'last_updated': datetime.now().isoformat(),
      'entity_type': 'resource'
    }
  _celery_api_request(
    'task_status_update',
    context,
    task_status
  )

def _get_status(context, data, task_type):
  task_status = {
      'entity_id': data['resource'],
      'task_type': task_type,
      'key': u'celery_task_id',
    }
  response = _celery_api_request(
    'task_status_show',
    context,
    task_status
  )
  try:
    status = response
  except:
    return

  return status.get('value')



class TwitterListener(StreamListener):
    def __init__(self, context, data):
      StreamListener.__init__(self)
      self.context = context
      self.data = data
    def on_data(self, data):
      print self
      print 'Data received...'
      _change_status(
        self.context,
        self.data,
        'Proccessing data, process %s' % ( getpid()),
        'streaming_tweets',
        state='Proccessing'
      )
      try:
        _twitter_save_data(json.loads(data), self.context, self.data)
      except Exception, e:
        print e
        _change_status(self.context,
          self.data,
          'Error {%s}, continue listening. Process %s' % (e, getpid()),
          'streaming_tweets',
          state='Listening'
        )
        return True
      _change_status(self.context,
        self.data,
        'Listening, process %s' % (getpid()),
        'streaming_tweets',
        state='Listening'
      )
      print 'Listening...'
      return True
    def on_error(self, status):
      _change_status(self.context,
        self.data,
        'Error %s, process %s' % (status, getpid()),
        'streaming_tweets',
        state='Listening'
      )
      print 'Error %d' % status
    def on_connect(self):
      print 'on_connect'
    def on_delete(self, status_id, user_id):
      print 'on_delete'
      print status_id
      print user_id
    def on_disconnect(self, notice):
      print 'on_disconnect'
      print notice
    def on_event(self, status):
      print 'on_event'
      print status
    def on_exception(self, exception):
      print 'on_exception'
      print exception
    def on_limit(self, track):
      print 'on_limit'
      print track
    def on_timeout(self):
      print 'on_timeout'
    def on_warning(self, notice):
      print 'on_warning'
      print notice


def init_twitter_stream(TwitterListener, context, data):
  return Stream(twitter.auth, TwitterListener(context, data))

def _twitter_save_data(data, context, data_dict):
  if not 'extended_entities' in data or not 'media' in data['extended_entities']:
    'Media not found...'
    return
  try:
    try:
      spatial = data['place']['bounding_box']
    except Exception, e:
      print e
      spatial = None
    resource = {
      'text': data['text'],
      'name': data['user']['screen_name'],
      'time': data['timestamp_ms'][:-3]
    }
  except Exception, e:
    print e
    print 'Data not saved'
    return
  for asset in data['extended_entities']['media']:
    try:
      resource.update(
        thumb=asset['media_url'],
        mimetype='image/jpeg',
        id=asset['id_str']
      )

      _celery_api_request('datastore_upsert', context, {
        'resource_id':data_dict['resource'],
        'force':True,
        'records':[{
          'assetID': resource['id'],
          'lastModified': datetime\
            .fromtimestamp( int(resource['time']) )\
            .strftime('%Y-%m-%d %H:%M:%S'),
          'name':resource['name'],
          'url':resource['thumb'],
          'metadata':resource,
          'spatial': spatial,
        }],
        'method': 'insert'
      })

      print 'Proccess %d. Item saved...' % getpid()
    except Exception, e:
      print e
      print 'Proccess %d. Problem with saving. Skipped..' % getpid()
      continue

def _create_datastore(id, context):
  _celery_api_request('datastore_create', context, {
    'resource_id':id,
    'force': True,
    'fields':[
      {'id':'assetID', 'type':'text'},
      {'id':'lastModified', 'type':'text'},
      {'id':'name', 'type':'text'},
      {'id':'url', 'type':'text'},
      {'id':'spatial', 'type':'json'},
      {'id':'metadata', 'type':'json'},
    ],
    'primary_key':['assetID'],
    'indexes':['name', 'assetID']
  })