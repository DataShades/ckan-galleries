#! /usr/bin/env python
# encoding='utf-8'
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import argparse, copy, json,ckanapi
from time import sleep
from os import getpid
from datetime import datetime

from  ckan.logic import NotFound


from sys import stdout
flush = stdout.flush

pid = getpid()
waiting = 2000

CK  = 'vKAo073zpwfmuiTkyR83qyZEe'
CS  = 'cciB0ZCwQnBASvRp9HPN1vbBdZSCEzyu118igFhQFxOwDVFmVD'
AT  = '23904345-OmiSA5CLpceClmy46IRJ98ddEKoFJAPura2j53ryN'
ATS = 'QYJwGyYODIFB5BJM8F5IXNUDn9coJnKzY6scJOErKRcAE'

USED_KEY = 0
twitter_api_keys = [
  dict(
    CK  = 'vKAo073zpwfmuiTkyR83qyZEe',
    CS  = 'cciB0ZCwQnBASvRp9HPN1vbBdZSCEzyu118igFhQFxOwDVFmVD',
    AT  = '23904345-OmiSA5CLpceClmy46IRJ98ddEKoFJAPura2j53ryN',
    ATS = 'QYJwGyYODIFB5BJM8F5IXNUDn9coJnKzY6scJOErKRcAE'
  ),
  dict(
    CK  = 'VmNHkKuFcza5ouvkNiimpoU8E',
    CS  = 'E9CcaBikENNmbNC2LaG9aWhpiNuvpBBhUElPtZNGwulpvzIVu1',
    AT  = '23904345-4PBhPAYyUn4XvniAFCDOv5HaVEIJt2ik2j7KhEWdx',
    ATS = 'a7Qtt296u2FnSia9fGGpbejJ3Jg420OC0LBPbCmYIIKVs',
  ),
  dict(
    CK  = 'A0aIjONlJLGHQxN9KR15OnQQp',
    CS  = 'khhb58i3Qi2BTD0QhxsfNPurOfZZ7YBQbtMheSoNWldWNyR2oe',
    AT  = '23904345-2MpF4FY06gvwGV1rNuJQ5oEdpvVMlMpWmWoEFXzMi',
    ATS = '8YExrwTKpPVDb3pEGTAGokDyuCzKvKUTLprzcxHlVQ5rG',
  ),
]

print twitter_api_keys[USED_KEY]['CK']
flush()
class TwitterListener(StreamListener):
  def on_data(self, data):
    print data
    print 'Data received...'

    _change_status(
      args,
      'Proccessing data',
      state='Proccessing'
    )
    try:
      _save_data(json.loads(data))
    except Exception, e:
      print e
      _change_status(
        args,
        'Error {%s}, continue listening' % e,
        state='Listening'
      )
      return True
    _change_status(
      args,
      'Listening',
      state='Listening'
    )
    print 'Listening...'
    flush()
    return True

  def on_error(self, status):
    raise Exception(status)

  def on_connect(self):
    print 'on_connect'

      
def _save_data(data):
  flush()
  if not 'extended_entities' in data or not 'media' in data['extended_entities']:
    print 'Media not found...'
    return
  try:
    try:
      spatial = data['place']['bounding_box']
    except Exception, e:
      print e
      spatial = None
    resource = {
      'name': data['user']['screen_name'],
      'time': data['timestamp_ms'][:-3]
    }
  except Exception, e:
    print e
    print 'Data not saved'
    return
  tags = ','.join( [ tag['text'] for tag in data['entities'].get('hashtags', []) ] )
  for asset in data['extended_entities']['media']:
    forbidden_id = ckan.call_action(
      'resource_show',
      {'id': args.resource}
    ).get('forbidden_id', '')
    if asset['id_str'] in forbidden_id: continue
    
    try:
      if len(data['text']) > 139:
        data['text'] = data['text'][:data['text'].rfind('http')]
      meta = copy.deepcopy(data)
      meta.update(
        thumb=asset['media_url']+':small',
        mimetype='image/jpeg',
        id=asset['id_str'],
        tags=tags,
        source='twitter',
      )
      tweet = {
        'assetID': meta['id'],
        'lastModified': datetime\
          .fromtimestamp( int(resource['time']) )\
          .strftime('%Y-%m-%d %H:%M:%S'),
        'name':resource['name'],
        'url':asset['media_url'],
        'metadata':meta,
        'spatial': spatial,
      }
      ckan.call_action('datastore_upsert',  {
        'resource_id':args.resource,
        'force':True,
        'records':[tweet],
        'method': 'insert'
      })

      tweet.update(id=args.resource)
      ckan.call_action(
        'solr_add_assets',
        {'items':[tweet]}
      )
      print 'Item saved...'
    except Exception, e:
      print e
      print 'Problem with saving. Skipped..'
      continue

def get_args():
  parser = argparse.ArgumentParser(description='This script allows to parse Tweets using Twitter\'s StreamAPI', epilog="Default values of Consumer keys and Access tokens should be used only in development and testing ")
  parser.add_argument('--host',
                      help='CKAN instance URL',
                      required=True)
  parser.add_argument('--ckan-api',
                      help='CKAN API-Key which will be used to create resources(need access to edit chosen dataset)',
                      dest='apikey',
                      required=True)
  # parser.add_argument('--dataset',
                      # help='Valid name of CKAN Dataset(alphanumeric, 2-100 characters, may contain - and _) or ID of existing package which will be used as container for resources(if not exists will be added by user who is owner of provided API-Key)',
                      # required=True)
  parser.add_argument('--resource',
                      help='Valid id of CKAN Resource which will be used as container for tweets',
                      required=True)
  parser.add_argument('--search',
                      help='Tag or word for streaming',
                      required=True)
  parser.add_argument('--ck',
                      default=twitter_api_keys[USED_KEY]['CK'],
                      nargs=1,
                      help='Consumer Key (API Key)')
  parser.add_argument('--cs',
                      default=twitter_api_keys[USED_KEY]['CS'],
                      nargs=1,
                      help='Consumer Secret (API Secret)')
  parser.add_argument('--at',
                      default=twitter_api_keys[USED_KEY]['AT'],
                      nargs=1,
                      help='Access Token')
  parser.add_argument('--ats',
                      default=twitter_api_keys[USED_KEY]['ATS'],
                      nargs=1,
                      help='Access Token Secret')
  return parser.parse_args()

def init_datastore(args, ckan):
  ckan.call_action('datastore_create', {
    'resource_id':args.resource,
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


def init_stream(args):
  auth = OAuthHandler(args.ck, args.cs)
  auth.set_access_token(args.at, args.ats)
  return Stream(auth, TwitterListener())

def start_parsing(args):
  print 'Proccess %d. Starting...' % pid
  while True:
    try:
      init_stream(args).filter(track=[args.search])
    except Exception, e:
      _change_status(
        args,
        'Exception %s, restart in %s seconds' % (e, waiting),
        state='Restarting'
      )
      print e
      print 'Restart in %d seconds' % waiting
      flush()
      sleep(waiting)
      _change_status(
        args,
        'Restarting',
        state='Restarting'
      )
      print 'Proccess %d. Restarting...' % pid
      flush()


def _change_status(args, status, state=''):
  task_status = {
      'entity_id': args.resource,
      'task_type': 'twitter_streaming',
      'key': 'celery_task_id',
      'value': status,
      'state':state,
      'error': pid,
      'last_updated': datetime.now().isoformat(),
      'entity_type': 'resource'
    }
  print task_status
  ckan.call_action(
    'task_status_update',
    task_status
  )

args = get_args()
ckan = ckanapi.RemoteCKAN(args.host, args.apikey)

init_datastore(args, ckan)

_change_status(
  args,
  'Started, process %s' % pid,
  state='Listening'
)
start_parsing(args)
