#! /usr/bin/env python
# encoding=utf-8
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import argparse
import json
from time import sleep
from os import getpid
from datetime import datetime

from  ckan.logic import NotFound
import ckanapi

from sys import stdout
flush = stdout.flush

pid = getpid()
waiting = 300

class TwitterListener(StreamListener):
    def on_data(self, data):
      print 'Proccess %d. %s. Data received...' % (pid, datetime.now())
      _save_data(json.loads(data))
      print 'Proccess %d. Listening...' % pid
      flush()
      return True
    def on_error(self, status):
      print 'Proccess %d. Error %d' % (pid, status)
      flush()


def _save_data(data):
  print(data)
  if not 'extended_entities' in data or not 'media' in data['extended_entities']:
    'Proccess %d. Media not found...' % pid
    return
  try:
    resource = {'package_id': package['id'],
              'description': data['text'],
              'spatial': data['place'].get('bounding_box']) if data['place'] else None,
              'name': '{0}'.format(data['user']['screen_name'])}
  except Exception, e:
    print e
    print 'Proccess %d. Data not saved' % pid
    flush()
    return
  for asset in data['extended_entities']['media']:
    try:
      resource.update(url=asset['media_url'], thumb=asset['media_url'], mimetype='image/jpeg')
      ckan.call_action('resource_create', resource)
      print 'Proccess %d. Item saved...' % pid
      flush()
    except Exception, e:
      print e
      print 'Proccess %d. Problem with saving. Skipped..' % pid
      flush()
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
  parser.add_argument('--dataset',
                      help='Valid name of CKAN Dataset(alphanumeric, 2-100 characters, may contain - and _) or ID of existing package which will be used as container for resources(if not exists will be added by user who is owner of provided API-Key)',
                      required=True)
  parser.add_argument('--search',
                      help='Tag or word for streaming',
                      required=True)
  parser.add_argument('--ck',
                      default='VmNHkKuFcza5ouvkNiimpoU8E',
                      nargs=1,
                      help='Consumer Key (API Key)')
  parser.add_argument('--cs',
                      default='E9CcaBikENNmbNC2LaG9aWhpiNuvpBBhUElPtZNGwulpvzIVu1',
                      nargs=1,
                      help='Consumer Secret (API Secret)')
  parser.add_argument('--at',
                      default='23904345-4PBhPAYyUn4XvniAFCDOv5HaVEIJt2ik2j7KhEWdx',
                      nargs=1,
                      help='Access Token')
  parser.add_argument('--ats',
                      default='a7Qtt296u2FnSia9fGGpbejJ3Jg420OC0LBPbCmYIIKVs',
                      nargs=1,
                      help='Access Token Secret')
  return parser.parse_args()

def init_package(args, ckan):
  try:
    return ckan.call_action('package_show',{'id': args.dataset})
  except NotFound:
    return ckan.call_action('package_create',{'name': args.dataset})

def init_stream(args):
  auth = OAuthHandler(args.ck, args.cs)
  auth.set_access_token(args.at, args.ats)
  return Stream(auth, TwitterListener())

def start_parsing(args, twitterStream):
  print 'Proccess %d. Starting...' % pid
  while True:
    try:
      twitterStream.filter(track=[args.search])
    except Exception, e:
      print e
      print 'Restart in %d seconds' % waiting
      flush()
      sleep(waiting)
      print 'Proccess %d. Restarting...' % pid
      flush()
    except KeyboardInterrupt:
      print 'Terminated'
      exit(0)


args = get_args()
ckan = ckanapi.RemoteCKAN(args.host, args.apikey)
package = init_package(args, ckan)

start_parsing(args, init_stream(args))

