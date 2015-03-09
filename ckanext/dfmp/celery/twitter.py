from tweepy import API, OAuthHandler, Stream
from tweepy.streaming import StreamListener
from dateutil import parser
import datetime, logging, json
from time import sleep
log = logging.getLogger(__name__)
from os import getpid


CK  = 'vKAo073zpwfmuiTkyR83qyZEe'
CS  = 'cciB0ZCwQnBASvRp9HPN1vbBdZSCEzyu118igFhQFxOwDVFmVD'
AT  = '23904345-OmiSA5CLpceClmy46IRJ98ddEKoFJAPura2j53ryN'
ATS = 'QYJwGyYODIFB5BJM8F5IXNUDn9coJnKzY6scJOErKRcAE'

auth = OAuthHandler(CK, CS)
auth.set_access_token(AT, ATS)

def init_api():
  return API(auth)

def search_tweets(api, word, deepness=None, with_media=True, **kargs):
  args = {
    'count': 100,
    'max_id': None,
    'total': 10000
  }
  if deepness:
    try:
      deepness = parser.parse(deepness)
    except ValueError:
      raise ValueError('Wrong date')
  else:
    deepness = datetime.datetime.now() - datetime.timedelta(days=2)

  args.update(kargs)

  tweets = 0
  while tweets < int(args['total']):
    result = api.search(word, **args)
    if not result:
      break
    last = result[-1]

    args.update(max_id = str(last.id))

    if with_media:
      result = filter(lambda x: 'media' in x.entities, result)
    tweets += len(result)
    yield result

    if last.created_at < deepness:
      break