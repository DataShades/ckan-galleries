import uuid
from ckan.lib.celery_app import celery
import celery as ce
import celery.events.state as st
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
from ckan.logic import side_effect_free

import logging
log = logging.getLogger(__name__)


from datetime import datetime
import json
from ckan import model
from ckan.model.types import make_uuid

from ckan.lib.dictization.model_dictize import resource_dictize

from pylons import config

@side_effect_free
def celery_cleaning(context, data_dict):
  task_id, celery_context = _prepare_celery(context, data_dict, 'clearing')
  log.warn(task_id)
  celery.send_task("dfmp.cleaning", args=[celery_context, data_dict], task_id=task_id)

@side_effect_free
def celery_getting_tweets(context, data_dict):
  task_id, celery_context = _prepare_celery(context, data_dict, 'getting_tweets')
  log.warn(task_id)
  celery.send_task("dfmp.getting_tweets", args=[celery_context, data_dict], task_id=task_id)

@side_effect_free
def celery_streaming_tweets(context, data_dict):
  task_id, celery_context = _prepare_celery(context, data_dict, 'streaming_tweets')
  log.warn('task_id')
  log.warn(task_id)
  a = celery.send_task("dfmp.streaming_tweets", args=[celery_context, data_dict], task_id=task_id)
  log.warn(a)

def celery_revoke(context, data_dict):
  task_id, celery_context = _prepare_celery(context, data_dict, 'revoke')
  celery.send_task("dfmp.revoke_listener", args=[celery_context, data_dict], task_id=task_id)
  return task_id

def _prepare_celery(context, data_dict, task_type):
  task_id = str(uuid.uuid4())
  user = context['auth_user_obj']
  if not user.sysadmin:
    raise toolkit.NotAuthorized

  userapikey = user.apikey

  celery_context = json.dumps({
      'site_url': config.get('ckan.site_url'),
      'apikey': userapikey,
  })

  task_status = {
            'entity_id': data_dict.get('resource', "without resource"),
            'entity_type': u'resource',
            'task_type': task_type,
            'key': u'celery_task_id',
            'value': data_dict.get('word', ''),
            'state':'Preparing',
            'error': u'task_id:%s' % task_id,
            'last_updated': datetime.now().isoformat()
        }

  toolkit.get_action('task_status_update')(context, task_status)

  return task_id, celery_context


@side_effect_free
def celery_flickr_import (context, data_dict):
  task_id, celery_context = _prepare_celery(context, data_dict, 'flickr_images')
  log.warn(task_id)
  celery.send_task("dfmp.flickr_images", args=[celery_context, data_dict], task_id=task_id)

@side_effect_free
def celery_solr_indexing(context, data_dict):
  data_dict.update(solr_index=True)
  task_id, celery_context = _prepare_celery(context, data_dict, 'clearing')
  log.warn(task_id)
  celery.send_task("dfmp.cleaning", args=[celery_context, data_dict], task_id=task_id)
