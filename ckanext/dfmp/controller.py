import ckan.plugins.toolkit as toolkit
import ckan.lib.base as base
import ckan.lib.helpers as h
import datetime, os
from dateutil import parser
import ckan.model as model

from ckan.common import c
session = model.Session

import logging
log = logging.getLogger(__name__)



class DFMPController(base.BaseController):

  def get_flickr(self):
    return base.render('package/dataset_from_flickr.html')

  def twitter_listeners(self):
    tasks = session.query(model.TaskStatus).filter_by(task_type='streaming_tweets').all()
    resources = dict(
      session.query(model.Resource.id, model.Resource.name)\
        .filter(
          model.Resource.id.in_(
            [ task.entity_id for task in tasks ])
        ).all()
    )

    for task in tasks:
      task.name = resources[task.entity_id]

    extra_vars = {
      'listeners':tasks,
    }
    return base.render('admin/twitter_listeners.html', extra_vars=extra_vars)

  def getting_tweets(self, id, resource_id):
    context = {
      'model': model,
      'user': c.user or c.author,
      'auth_user_obj': c.userobj
    }

    log.warn(context)
    now = datetime.datetime.now() - datetime.timedelta(1)
    pid = None
    extra_vars = {
      'pkg_id':id,
      'res_id': resource_id,
      'getting_status': {},
      'streaming_status': {},
      'pull_data':{
          'pull_from': now.strftime('%Y-%m-%d'),
          'pull_from_time': now.strftime('%H:%M:%S'),
      },
      'pull_error_summary':{},
      'stream_error_summary':{},
    }
    
    try:
      getting_status = toolkit.get_action('task_status_show')(None, {
        'task_type': 'getting_tweets',
        'entity_id': resource_id,
        'key':'celery_task_id'
      })
      extra_vars.update(getting_status=getting_status)
    except toolkit.ObjectNotFound:
      pass

    try:
      streaming_status = toolkit.get_action('task_status_show')(None, {
        'task_type': 'streaming_tweets',
        'entity_id': resource_id,
        'key':'celery_task_id'
        })
      extra_vars.update(streaming_status=streaming_status)
      if 'value' in streaming_status:
        try:
          pos = streaming_status['value'].rfind(' ')
          pid = streaming_status['value'][pos:]
          pid = int(pid)
          if not os.system('ps %s' % pid):
            extra_vars.update(may_kill = True)
        except:
          pass
    except toolkit.ObjectNotFound:
      pass

    if toolkit.request.method == 'POST':
      pst = toolkit.request.POST
      stable = True

      if 'kill_listener' in pst and pid:
        os.system('kill %s' % pid)
        toolkit.get_action('task_status_update')(None, {
          'entity_id': resource_id,
          'task_type': 'streaming_tweets',
          'key': 'celery_task_id',
          'value': 'Terminated',
          'error': u'',
          'last_updated': datetime.datetime.now().isoformat(),
          'entity_type': 'resource'
        })
        base.redirect(h.url_for('getting_tweets', id=id, resource_id=resource_id))

      

      if 'pull_from' in pst or 'pull_word' in pst:
        word = pst.get('pull_word')
        if not word:
          stable = False
          extra_vars['pull_error_summary'].update( { 'Hashtag': 'Must be defined' } )
        date = pst.get('pull_from', '') + ' ' + pst.get('pull_from_time', '')
        try:
          date = parser.parse(date).isoformat(' ')
        except ValueError:
          stable = False
          extra_vars['pull_error_summary'].update( { 'Date': 'Wrong date' } )

        if stable:
          getting = toolkit.get_action('celery_getting_tweets')(context,{
            'resource': resource_id,
            'word': pst['pull_word'],
            'deepness': date,
          })
          base.redirect(h.url_for('getting_tweets', id=id, resource_id=resource_id))

      elif 'stream_word' in pst:
        log.warn(pst)
        word = pst.get('stream_word')
        if not word:
          stable = False
          extra_vars['stream_error_summary'].update( { 'Hashtag': 'Must be defined' } )
        
        if stable:
          streaminging = toolkit.get_action('celery_streaming_tweets')(context,{
            'resource': resource_id,
            'word': pst['stream_word'],
          })
          base.redirect(h.url_for('getting_tweets', id=id, resource_id=resource_id))

    try:
      toolkit.c.pkg_dict = toolkit.get_action('package_show')(None, {'id': id})
      toolkit.c.resource = toolkit.get_action('resource_show')(None, {'id': resource_id})
    except toolkit.ObjectNotFound:
      base.abort(404, _('Resource not found'))
    except toolkit.NotAuthorized:
      base.abort(401, _('Unauthorized to edit this resource'))



    return base.render('package/twitter_actions.html', extra_vars=extra_vars)
