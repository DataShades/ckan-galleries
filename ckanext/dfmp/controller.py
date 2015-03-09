import ckan.plugins.toolkit as toolkit
import ckan.lib.base as base
import ckan.lib.helpers as h
import datetime
from dateutil import parser
import ckan.model as model

from ckan.common import c
# import ckan.lib.helpers as helpers

import logging
log = logging.getLogger(__name__)

class DFMPController(base.BaseController):
    def getting_tweets(self, id, resource_id):
        now = datetime.datetime.now() - datetime.timedelta(1)
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

        if toolkit.request.method == 'POST':
            context = {
                'model': model,
                'user': c.user or c.author,
                'auth_user_obj': c.userobj
            }

            stable = True
            pst = toolkit.request.POST

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
            getting_status = toolkit.get_action('task_status_show')(None, {
                'task_type': 'getting_tweets',
                'entity_id': resource_id,
                'key':'celery_task_id'
                })
            extra_vars.update(getting_status=getting_status)
        except toolkit.ObjectNotFound:
            pass

        try:
            toolkit.c.pkg_dict = toolkit.get_action('package_show')(
                None, {'id': id}
            )
            toolkit.c.resource = toolkit.get_action('resource_show')(
                None, {'id': resource_id}
            )
            # toolki.c.form_action = helpers.url_for('getting_tweets', id=id, resource_id=resource_id)
        except toolkit.ObjectNotFound:
            base.abort(404, _('Resource not found'))
        except toolkit.NotAuthorized:
            base.abort(401, _('Unauthorized to edit this resource'))

 

        return base.render('package/twitter_actions.html', extra_vars=extra_vars)
