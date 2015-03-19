import ckan.plugins.toolkit as toolkit
import ckan.lib.base as base
import ckan.lib.helpers as h
import datetime, os, re, json
from dateutil import parser
import ckan.model as model
from pylons import config
from ckan.common import c, g, _, OrderedDict, request
from urllib import urlencode

session = model.Session
from ckanext.dfmp.dfmp_solr import DFMPSolr, DFMPSearchQuery
import logging
log = logging.getLogger(__name__)
import ckanext.dfmp.scripts as scripts
from ckanext.dfmp.actions.action import _name_normalize

ASSETS_PER_PAGE = 20
log_path = '/var/log/dfmp/'

def search_url(params):
  url = h.url_for('search_assets')
  return url_with_params(url, params)

def url_with_params(url, params):
  params = _encode_params(params)
  return url + u'?' + urlencode(params)

def _encode_params(params):
  return [(k, v.encode('utf-8') if isinstance(v, basestring) else str(v))
    for k, v in params]

class DFMPController(base.BaseController):

  def get_flickr(self):
    return base.render('package/dataset_from_flickr.html')

  def search_assets(self):

    q = c.q = request.params.get('q', u'')
    c.query_error = False
    sort_by = request.params.get('sort', None)
    c.sort_by_selected = sort_by

    try:
      page = int(request.params.get('page', 1))
    except ValueError, e:
      abort(400, ('"page" parameter must be an integer'))
    params_nopage = [
      (k, v) for k, v in request.params.items()
      if k != 'page'
    ]
    params_nosort = [(k, v) for k, v in params_nopage if k != 'sort']

    def _sort_by(fields):
      """
      Sort by the given list of fields.
      Each entry in the list is a 2-tuple: (fieldname, sort_order)
      eg - [('metadata_modified', 'desc'), ('name', 'asc')]
      If fields is empty, then the default ordering is used.
      """
      params = params_nosort[:]

      if fields:
        sort_string = ', '.join('%s %s' % f for f in fields)
        params.append(('sort', sort_string))
      return search_url(params)
    c.sort_by = _sort_by
    if sort_by is None:
      sort_by = 'metadata_modified desc'
      c.sort_by_fields = []
    else:
      c.sort_by_fields = [field.split()[0]
                            for field in sort_by.split(',')]


    def remove_field(key, value=None, replace=None):
      return h.remove_url_param(key, value=value, replace=replace,
                            controller='ckanext.dfmp.controller:DFMPController', action='search_assets')
    c.remove_field = remove_field

    def pager_url(q=None, page=None):
      params = list(params_nopage)
      params.append(('page', page))
      return search_url(params)
    c.search_url_params = urlencode(_encode_params(params_nopage))

    facet_fields = [
      'organization',
      # 'groups',
      'tags',
      # 'res_format',
      'license_id']

    fq = ''
    # log.warn(params_nosort)
    for param in params_nosort:
      if param[0] in facet_fields:
        fq += ' +{0}:"{1}"'.format(*param)



    result = DFMPSearchQuery()({
      'q':q,
      'fl':'data_dict',
      'fq':fq,
      'facet.field':facet_fields,
      'sort':sort_by,
      'rows':ASSETS_PER_PAGE,
      'start':(page - 1) * ASSETS_PER_PAGE

    })

    default_facet_titles = {
      'organization': _('Organizations'),
      'groups': _('Groups'),
      'tags': _('Tags'),
      'res_format': _('Formats'),
      'license_id': _('Licenses'),
    }

    facets = OrderedDict()
    for facet in facet_fields:
      if facet in default_facet_titles:
        facets[facet] = default_facet_titles[facet]
      else:
        facets[facet] = facet
    c.facet_titles = facets

    restructured_facets = {}
    for key, value in result['facets'].items():
        restructured_facets[key] = {
                'title': key,
                'items': []
                }
        for key_, value_ in value.items():
            new_facet_dict = {}
            new_facet_dict['name'] = key_
            if key in ('groups', 'organization'):
                group = model.Group.get(key_)
                if group:
                    new_facet_dict['display_name'] = group.display_name
                else:
                    new_facet_dict['display_name'] = key_
            elif key == 'license_id':
                license = model.Package.get_license_register().get(key_)
                if license:
                    new_facet_dict['display_name'] = license.title
                else:
                    new_facet_dict['display_name'] = key_
            else:
                new_facet_dict['display_name'] = key_
            new_facet_dict['count'] = value_
            restructured_facets[key]['items'].append(new_facet_dict)
    c.search_facets = restructured_facets

    c.search_facets_limits = {}
    for facet in c.search_facets.keys():
      try:
        limit = int(request.params.get('_%s_limit' % facet,
                                       g.facets_default_number))
      except ValueError:
        abort(400, _('Parameter "{parameter_name}" is not '
           'an integer').format(
             parameter_name='_%s_limit' % facet
           ))
      c.search_facets_limits[facet] = limit

    assets = [ json.loads(item['data_dict']) for item in result['results'] ]

    c.page = h.Page(
        collection=assets,#query['results'],
        page=page,#page,
        url=pager_url,#pager_url,
        item_count=result['count'],
        items_per_page=ASSETS_PER_PAGE,#limit
    )
    c.page.items = assets

    extra_vars = {
      'assets':assets,

    }
    return base.render('package/search_assets.html', extra_vars = extra_vars)

  def terminate_listener(self, id, resource_id):
    self._listener_route('terminate', id, resource_id)

  def start_listener(self, id, resource_id):
    self._listener_route('start', id, resource_id)

  def solr_commit(self):
    DFMPSolr().commit()
    base.redirect(c.environ.get('HTTP_REFERER', config.get('ckan.site_url','/')))

  def twitter_listeners(self):
    if not c.userobj or not c.userobj.sysadmin:
      base.abort(404)

    tasks = []
    for task, resource in session.query(model.TaskStatus, model.Resource)\
      .filter(
        model.TaskStatus.entity_id == model.Resource.id,
        model.TaskStatus.task_type == 'twitter_streaming',
        model.Resource.state == 'active').all():

      pid = task.error or ''
      if pid:
        task.pid = pid
        if not os.system('ps %s' % pid):
            task.is_active = True
      task.name = resource.name
      task.pkg = resource.get_package_id()
      tasks.append(task)
   
    extra_vars = {
      'listeners':tasks,
      'now':datetime.datetime.now()
    }
    return base.render('admin/twitter_listeners.html', extra_vars=extra_vars)
  

  def _listener_route(self, action, id, resource_id):
    if not c.userobj or not c.userobj.sysadmin:
      base.abort(404)
    context = {
      'model': model,
      'user': c.user or c.author,
      'auth_user_obj': c.userobj
    }
    if action == 'terminate':
      task = session.query(model.TaskStatus)\
        .filter(
          model.TaskStatus.task_type=='twitter_streaming',
          model.TaskStatus.entity_id==resource_id)\
        .first()
      if not task:
        h.flash_error("Can't find listener")
      if task:
        pid = task.error or '' 
        if not pid:
          h.flash_error("Can't get PID of process")
        else:
          h.flash_success('Success')
          toolkit.get_action('task_status_update')(None, {
            'entity_id': resource_id,
            'task_type': 'twitter_streaming',
            'key': 'celery_task_id',
            'state': 'Terminated',
            'value': 'Ready for start',
            'error': pid,
            'last_updated': datetime.datetime.now().isoformat(),
            'entity_type': 'resource'
          })
          if os.system('kill -9 %s' % pid):
            toolkit.get_action('celery_revoke')(context, {'id': pid, 'resource': resource_id})
    base.redirect(h.url_for('getting_tweets', id=id, resource_id=resource_id))
    

  def getting_tweets(self, id, resource_id):
    if not c.userobj or not c.userobj.sysadmin:
      base.abort(404)
    context = {
      'model': model,
      'user': c.user or c.author,
      'auth_user_obj': c.userobj
    }


    log_access = os.access(log_path, os.W_OK)
    if not log_access:
      h.flash_error('Listener will be working without log file. Create or modify access to {log_path} if you want to see process log'.format(log_path=log_path))

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
        'task_type': 'twitter_streaming',
        'entity_id': resource_id,
        'key':'celery_task_id'
        })
      log.warn(streaming_status)
      extra_vars.update(streaming_status=streaming_status)
      if 'error' in streaming_status:
        try:
          pid = streaming_status['error']
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

      if 'kill_listener' in pst:
        self._listener_route('terminate', id, resource_id)

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
        word = pst['stream_word']
        if not word:
          stable = False
          extra_vars['stream_error_summary'].update( { 'Hashtag': 'Must be defined' } )

        if stable:
          args = ' --host {host} --ckan-api {apikey} --resource {resource} --search \'{word}\''.\
              format(
                host=config.get('ckan.site_url', ''),
                apikey=c.userobj.apikey,
                resource=resource_id,
                word=word
              )
          valid_word = _name_normalize(word)
          log_file = ( '>' + log_path + 'dfmp_{word}.log'.format(word=valid_word) ) if log_access else ''
          log.warn(log_file)
          command = 'nohup ' + os.path.dirname(scripts.__file__) + os.path.sep + 'twitter.py ' + args  +  log_file + ' & 1'
          log.warn(command)
          status = os.system(command)

          base.redirect(h.url_for('getting_tweets', id=id, resource_id=resource_id))

    try:
      toolkit.c.pkg_dict = toolkit.get_action('package_show')(None, {'id': id})
      toolkit.c.resource = toolkit.get_action('resource_show')(None, {'id': resource_id})
    except toolkit.ObjectNotFound:
      base.abort(404, _('Resource not found'))
    except toolkit.NotAuthorized:
      base.abort(401, _('Unauthorized to edit this resource'))



    return base.render('package/twitter_actions.html', extra_vars=extra_vars)

def _get_pid(val):
  res = re.search('\d+$',val)
  if res:
    return res.group()